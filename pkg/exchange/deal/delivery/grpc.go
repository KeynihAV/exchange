package delivery

import (
	context "context"
	"database/sql"

	configPkg "github.com/KeynihAV/exchange/pkg/config"
	dealPkg "github.com/KeynihAV/exchange/pkg/exchange/deal"
	dealUsecasePkg "github.com/KeynihAV/exchange/pkg/exchange/deal/usecase"
	"github.com/KeynihAV/exchange/pkg/logging"
	"go.uber.org/zap"
)

type MyExchangeServer struct {
	UnimplementedExchangeServer
	DealsManager *dealUsecasePkg.DealsManager
	Logger       *logging.Logger
}

func NewExchangeServer(db *sql.DB, config *configPkg.Config, logger *logging.Logger) (*MyExchangeServer, error) {
	dm, err := dealUsecasePkg.NewDealsManager(db, config)
	if err != nil {
		return nil, err
	}
	dm.DealsFlowCh = make(chan *dealPkg.Deal, 10000)
	return &MyExchangeServer{DealsManager: dm, Logger: logger}, nil
}

func (es *MyExchangeServer) Create(ctx context.Context, deal *Deal) (*DealID, error) {

	newOrder := &dealPkg.Order{
		ID:       deal.ID,
		BrokerID: deal.BrokerID,
		ClientID: deal.ClientID,
		Ticker:   deal.Ticker,
		Volume:   deal.Volume,
		Price:    deal.Price,
		Type:     deal.Type,
	}
	dealID, err := es.DealsManager.CreateOrder(newOrder)
	if err != nil {
		es.Logger.Zap.Error("create order",
			zap.String("logger", "grpcServer"),
			zap.String("err", err.Error()),
		)
		return nil, err
	}

	return &DealID{ID: dealID}, nil
}

func (es *MyExchangeServer) Cancel(ctx context.Context, dealID *DealID) (*CancelResult, error) {
	err := es.DealsManager.CancelOrder(dealID.ID)
	if err != nil {
		es.Logger.Zap.Error("cancel order",
			zap.String("logger", "grpcServer"),
			zap.String("err", err.Error()),
		)
		return nil, err
	}
	return &CancelResult{Success: true}, nil
}

func (es *MyExchangeServer) Statistic(broker *BrokerID, ess Exchange_StatisticServer) error {
	chanOHLCV := make(chan dealPkg.OHLCV, 10000)
	defer func() {
		es.DealsManager.StatsConsumers.Mux.Lock()
		delete(es.DealsManager.StatsConsumers.Channels, chanOHLCV)
		es.DealsManager.StatsConsumers.Mux.Unlock()
	}()

	es.DealsManager.StatsConsumers.Mux.Lock()
	es.DealsManager.StatsConsumers.Channels[chanOHLCV] = struct{}{}
	es.DealsManager.StatsConsumers.Mux.Unlock()

	for {
		select {
		case <-ess.Context().Done():
			return nil
		case ohlcv := <-chanOHLCV:
			err := ess.Send(&OHLCV{
				ID:       ohlcv.ID,
				Time:     ohlcv.Time,
				Interval: ohlcv.Interval,
				Open:     ohlcv.Open,
				High:     ohlcv.High,
				Low:      ohlcv.Low,
				Close:    ohlcv.Close,
				Volume:   ohlcv.Volume,
				Ticker:   ohlcv.Ticker,
			})
			if err != nil {
				es.Logger.Zap.Error("statistic",
					zap.String("logger", "grpcServer"),
					zap.String("err", err.Error()),
				)
				return err
			}
		}
	}
}

func (es *MyExchangeServer) Results(broker *BrokerID, ers Exchange_ResultsServer) error {
	chanResults := make(chan dealPkg.Deal, 10000)
	defer func() {
		es.DealsManager.ResultsConsumers.Mux.Lock()
		delete(es.DealsManager.ResultsConsumers.Channels, broker.ID)
		es.DealsManager.ResultsConsumers.Mux.Unlock()
	}()

	//при подключении можно проверить наличие недоставленных брокеру сделок
	es.DealsManager.ResultsConsumers.Mux.Lock()
	es.DealsManager.ResultsConsumers.Channels[broker.ID] = chanResults
	es.DealsManager.ResultsConsumers.Mux.Unlock()

	for {
		select {
		case <-ers.Context().Done():
			return nil
		case deal := <-chanResults:
			err := ers.Send(&Deal{
				ID:       deal.ID,
				BrokerID: deal.BrokerID,
				ClientID: deal.ClientID,
				OrderID:  deal.OrderID,
				Ticker:   deal.Ticker,
				Volume:   deal.Volume,
				Partial:  deal.Partial,
				Time:     deal.Time,
				Price:    deal.Price,
				Type:     deal.Type,
			})
			if err != nil {
				es.Logger.Zap.Error("results",
					zap.String("logger", "grpcServer"),
					zap.String("err", err.Error()),
				)
				return err
			}
			err = es.DealsManager.MarkDealShipped(deal.ID)
			if err != nil {
				es.Logger.Zap.Error("mark deal shipped",
					zap.String("logger", "grpcServer"),
					zap.String("err", err.Error()),
				)
			}
		}
	}
}
