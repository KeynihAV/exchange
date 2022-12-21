package usecase

import (
	"fmt"
	"sync"
	"time"

	configPkg "github.com/KeynihAV/exchange/pkg/exchange/config"
	"github.com/KeynihAV/exchange/pkg/exchange/deal"
	dealPkg "github.com/KeynihAV/exchange/pkg/exchange/deal"
	dealRepoPkg "github.com/KeynihAV/exchange/pkg/exchange/deal/repo"
)

type ExchangeRepo interface {
	AddOrder(order *dealPkg.Order) (int64, error)
	DeleteOrder(orderID int64) error
	GetOrdersForClose(ticker string, price float32) ([]*dealPkg.Order, error)
	MakeDeal(order *dealPkg.Order, volumeToClose int32) (*deal.Deal, error)
	MarkDealShipped(dealID int64) error
}

type Consumers struct {
	Channels map[chan dealPkg.OHLCV]struct{}
	Mux      *sync.RWMutex
}

type ResultsConsumers struct {
	Channels map[int64]chan dealPkg.Deal
	Mux      *sync.RWMutex
}

type DealsManager struct {
	ER               ExchangeRepo
	DealsFlowCh      chan *dealPkg.Deal
	StatsConsumers   *Consumers
	ResultsConsumers *ResultsConsumers
}

func NewDealsManager(config *configPkg.ExchangeConfig) (*DealsManager, error) {
	exchangeDB, err := dealRepoPkg.NewExchangeDB(nil, config)
	if err != nil {
		return nil, err
	}
	return &DealsManager{
		ER: exchangeDB,
		StatsConsumers: &Consumers{
			Mux:      &sync.RWMutex{},
			Channels: map[chan dealPkg.OHLCV]struct{}{},
		},
		ResultsConsumers: &ResultsConsumers{
			Mux:      &sync.RWMutex{},
			Channels: make(map[int64]chan dealPkg.Deal),
		},
	}, nil
}

func (dm *DealsManager) CreateOrder(order *dealPkg.Order) (int64, error) {
	return dm.ER.AddOrder(order)
}

func (dm *DealsManager) CancelOrder(dealID int64) error {
	return dm.ER.DeleteOrder(dealID)
}

func (dm *DealsManager) ProcessingTradingOperations(IntervalSeconds int) {
	tiker := time.NewTicker(time.Duration(IntervalSeconds) * time.Second)
	stats := make(map[string]*dealPkg.OHLCV, 0)
	var ohclvID int64

	for {
		select {
		case <-tiker.C:
			for _, ohlcv := range stats {
				for ch := range dm.StatsConsumers.Channels {
					ch <- *ohlcv
				}
				fmt.Println(ohlcv)
			}
			stats = make(map[string]*dealPkg.OHLCV, 0)
		case deal := <-dm.DealsFlowCh:
			calculateStats(stats, deal, ohclvID)

			ordersForClose, err := dm.ER.GetOrdersForClose(deal.Ticker, deal.Price)
			if err != nil {
				fmt.Printf("error get orders for close: %v", err.Error())
			}
			if len(ordersForClose) == 0 {
				continue
			}
			allVolume := deal.Volume
			for _, orderForClose := range ordersForClose {
				var volumeToClose int32
				if allVolume >= orderForClose.Volume {
					allVolume -= orderForClose.Volume
					volumeToClose = orderForClose.Volume
				} else {
					//закрыть частично и попробовать закрыть следующим объемом
					volumeToClose = allVolume
					allVolume = 0
				}
				err := dm.makeDeal(orderForClose, volumeToClose)
				if err != nil {
					fmt.Printf("not close deal: %v", err.Error())
				}
				if allVolume == 0 {
					break
				}
			}
		}
	}
}

func (dm *DealsManager) makeDeal(order *dealPkg.Order, volume int32) error {
	order.Volume = volume
	deal, err := dm.ER.MakeDeal(order, volume)
	if err != nil {
		return err
	}
	chToBroker, ok := dm.ResultsConsumers.Channels[int64(order.BrokerID)]
	if ok {
		chToBroker <- *deal
	}

	return nil
}

func (dm *DealsManager) MarkDealShipped(dealID int64) error {
	return dm.ER.MarkDealShipped(dealID)
}

func calculateStats(stats map[string]*dealPkg.OHLCV, deal *dealPkg.Deal, ohclvID int64) {
	ohlcv, ok := stats[deal.Ticker]
	if !ok {
		ohclvID++
		ohlcv = &dealPkg.OHLCV{
			Ticker:   deal.Ticker,
			Time:     int32(time.Now().Unix()),
			Interval: 1,
			Open:     deal.Price,
			Low:      deal.Price,
			ID:       ohclvID,
		}
		stats[deal.Ticker] = ohlcv
	}
	ohlcv.Volume += deal.Volume
	ohlcv.Close = deal.Price
	if ohlcv.Low > deal.Price {
		ohlcv.Low = deal.Price
	}
	if ohlcv.High < deal.Price {
		ohlcv.High = deal.Price
	}
}
