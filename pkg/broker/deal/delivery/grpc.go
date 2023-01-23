package delivery

import (
	"context"
	"fmt"
	"io"

	"github.com/KeynihAV/exchange/pkg/config"
	dealPkg "github.com/KeynihAV/exchange/pkg/exchange/deal"
	dealDeliveryPkg "github.com/KeynihAV/exchange/pkg/exchange/deal/delivery"
	"github.com/KeynihAV/exchange/pkg/logging"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type DealsManagerInterface interface {
	DealProcessing(deal *dealPkg.Deal) error
}

func CreateOrder(order *dealPkg.Order, exchClient dealDeliveryPkg.ExchangeClient) (int64, error) {

	deal := &dealDeliveryPkg.Deal{
		BrokerID: order.BrokerID,
		ClientID: order.ClientID,
		Ticker:   order.Ticker,
		Volume:   order.Volume,
		Price:    order.Price,
		Type:     order.Type,
	}

	ctx := context.Background()
	dealResult, err := exchClient.Create(ctx, deal)
	if err != nil {
		return 0, err
	}

	return dealResult.ID, nil
}

func CancelOrder(exchangeID int64, exchClient dealDeliveryPkg.ExchangeClient) error {
	ctx := context.Background()

	dealID := &dealDeliveryPkg.DealID{ID: exchangeID}
	cancelResult, err := exchClient.Cancel(ctx, dealID)
	if err != nil {
		return err
	}

	if !cancelResult.Success {
		return fmt.Errorf("not delete order %v", exchangeID)
	}

	return nil
}

func ConsumeDeals(dmInterface DealsManagerInterface, config *config.Config, logger *logging.Logger) error {
	grcpConn, err := grpc.Dial(
		config.Broker.ExchangeEndpoint,
		grpc.WithInsecure(),
	)
	if err != nil {
		logger.Zap.Error("consume stats dial exchange",
			zap.String("logger", "grpcClient"),
			zap.String("err", err.Error()),
		)
		return err
	}

	ctx := context.Background()
	md := metadata.Pairs()

	exchClient := dealDeliveryPkg.NewExchangeClient(grcpConn)
	resultsStream, err := exchClient.Results(metadata.NewOutgoingContext(ctx, md), &dealDeliveryPkg.BrokerID{ID: int64(config.Broker.ID)})
	if err != nil {
		logger.Zap.Error("get deals stream",
			zap.String("logger", "grpcClient"),
			zap.String("err", err.Error()),
		)
		return err
	}

	for {
		deal, err := resultsStream.Recv()
		if err != nil && err != io.EOF {
			logger.Zap.Warn("unexpected error",
				zap.String("logger", "grpcClient"),
				zap.String("err", err.Error()),
			)
			continue
		} else if err == io.EOF {
			break
		}
		err = dmInterface.DealProcessing(&dealPkg.Deal{
			ClientID: deal.ClientID,
			Ticker:   deal.Ticker,
			Volume:   deal.Volume,
			Partial:  deal.Partial,
			Time:     deal.Time,
			Price:    deal.Price,
			ID:       deal.ID,
			OrderID:  deal.OrderID,
			Type:     deal.Type,
		})
		if err != nil {
			logger.Zap.Warn("write deal stream",
				zap.String("logger", "grpcClient"),
				zap.String("err", err.Error()),
			)
			continue
		}
	}
	return nil
}
