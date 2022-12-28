package delivery

import (
	"context"
	"fmt"
	"io"

	"github.com/KeynihAV/exchange/pkg/broker/config"
	dealPkg "github.com/KeynihAV/exchange/pkg/exchange/deal"
	dealDeliveryPkg "github.com/KeynihAV/exchange/pkg/exchange/deal/delivery"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type DealsManagerInterface interface {
	DealProcessing(deal *dealPkg.Deal) error
}

func CreateOrder(order *dealPkg.Order, config *config.Config) (int64, error) {
	grcpConn, err := grpc.Dial(
		config.ExchangeEndpoint,
		grpc.WithInsecure(),
	)
	if err != nil {
		fmt.Printf("cant connect to grpc: %v", err)
	}
	ctx := context.Background()
	exchClient := dealDeliveryPkg.NewExchangeClient(grcpConn)

	deal := &dealDeliveryPkg.Deal{
		BrokerID: order.BrokerID,
		ClientID: order.ClientID,
		Ticker:   order.Ticker,
		Volume:   order.Volume,
		Price:    order.Price,
		Type:     order.Type,
	}

	dealResult, err := exchClient.Create(ctx, deal)
	if err != nil {
		return 0, err
	}

	return dealResult.ID, nil
}

func CancelOrder(exchangeID int64, config *config.Config) error {
	grcpConn, err := grpc.Dial(
		config.ExchangeEndpoint,
		grpc.WithInsecure(),
	)
	if err != nil {
		fmt.Printf("cant connect to grpc: %v", err)
	}
	ctx := context.Background()
	exchClient := dealDeliveryPkg.NewExchangeClient(grcpConn)

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

func ConsumeDeals(dmInterface DealsManagerInterface, config *config.Config) error {
	grcpConn, err := grpc.Dial(
		config.ExchangeEndpoint,
		grpc.WithInsecure(),
	)
	if err != nil {
		fmt.Printf("cant connect to grpc: %v", err)
	}

	ctx := context.Background()
	md := metadata.Pairs()

	exchClient := dealDeliveryPkg.NewExchangeClient(grcpConn)
	resultsStream, err := exchClient.Results(metadata.NewOutgoingContext(ctx, md), &dealDeliveryPkg.BrokerID{ID: int64(config.BrokerID)})
	if err != nil {
		fmt.Printf("error get stats stream %v\n", err)
	}

	for {
		deal, err := resultsStream.Recv()
		if err != nil && err != io.EOF {
			fmt.Printf("unexpected error %v\n", err)
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
		})
		if err != nil {
			fmt.Printf("Error write deal: %v\n", err)
		}
	}
	return nil
}
