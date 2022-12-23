package delivery

import (
	"context"
	"fmt"
	"io"

	"github.com/KeynihAV/exchange/pkg/broker/config"
	statsRepoPkg "github.com/KeynihAV/exchange/pkg/broker/stats/repo"
	dealDeliveryPkg "github.com/KeynihAV/exchange/pkg/exchange/deal/delivery"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func ConsumeStats(statsRepo *statsRepoPkg.StatsRepo, config *config.Config) error {
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
	statsStream, err := exchClient.Statistic(metadata.NewOutgoingContext(ctx, md), &dealDeliveryPkg.BrokerID{ID: int64(config.BrokerID)})
	if err != nil {
		fmt.Printf("error get stats stream %v\n", err)
	}

	for {
		stat, err := statsStream.Recv()
		if err != nil && err != io.EOF {
			fmt.Printf("unexpected error %v\n", err)
		} else if err == io.EOF {
			break
		}
		err = statsRepo.Add(stat)
		if err != nil {
			fmt.Printf("Error write stats: %v\n", err)
		}
	}
	return nil
}
