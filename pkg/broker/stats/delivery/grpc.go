package delivery

import (
	"context"
	"fmt"
	"io"

	statsPkg "github.com/KeynihAV/exchange/pkg/broker/stats"
	statsRepoPkg "github.com/KeynihAV/exchange/pkg/broker/stats/repo"
	"github.com/KeynihAV/exchange/pkg/config"
	dealDeliveryPkg "github.com/KeynihAV/exchange/pkg/exchange/deal/delivery"
	"github.com/KeynihAV/exchange/pkg/logging"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func ConsumeStats(statsRepo *statsRepoPkg.StatsRepo, config *config.Config, logger *logging.Logger) error {
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
	statsStream, err := exchClient.Statistic(metadata.NewOutgoingContext(ctx, md), &dealDeliveryPkg.BrokerID{ID: int64(config.Broker.ID)})
	if err != nil {
		logger.Zap.Error("get stats stream",
			zap.String("logger", "grpcClient"),
			zap.String("err", err.Error()),
		)
		return err
	}

	for {
		stat, err := statsStream.Recv()
		if err != nil && err != io.EOF {
			fmt.Printf("unexpected error %v\n", err)
		} else if err == io.EOF {
			break
		}
		err = statsRepo.Add(&statsPkg.OHLCV{
			TimeInt:  stat.Time,
			Interval: stat.Interval,
			Open:     stat.Open,
			High:     stat.High,
			Low:      stat.Low,
			Close:    stat.Close,
			Volume:   stat.Volume,
			Ticker:   stat.Ticker,
		})
		if err != nil {
			logger.Zap.Warn("write stats stream",
				zap.String("logger", "grpcClient"),
				zap.String("err", err.Error()),
			)
			continue
		}
	}
	return nil
}
