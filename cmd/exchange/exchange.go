package main

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strconv"

	configPkg "github.com/KeynihAV/exchange/pkg/config"
	dealDeliveryPkg "github.com/KeynihAV/exchange/pkg/exchange/deal/delivery"
	dealsFlowDeliveryPkg "github.com/KeynihAV/exchange/pkg/exchange/dealsFlow/delivery"
	"github.com/KeynihAV/exchange/pkg/logging"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var appName = "exchange"

func main() {
	logger := logging.New()
	defer logger.Zap.Sync()

	ctx, finish := context.WithCancel(context.Background())
	defer finish()

	exConfig := &configPkg.Config{}
	configPkg.Read(appName, exConfig)
	filePath, err := filepath.Abs(exConfig.Exchange.DealsFlowFile)
	if err != nil {
		logger.Zap.Fatal("not find deals flow file",
			zap.String("logger", "ZAP"),
			zap.String("err: ", err.Error()))
	}
	exConfig.Exchange.DealsFlowFile = filePath

	err = StartExchange(ctx, exConfig, logger)
	if err != nil {
		logger.Zap.Fatal("error starting exchange server",
			zap.String("logger", "ZAP"),
			zap.String("err: ", err.Error()))
	}
}

func StartExchange(ctx context.Context, config *configPkg.Config, logger *logging.Logger) error {
	lc := net.ListenConfig{}
	lis, err := lc.Listen(ctx, "tcp", ":"+strconv.Itoa(config.HTTP.Port))
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer()
	exchangeServer, err := dealDeliveryPkg.NewExchangeServer(config, logger)
	if err != nil {
		return err
	}
	dealDeliveryPkg.RegisterExchangeServer(grpcServer, exchangeServer)

	go func() {
		<-ctx.Done()
		grpcServer.Stop()
	}()

	file, err := os.Open(config.Exchange.DealsFlowFile)
	if err != nil {
		return err
	}
	go dealsFlowDeliveryPkg.StartFlow(file, exchangeServer.DealsManager.DealsFlowCh, logger)

	go exchangeServer.DealsManager.ProcessingTradingOperations(config.Exchange.TradingInterval, logger)

	logger.Zap.Info("starting exchange server",
		zap.String("logger", "ZAP"),
		zap.Int("port", config.HTTP.Port),
	)
	err = grpcServer.Serve(lis)

	return err
}
