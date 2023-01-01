package main

import (
	"context"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"

	configPkg "github.com/KeynihAV/exchange/pkg/config"
	dealDeliveryPkg "github.com/KeynihAV/exchange/pkg/exchange/deal/delivery"
	dealsFlowDeliveryPkg "github.com/KeynihAV/exchange/pkg/exchange/dealsFlow/delivery"
	"google.golang.org/grpc"
)

var appName = "exchange"

func main() {
	ctx, finish := context.WithCancel(context.Background())
	defer finish()

	exConfig := &configPkg.Config{}
	configPkg.Read(appName, exConfig)
	filePath, err := filepath.Abs(exConfig.Exchange.DealsFlowFile)
	if err != nil {
		log.Fatalf("error get abs path")
	}
	exConfig.Exchange.DealsFlowFile = filePath

	err = StartExchange(ctx, exConfig)
	if err != nil {
		log.Fatalf("Ошибка запуска: %v", err)
	}
}

func StartExchange(ctx context.Context, config *configPkg.Config) error {
	lc := net.ListenConfig{}
	lis, err := lc.Listen(ctx, "tcp", ":"+strconv.Itoa(config.HTTP.Port))
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer()
	exchangeServer, err := dealDeliveryPkg.NewExchangeServer(config)
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
	go dealsFlowDeliveryPkg.StartFlow(file, exchangeServer.DealsManager.DealsFlowCh)

	go exchangeServer.DealsManager.ProcessingTradingOperations(config.Exchange.TradingInterval)

	err = grpcServer.Serve(lis)

	return err
}
