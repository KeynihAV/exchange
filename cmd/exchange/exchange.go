package main

import (
	"context"
	"log"
	"net"
	"os"
	"path/filepath"

	configPkg "github.com/KeynihAV/exchange/pkg/exchange/config"
	dealDeliveryPkg "github.com/KeynihAV/exchange/pkg/exchange/deal/delivery"
	dealsFlowDeliveryPkg "github.com/KeynihAV/exchange/pkg/exchange/dealsFlow/delivery"
	"google.golang.org/grpc"
)

func main() {
	ctx, finish := context.WithCancel(context.Background())
	defer finish()

	filePath, err := filepath.Abs("../../assets/SPFB.RTS_190517_190517.txt")
	if err != nil {
		log.Fatalf("error get abs path")
	}
	ExConfig := &configPkg.ExchangeConfig{
		ListenAddr:      ":8081",
		PGConnString:    "user=postgres password=123Qwer host=192.168.1.188 port=5432 sslmode=disable",
		DealsFlowFile:   filePath,
		TradingInterval: 1,
	}
	err = StartExchange(ctx, ExConfig)
	if err != nil {
		log.Fatalf("Ошибка запуска: %v", err)
	}
}

func StartExchange(ctx context.Context, config *configPkg.ExchangeConfig) error {
	lc := net.ListenConfig{}
	lis, err := lc.Listen(ctx, "tcp", config.ListenAddr)
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

	file, err := os.Open(config.DealsFlowFile)
	if err != nil {
		return err
	}
	go dealsFlowDeliveryPkg.StartFlow(file, exchangeServer.DealsManager.DealsFlowCh)

	go exchangeServer.DealsManager.ProcessingTradingOperations(config.TradingInterval)

	err = grpcServer.Serve(lis)

	return err
}
