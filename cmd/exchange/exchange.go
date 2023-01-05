package main

import (
	"context"
	"database/sql"
	"fmt"
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

	db, err := initDB(exConfig)
	if err != nil {
		logger.Zap.Fatal("not init db",
			zap.String("logger", "ZAP"),
			zap.String("err: ", err.Error()))
	}

	err = StartExchange(ctx, db, exConfig, logger)
	if err != nil {
		logger.Zap.Fatal("error starting exchange server",
			zap.String("logger", "ZAP"),
			zap.String("err: ", err.Error()))
	}
}

func StartExchange(ctx context.Context, db *sql.DB, config *configPkg.Config, logger *logging.Logger) error {
	lc := net.ListenConfig{}
	lis, err := lc.Listen(ctx, "tcp", ":"+strconv.Itoa(config.HTTP.Port))
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer()
	exchangeServer, err := dealDeliveryPkg.NewExchangeServer(db, config, logger)
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

func initDB(config *configPkg.Config) (*sql.DB, error) {
	dbName := "exchange"

	connString := fmt.Sprintf("user=%v password=%v host=%v port=%v sslmode=disable",
		config.DB.Username, config.DB.Password, config.DB.Host, config.DB.Port)

	var DBMS *sql.DB
	var err error
	DBMS, err = sql.Open("pgx", connString)
	if err != nil {
		return nil, err
	}

	err = DBMS.Ping()
	if err != nil {
		return nil, err
	}

	rows, err := DBMS.Query(`SELECT 1 FROM pg_database WHERE datname = $1`, dbName)
	if err != nil {
		return nil, err
	}
	if !rows.Next() {
		_, err = DBMS.Exec(fmt.Sprintf(`CREATE DATABASE %v`, dbName))
		if err != nil {
			return nil, err
		}
	}

	var exchangeDB *sql.DB
	err = DBMS.Close()
	if err != nil {
		return nil, err
	}

	exchangeDB, err = sql.Open("pgx", fmt.Sprintf("%v dbname=%v", connString, dbName))
	if err != nil {
		return nil, err
	}

	err = exchangeDB.Ping()
	if err != nil {
		return nil, err
	}

	return exchangeDB, nil
}
