package main

import (
	"database/sql"
	"fmt"

	clientDeliveryPkg "github.com/KeynihAV/exchange/pkg/broker/client/delivery"
	clientsUsecasePkg "github.com/KeynihAV/exchange/pkg/broker/client/usecase"
	dealDeliveryPkg "github.com/KeynihAV/exchange/pkg/broker/deal/delivery"
	dealUsecasePkg "github.com/KeynihAV/exchange/pkg/broker/deal/usecase"
	sessDeliveryPkg "github.com/KeynihAV/exchange/pkg/broker/session/delivery"
	sessUsecasePkg "github.com/KeynihAV/exchange/pkg/broker/session/usecase"
	statsDeliveryPkg "github.com/KeynihAV/exchange/pkg/broker/stats/delivery"
	statsRepoPkg "github.com/KeynihAV/exchange/pkg/broker/stats/repo"
	configPkg "github.com/KeynihAV/exchange/pkg/config"
	"github.com/KeynihAV/exchange/pkg/logging"
	"go.uber.org/zap"
)

var appName = "broker"

func main() {
	logger := logging.New()
	defer logger.Zap.Sync()

	config := &configPkg.Config{}
	err := configPkg.Read(appName, config)
	if err != nil {
		logger.Zap.Fatal("read config",
			zap.String("logger", "ZAP"),
			zap.String("err: ", err.Error()))
	}

	db, err := initDB(config)
	if err != nil {
		logger.Zap.Fatal("init db",
			zap.String("logger", "ZAP"),
			zap.String("err: ", err.Error()))
	}

	err = startBroker(db, config, logger)
	if err != nil {
		logger.Zap.Fatal("start broker",
			zap.String("logger", "ZAP"),
			zap.String("err: ", err.Error()))
	}
}

func startBroker(db *sql.DB, config *configPkg.Config, logger *logging.Logger) error {
	clientsManager, err := clientsUsecasePkg.NewClientsManager(db)
	if err != nil {
		return err
	}
	statsRepo, err := statsRepoPkg.NewStatsRepo(db)
	if err != nil {
		return err
	}

	dealsManager, err := dealUsecasePkg.NewDealsManager(db)
	if err != nil {
		return err
	}

	sessManager, err := sessUsecasePkg.NewSessionsManager(config)
	if err != nil {
		return err
	}

	sessHandler := &sessDeliveryPkg.SessionHandler{
		SessionManager: sessManager,
		Config:         config,
	}
	go sessDeliveryPkg.StartWebServer(sessHandler)

	go statsDeliveryPkg.ConsumeStats(statsRepo, config, logger)

	go dealDeliveryPkg.ConsumeDeals(dealsManager, config, logger)

	logger.Zap.Info("starting broker",
		zap.String("logger", "ZAP"),
		zap.Int("port", config.HTTP.Port),
	)

	err = clientDeliveryPkg.StartTgBot(config, clientsManager, statsRepo, dealsManager, sessManager, logger)

	if err != nil {
		logger.Zap.Error("start tgbot",
			zap.String("logger", "tgbot"),
			zap.String("err", err.Error()),
		)
		return err
	}
	return nil
}

func initDB(config *configPkg.Config) (*sql.DB, error) {
	dbName := "broker"

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

	var brokerDB *sql.DB
	err = DBMS.Close()
	if err != nil {
		return nil, err
	}

	brokerDB, err = sql.Open("pgx", fmt.Sprintf("%v dbname=%v", connString, dbName))
	if err != nil {
		return nil, err
	}

	err = brokerDB.Ping()
	if err != nil {
		return nil, err
	}

	return brokerDB, nil
}
