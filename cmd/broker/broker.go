package main

import (
	"database/sql"
	"fmt"
	"log"

	clientDeliveryPkg "github.com/KeynihAV/exchange/pkg/broker/client/delivery"
	clientsUsecasePkg "github.com/KeynihAV/exchange/pkg/broker/client/usecase"
	configPkg "github.com/KeynihAV/exchange/pkg/broker/config"
	statsDeliveryPkg "github.com/KeynihAV/exchange/pkg/broker/stats/delivery"
	statsRepoPkg "github.com/KeynihAV/exchange/pkg/broker/stats/repo"
)

func main() {
	config := &configPkg.Config{
		ListenAddr:       ":8082",
		BotToken:         "5804418153:AAGww9r9ecm9EwIlG4JZk6Q452S5fTiJrWM",
		WebhookURL:       "https://24f4-5-44-170-102.eu.ngrok.io",
		PGConnString:     "user=postgres password=123Qwer host=192.168.1.188 port=5432 sslmode=disable",
		BrokerID:         1,
		ExchangeEndpoint: ":8081",
	}

	db, err := initDB(config)
	if err != nil {
		log.Fatalln(err)
	}

	err = startBroker(db, config)
	if err != nil {
		log.Fatalln(err)
	}
}

func startBroker(db *sql.DB, config *configPkg.Config) error {
	clientsManager, err := clientsUsecasePkg.NewClientsManager(db)
	if err != nil {
		return err
	}
	statsRepo, err := statsRepoPkg.NewStatsRepo(db)
	if err != nil {
		return err
	}

	go statsDeliveryPkg.ConsumeStats(statsRepo, config)

	err = clientDeliveryPkg.StartTgBot(config, clientsManager)

	if err != nil {
		return err
	}
	return nil
}

func initDB(config *configPkg.Config) (*sql.DB, error) {
	dbName := "broker"

	var DBMS *sql.DB
	var err error
	DBMS, err = sql.Open("pgx", config.PGConnString)
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

	brokerDB, err = sql.Open("pgx", fmt.Sprintf("%v dbname=%v", config.PGConnString, dbName))
	if err != nil {
		return nil, err
	}

	err = brokerDB.Ping()
	if err != nil {
		return nil, err
	}

	return brokerDB, nil
}
