package main

import (
	"database/sql"
	"fmt"
	"log"

	clientDeliveryPkg "github.com/KeynihAV/exchange/pkg/broker/client/delivery"
	clientsUsecasePkg "github.com/KeynihAV/exchange/pkg/broker/client/usecase"
	dealDeliveryPkg "github.com/KeynihAV/exchange/pkg/broker/deal/delivery"
	dealUsecasePkg "github.com/KeynihAV/exchange/pkg/broker/deal/usecase"
	statsDeliveryPkg "github.com/KeynihAV/exchange/pkg/broker/stats/delivery"
	statsRepoPkg "github.com/KeynihAV/exchange/pkg/broker/stats/repo"
	configPkg "github.com/KeynihAV/exchange/pkg/config"
)

var appName = "broker"

func main() {
	config := &configPkg.Config{}
	err := configPkg.Read(appName, config)
	if err != nil {
		log.Fatalln(err)
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

	dealsManager, err := dealUsecasePkg.NewDealsManager(db)
	if err != nil {
		return err
	}

	go statsDeliveryPkg.ConsumeStats(statsRepo, config)

	go dealDeliveryPkg.ConsumeDeals(dealsManager, config)

	err = clientDeliveryPkg.StartTgBot(config, clientsManager, statsRepo, dealsManager)

	if err != nil {
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
