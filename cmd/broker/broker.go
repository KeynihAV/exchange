package main

import (
	"database/sql"
	"fmt"
	"net/http"
	"strconv"

	clientDeliveryPkg "github.com/KeynihAV/exchange/pkg/broker/client/delivery"
	clientsUsecasePkg "github.com/KeynihAV/exchange/pkg/broker/client/usecase"
	dealDeliveryPkg "github.com/KeynihAV/exchange/pkg/broker/deal/delivery"
	dealUsecasePkg "github.com/KeynihAV/exchange/pkg/broker/deal/usecase"
	metricsPkg "github.com/KeynihAV/exchange/pkg/broker/metrics"
	sessDeliveryPkg "github.com/KeynihAV/exchange/pkg/broker/session/delivery"
	sessUsecasePkg "github.com/KeynihAV/exchange/pkg/broker/session/usecase"
	statsDeliveryPkg "github.com/KeynihAV/exchange/pkg/broker/stats/delivery"
	statsRepoPkg "github.com/KeynihAV/exchange/pkg/broker/stats/repo"
	configPkg "github.com/KeynihAV/exchange/pkg/config"
	"github.com/KeynihAV/exchange/pkg/logging"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

var (
	appName = "broker"
)

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

	dealsManager, err := dealUsecasePkg.NewDealsManager(db, config)
	if err != nil {
		return err
	}

	sessManager, err := sessUsecasePkg.NewSessionsManager(config)
	if err != nil {
		return err
	}

	sessHandler := &sessDeliveryPkg.SessionHandler{
		SessionManager: sessManager,
		ClientsManager: clientsManager,
		Config:         config,
	}

	go statsDeliveryPkg.ConsumeStats(statsRepo, config, logger)

	go dealDeliveryPkg.ConsumeDeals(dealsManager, config, logger)

	logger.Zap.Info("starting broker",
		zap.String("logger", "ZAP"),
		zap.Int("port", config.HTTP.Port),
	)

	clientsHandler := clientDeliveryPkg.ClientsHandler{ClientsManager: clientsManager}
	statsHandler := statsDeliveryPkg.StatsHandler{StatsRepo: statsRepo}
	dealsHandler := dealDeliveryPkg.DealsHandler{DealsManager: dealsManager, Config: config}

	r := mux.NewRouter()
	r.HandleFunc("/api/v1/stats/{ticker}", statsHandler.GeStatsByTicker).Methods("GET")
	r.HandleFunc("/api/v1/deal", dealsHandler.CreateOrder).Methods("POST")
	r.HandleFunc("/api/v1/cancel/{order}", dealsHandler.CancelOrder).Methods("DELETE")
	r.HandleFunc("/api/v1/orders/byClient/{client}", dealsHandler.OrdersByClient).Methods("GET")
	r.HandleFunc("/api/v1/status", clientsHandler.GetBalance).Methods("GET")
	r.HandleFunc("/api/v1/checkAuth", sessHandler.CheckAuth).Methods("POST")
	r.HandleFunc("/api/v1/user/login_oauth", sessHandler.AuthCallback).Methods("GET")
	r.HandleFunc("/metrics", promhttp.Handler().ServeHTTP)

	mux := logger.WriteAccessLog(r)
	mux = logger.SetupLogger(mux)
	mux = logger.AddReqID(mux)
	mux = metricsPkg.TimeTrackingMiddleware(mux)

	err = http.ListenAndServe(":"+strconv.Itoa(config.HTTP.Port), mux)
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
