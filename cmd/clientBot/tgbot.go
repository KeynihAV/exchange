package main

import (
	clientDeliveryPkg "github.com/KeynihAV/exchange/pkg/clientBot/client/delivery"
	clientRepoPkg "github.com/KeynihAV/exchange/pkg/clientBot/client/repo"
	dealRepoPkg "github.com/KeynihAV/exchange/pkg/clientBot/deal/repo"
	statsRepoPkg "github.com/KeynihAV/exchange/pkg/clientBot/stats/repo"
	configPkg "github.com/KeynihAV/exchange/pkg/config"
	"github.com/KeynihAV/exchange/pkg/logging"
	"go.uber.org/zap"
)

var appName = "tgbot"

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

	err = StartTgBot(config, logger)
	if err != nil {
		logger.Zap.Fatal("start tgbot client",
			zap.String("logger", "ZAP"),
			zap.String("err: ", err.Error()))
	}
}

func StartTgBot(config *configPkg.Config, logger *logging.Logger) error {

	clientsRepo := clientRepoPkg.NewClientsRepo(config)
	dealsRepo := dealRepoPkg.NewDealsRepo(config)
	statsRepo := statsRepoPkg.NewStatsRepo(config)

	logger.Zap.Info("starting tgbot client",
		zap.String("logger", "ZAP"),
		zap.Int("port", config.HTTP.Port),
	)

	err := clientDeliveryPkg.StartTgBot(config, clientsRepo, dealsRepo, statsRepo, logger)

	if err != nil {
		logger.Zap.Error("start tgbot",
			zap.String("logger", "tgbot"),
			zap.String("err", err.Error()),
		)
		return err
	}
	return nil
}
