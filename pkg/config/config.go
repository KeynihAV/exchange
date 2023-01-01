package config

import (
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	HTTP struct {
		Port int
	}
	DB struct {
		Host     string
		Port     int
		Username string
		Password string
		Database string
	}
	Bot struct {
		Token      string
		WebhookURL string
	}
	Broker struct {
		ID               int
		Tickers          []string
		ExchangeEndpoint string
	}
	Exchange struct {
		DealsFlowFile   string
		TradingInterval int
	}
}

func Read(appName string, cfg interface{}) error {
	v := viper.New()

	v.SetConfigName(appName)
	v.AddConfigPath("../../configs/")
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	err := v.ReadInConfig()
	if err != nil {
		return err
	}
	if cfg != nil {
		err := v.Unmarshal(cfg)
		if err != nil {
			return err
		}
	}
	return nil
}
