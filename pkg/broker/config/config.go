package config

type Config struct {
	ListenAddr       string
	BotToken         string
	WebhookURL       string
	PGConnString     string
	ExchangeEndpoint string
	BrokerID         int
	Tickers          []string
}
