package config

type ExchangeConfig struct {
	ListenAddr      string
	PGConnString    string
	DealsFlowFile   string
	TradingInterval int
}
