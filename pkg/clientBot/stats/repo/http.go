package repo

import (
	"net"
	"net/http"
	"time"

	statsPkg "github.com/KeynihAV/exchange/pkg/broker/stats"
	"github.com/KeynihAV/exchange/pkg/common"
	"github.com/KeynihAV/exchange/pkg/config"
)

type StatsRepo struct {
	HttpClient *http.Client
	config     *config.Config
}

func NewStatsRepo(config *config.Config) *StatsRepo {
	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns: 100,
	}

	return &StatsRepo{
		HttpClient: &http.Client{
			Timeout:   time.Second * 10,
			Transport: transport,
		},
		config: config}
}

func (cr *StatsRepo) GeStatsByTicker(ticker string) ([]*statsPkg.OHLCV, error) {
	method := "/api/v1/stats/"

	req, err := http.NewRequest(http.MethodGet, cr.config.Bot.BrokerEndpoint+method+ticker, nil)
	if err != nil {
		return nil, err
	}

	resp, err := cr.HttpClient.Do(req)
	if err != nil {
		return nil, err
	}

	stats := make([]*statsPkg.OHLCV, 0)
	err = common.GetStructFromResponse(&stats, resp)
	if err != nil {
		return nil, err
	}

	return stats, nil
}
