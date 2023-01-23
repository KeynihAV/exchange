package repo

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"time"

	clientPkg "github.com/KeynihAV/exchange/pkg/broker/client"
	"github.com/KeynihAV/exchange/pkg/common"
	"github.com/KeynihAV/exchange/pkg/config"
)

type ClientsRepo struct {
	HttpClient *http.Client
	config     *config.Config
}

func NewClientsRepo(config *config.Config) *ClientsRepo {
	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns: 100,
	}

	return &ClientsRepo{
		HttpClient: &http.Client{
			Timeout:   time.Second * 10,
			Transport: transport,
		},
		config: config}
}

func (cr *ClientsRepo) CheckAuth(login string, userID int64) (*clientPkg.Client, error) {
	method := "/api/v1/checkAuth"
	currentClient := clientPkg.Client{Login: login, ChatID: userID}
	reqData, err := json.Marshal(currentClient)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodPost, cr.config.Bot.BrokerEndpoint+method, bytes.NewBuffer(reqData))
	if err != nil {
		return nil, err
	}

	resp, err := cr.HttpClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}

	clientFromBroker := &clientPkg.Client{}
	err = common.GetStructFromResponse(clientFromBroker, resp)
	if err != nil {
		return nil, err
	}

	return clientFromBroker, nil
}

func (cr *ClientsRepo) GetBalance(client *clientPkg.Client) ([]*clientPkg.Position, error) {
	method := "/api/v1/status"

	reqData, err := json.Marshal(client)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodGet, cr.config.Bot.BrokerEndpoint+method, bytes.NewBuffer(reqData))
	if err != nil {
		return nil, err
	}

	resp, err := cr.HttpClient.Do(req)
	if err != nil {
		return nil, err
	}

	positions := make([]*clientPkg.Position, 0)
	err = common.GetStructFromResponse(&positions, resp)
	if err != nil {
		return nil, err
	}

	return positions, nil
}
