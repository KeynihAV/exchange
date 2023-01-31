package repo

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/KeynihAV/exchange/pkg/common"
	"github.com/KeynihAV/exchange/pkg/config"
	dealPkg "github.com/KeynihAV/exchange/pkg/exchange/deal"
)

type DealsRepo struct {
	HttpClient *http.Client
	config     *config.Config
}

func NewDealsRepo(config *config.Config) *DealsRepo {
	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns: 100,
	}

	return &DealsRepo{
		HttpClient: &http.Client{
			Timeout:   time.Second * 10,
			Transport: transport,
		},
		config: config}
}

func (cr *DealsRepo) OrdersByClient(clientID int) ([]*dealPkg.Order, error) {
	method := "/api/v1/orders/byClient/"

	req, err := http.NewRequest(http.MethodGet, cr.config.Bot.BrokerEndpoint+method+strconv.Itoa(clientID), nil)
	if err != nil {
		return nil, err
	}

	resp, err := cr.HttpClient.Do(req)
	if err != nil {
		return nil, err
	}

	orders := make([]*dealPkg.Order, 0)
	err = common.GetStructFromResponse(&orders, resp)
	if err != nil {
		return nil, err
	}

	return orders, nil
}

func (cr *DealsRepo) CreateOrder(currentOrder *dealPkg.Order) (int64, error) {
	method := "/api/v1/deal"

	reqData, err := json.Marshal(currentOrder)
	if err != nil {
		return 0, err
	}

	req, err := http.NewRequest(http.MethodPost, cr.config.Bot.BrokerEndpoint+method, bytes.NewBuffer(reqData))
	if err != nil {
		return 0, err
	}
	resp, err := cr.HttpClient.Do(req)
	if err != nil {
		return 0, err
	}

	order := &dealPkg.Order{}
	err = common.GetStructFromResponse(order, resp)
	if err != nil {
		return 0, err
	}

	return order.ID, nil
}

func (cr *DealsRepo) CancelOrder(orderID int64) error {
	method := "/api/v1/cancel/"

	req, err := http.NewRequest(http.MethodDelete, cr.config.Bot.BrokerEndpoint+method+strconv.FormatInt(orderID, 10), nil)
	if err != nil {
		return err
	}

	resp, err := cr.HttpClient.Do(req)
	if err != nil {
		return err
	}

	err = common.GetStructFromResponse(struct{}{}, resp)
	if err != nil {
		return err
	}

	return nil
}
