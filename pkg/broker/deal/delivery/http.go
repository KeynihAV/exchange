package delivery

import (
	"net/http"
	"strconv"

	"github.com/KeynihAV/exchange/pkg/common"
	"github.com/KeynihAV/exchange/pkg/config"
	dealPkg "github.com/KeynihAV/exchange/pkg/exchange/deal"
	"github.com/gorilla/mux"
)

type DealsHandler struct {
	DealsManager DMInterface
	Config       *config.Config
}

type DMInterface interface {
	CancelOrder(orderID int64, config *config.Config) error
	OrdersByClient(clientID int) ([]*dealPkg.Order, error)
	CreateOrder(order *dealPkg.Order, config *config.Config) (int64, error)
}

func (h *DealsHandler) OrdersByClient(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	clientID, err := strconv.Atoi(vars["client"])
	if err != nil {
		common.RespJSONError(w, http.StatusBadRequest, err, err.Error(), r.Context())
		return
	}

	positions, err := h.DealsManager.OrdersByClient(clientID)
	if err != nil {
		common.RespJSONError(w, http.StatusInternalServerError, err, err.Error(), r.Context())
		return
	}
	common.WriteStructToResponse(positions, r.Context(), w)
}

func (h *DealsHandler) CreateOrder(w http.ResponseWriter, r *http.Request) {
	order := &dealPkg.Order{}
	ok := common.GetStructFromRequest(order, r, w)
	if !ok {
		return
	}

	orderID, err := h.DealsManager.CreateOrder(order, h.Config)
	order.ID = orderID
	if err != nil {
		common.RespJSONError(w, http.StatusInternalServerError, err, err.Error(), r.Context())
		return
	}
	common.WriteStructToResponse(order, r.Context(), w)
}

func (h *DealsHandler) CancelOrder(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	orderID, err := strconv.ParseInt(vars["order"], 10, 64)
	if err != nil {
		common.RespJSONError(w, http.StatusBadRequest, err, err.Error(), r.Context())
		return
	}

	err = h.DealsManager.CancelOrder(orderID, h.Config)
	if err != nil {
		common.RespJSONError(w, http.StatusInternalServerError, err, err.Error(), r.Context())
		return
	}
}
