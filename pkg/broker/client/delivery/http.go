package delivery

import (
	"net/http"
	"strconv"

	clientUsecasekg "github.com/KeynihAV/exchange/pkg/broker/client/usecase"
	"github.com/KeynihAV/exchange/pkg/common"
	"github.com/gorilla/mux"
)

type ClientsHandler struct {
	ClientsManager *clientUsecasekg.ClientsManager
}

func (h *ClientsHandler) GetBalance(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	clientID, err := strconv.Atoi(vars["client"])

	positions, err := h.ClientsManager.GetBalance(clientID)
	if err != nil {
		common.RespJSONError(w, http.StatusInternalServerError, err, err.Error(), r.Context())
		return
	}
	common.WriteStructToResponse(positions, r.Context(), w)
}
