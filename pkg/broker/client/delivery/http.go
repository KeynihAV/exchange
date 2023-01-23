package delivery

import (
	"net/http"

	clientPkg "github.com/KeynihAV/exchange/pkg/broker/client"
	clientUsecasekg "github.com/KeynihAV/exchange/pkg/broker/client/usecase"
	"github.com/KeynihAV/exchange/pkg/common"
)

type ClientsHandler struct {
	ClientsManager *clientUsecasekg.ClientsManager
}

func (h *ClientsHandler) GetBalance(w http.ResponseWriter, r *http.Request) {
	inputClient := &clientPkg.Client{}
	ok := common.GetStructFromRequest(inputClient, r, w)
	if !ok {
		return
	}

	positions, err := h.ClientsManager.GetBalance(inputClient)
	if err != nil {
		common.RespJSONError(w, http.StatusInternalServerError, err, err.Error(), r.Context())
		return
	}
	common.WriteStructToResponse(positions, r.Context(), w)
}
