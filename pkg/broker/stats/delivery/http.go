package delivery

import (
	"net/http"

	statsRepoPkg "github.com/KeynihAV/exchange/pkg/broker/stats/repo"
	"github.com/KeynihAV/exchange/pkg/common"
	"github.com/gorilla/mux"
)

type StatsHandler struct {
	StatsRepo *statsRepoPkg.StatsRepo
}

func (sh *StatsHandler) GeStatsByTicker(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	stats, err := sh.StatsRepo.GeStatsByTicker(vars["ticker"])
	if err != nil {
		common.RespJSONError(w, http.StatusInternalServerError, err, err.Error(), ctx)
		return
	}
	common.WriteStructToResponse(stats, ctx, w)
}
