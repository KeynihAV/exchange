package delivery

import (
	"net/http"
	"net/url"
	"strconv"

	clientPkg "github.com/KeynihAV/exchange/pkg/broker/client"
	clientUsecaseRepo "github.com/KeynihAV/exchange/pkg/broker/client/usecase"
	"github.com/KeynihAV/exchange/pkg/broker/session/usecase"
	"github.com/KeynihAV/exchange/pkg/common"
	"github.com/KeynihAV/exchange/pkg/config"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/vk"
)

type SessionHandler struct {
	SessionManager *usecase.SessionsManager
	ClientsManager *clientUsecaseRepo.ClientsManager
	Config         *config.Config
}

func (h *SessionHandler) AuthCallback(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	oConf := oauth2.Config{
		ClientID:     h.Config.Bot.Auth.App_id,
		ClientSecret: h.Config.Bot.Auth.App_key,
		RedirectURL:  h.Config.Bot.Auth.Redirect_uri,
		Endpoint:     vk.Endpoint,
	}
	qParams, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		common.RespJSONError(w, http.StatusBadRequest, err, "cannot parse query", ctx)
		return
	}
	token, err := oConf.Exchange(ctx, qParams.Get("code"))
	if err != nil {
		common.RespJSONError(w, http.StatusInternalServerError, err, "cannot exchange", ctx)
		return
	}
	userID, err := strconv.ParseInt(qParams.Get("state"), 10, 64)
	if err != nil {
		common.RespJSONError(w, http.StatusBadRequest, err, "cannot parse state", ctx)
		return
	}
	_, err = h.SessionManager.CreateSession(userID, token.Expiry)
	if err != nil {
		common.RespJSONError(w, http.StatusBadRequest, err, "cannot create session", ctx)
		return
	}
}

func (h *SessionHandler) CheckAuth(w http.ResponseWriter, r *http.Request) {
	inputClient := &clientPkg.Client{}
	ok := common.GetStructFromRequest(inputClient, r, w)
	if !ok {
		return
	}

	_, err := h.SessionManager.GetSession(inputClient.ChatID)
	if err != nil {
		common.RespJSONError(w, http.StatusNotFound, err, err.Error(), r.Context())
		return
	}
	client, err := h.ClientsManager.CheckAndCreateClient(inputClient.Login, inputClient.ChatID)
	if err != nil {
		common.RespJSONError(w, http.StatusInternalServerError, err, err.Error(), r.Context())
		return
	}

	common.WriteStructToResponse(client, r.Context(), w)
}
