package delivery

import (
	"net/http"
	"net/url"
	"strconv"

	"github.com/KeynihAV/exchange/pkg/broker/session/usecase"
	"github.com/KeynihAV/exchange/pkg/config"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/vk"
)

type SessionHandler struct {
	SessionManager *usecase.SessionsManager
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
		http.Error(w, "cannot parse query "+err.Error(), 500)
		return
	}
	token, err := oConf.Exchange(ctx, qParams.Get("code"))
	if err != nil {
		http.Error(w, "cannot exchange "+err.Error(), http.StatusInternalServerError)
		return
	}
	userID, err := strconv.ParseInt(qParams.Get("state"), 10, 64)
	if err != nil {
		http.Error(w, "cannot parse state "+err.Error(), http.StatusBadRequest)
		return
	}
	_, err = h.SessionManager.CreateSession(userID, token.Expiry)
	if err != nil {
		http.Error(w, "cannot create session "+err.Error(), http.StatusInternalServerError)
		return
	}
}

func StartWebServer(sessHandler *SessionHandler) error {
	http.HandleFunc("/user/login_oauth", sessHandler.AuthCallback)

	err := http.ListenAndServe(":"+strconv.Itoa(sessHandler.Config.Bot.Auth.Port), nil)
	if err != nil {
		return err
	}
	return nil
}
