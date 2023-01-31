package common

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	. "github.com/KeynihAV/exchange/pkg/logging"
)

type MyResponse struct {
	Body  interface{} `json:"body,omitempty"`
	Error string      `json:"error,omitempty"`
}

func RespJSONError(w http.ResponseWriter, status int, err error, resp string, ctx context.Context) {
	if err != nil {
		Sl(ctx).Error(err.Error())
	}
	w.WriteHeader(status)
	w.Header().Add("Content-Type", "application/json")
	respJSON, _ := json.Marshal(&MyResponse{
		Error: resp,
	})
	w.Write(respJSON)
}

func GetStructFromRequest(in interface{}, r *http.Request, w http.ResponseWriter) bool {
	body, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	ctx := r.Context()
	if err != nil {
		errTxt := fmt.Sprintf("request body read error: %v", body)
		RespJSONError(w, http.StatusBadRequest, err, errTxt, ctx)
		return false
	}

	err = json.Unmarshal(body, in)
	if err != nil {
		errTxt := fmt.Sprintf("json parsing error %v", err)
		RespJSONError(w, http.StatusBadRequest, err, errTxt, ctx)
		return false
	}
	return true
}

func GetStructFromResponse(in interface{}, resp *http.Response) error {
	body, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		return err
	}
	if len(body) == 0 && resp.StatusCode == http.StatusOK {
		return nil
	}
	myResp := &MyResponse{Body: in}
	err = json.Unmarshal(body, myResp)
	if err != nil {
		return fmt.Errorf("error parsing response: %v, status: %v, txt: %v", err, resp.StatusCode, string(body))
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf(myResp.Error)
	}
	return nil
}

func WriteStructToResponse(in interface{}, ctx context.Context, w http.ResponseWriter) bool {
	w.Header().Set("Content-type", "application/json")
	respJson, err := json.Marshal(&MyResponse{
		Body: in,
	})

	if err != nil {
		errTxt := fmt.Sprintf("json marshal error: %v", err.Error())
		RespJSONError(w, http.StatusInternalServerError, err, errTxt, ctx)
		return false
	}
	w.Write(respJson)

	return true
}
