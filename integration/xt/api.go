package xt

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

const (
	apiBase           = "https://sapi.xt.com"
	apiSymbols        = apiBase + "/v4/public/symbol"
	apiBalances       = apiBase + "/v4/balances"
	apiNewOrder       = apiBase + "/v4/order"
	apiCancelOrder    = apiBase + "/v4/order/%s"
	apiOpenOrders     = apiBase + "/v4/open-order"
	apiGetBatchOrders = apiBase + "/v4/batch-order"
	apiDepth          = apiBase + "/v4/public/depth"
)

type API struct {
	options    gateway.Options
	httpClient *utils.HttpClient
}

func NewAPI(options gateway.Options) *API {
	client := utils.NewHttpClient()
	client.UseProxies(options.Proxies)

	return &API{
		options:    options,
		httpClient: client,
	}
}

type APIResponse struct {
	RC     int              `json:"rc"`
	MC     string           `json:"mc"`
	Result *json.RawMessage `json:"result,omitempty"`
}

type APISymbolRes struct {
	Version string      `json:"version"`
	Symbols []APISymbol `json:"symbols"`
}

type APISymbol struct {
	Symbol       string `json:"symbol"`
	BaseCurrency string `json:"baseCurrency"`
	Filters      []struct {
		Filter string  `json:"filter"`
		Min    float64 `json:"min,string"`
	} `json:"filters"`
	PricePrecision    int64  `json:"pricePrecision"`
	QuantityPrecision int64  `json:"quantityPrecision"`
	QuoteCurrency     string `json:"quoteCurrency"`
}

func (a *API) GetSymbols() (res APISymbolRes, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiSymbols, nil, false)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

type APIDepth struct {
	Timestamp    int64                `json:"timestamp"`
	LastUpdateID int64                `json:"lastUpdateID"`
	Bids         []gateway.PriceArray `json:"bids"`
	Asks         []gateway.PriceArray `json:"asks"`
}

func (a *API) Depth(params map[string]string) (res APIDepth, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiDepth, params, false)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

type APIAsset struct {
	Currency        string  `json:"currency"`
	TotalAmount     float64 `json:"totalAmount,string"`
	AvailableAmount float64 `json:"availableAmount,string"`
}

type APIAccountAssets map[string]APIAsset

type APIBalanceRes struct {
	Assets []APIAsset `json:"assets"`
}

func (a *API) Balances() (res APIBalanceRes, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiBalances, nil, true)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

type NewOrderRes struct {
	OrderID string `json:"orderId"`
}

func (a *API) NewOrder(params map[string]string) (string, error) {
	req, err := a.newHttpRequest(http.MethodPost, apiNewOrder, params, true)
	if err != nil {
		return "", err
	}

	var res NewOrderRes
	err = a.makeHttpRequest(req, &res)
	if err != nil {
		return "", err
	}

	return res.OrderID, err
}

type CancelOrderRes struct {
	CancelID string `json:"cancelId"`
}

func (a *API) CancelOrder(orderID string) error {
	p := fmt.Sprintf(apiCancelOrder, orderID)
	req, err := a.newHttpRequest(http.MethodDelete, p, nil, true)
	if err != nil {
		return err
	}

	var res CancelOrderRes
	err = a.makeHttpRequest(req, &res)
	if err != nil {
		return err
	}

	return nil
}

type APIOrder struct {
	OrderID      string  `json:"orderId"`
	AvgPrice     float64 `json:"avgPrice,string"`
	BaseCurrency string  `json:"baseCurrency"`
	ExecutedQty  float64 `json:"executedQty,string"`
	Fee          float64 `json:"fee,string"`
	FeeCurrency  string  `json:"feeCurrency"`
	LeavingQty   float64 `json:"leavingQty,string"`
	OrigQty      float64 `json:"origQty,string"`
	Price        float64 `json:"price,string"`
	Side         string  `json:"side"`
	State        string  `json:"state"`
	Symbol       string  `json:"symbol"`
	Type         string  `json:"type"`
}

func (a *API) OpenOrders(params map[string]string) ([]APIOrder, error) {
	req, err := a.newHttpRequest(http.MethodGet, apiOpenOrders, params, true)
	if err != nil {
		return []APIOrder{}, err
	}

	var res []APIOrder
	err = a.makeHttpRequest(req, &res)
	return res, err
}

func (a *API) GetOrdersByIDS(ids []string) ([]APIOrder, error) {
	params := make(map[string]string)
	params["orderIds"] = strings.Join(ids, ",")

	req, err := a.newHttpRequest(http.MethodGet, apiGetBatchOrders, params, true)
	if err != nil {
		return []APIOrder{}, err
	}

	var res []APIOrder
	err = a.makeHttpRequest(req, &res)
	return res, err
}

func (a *API) newHttpRequest(method string, uri string, params map[string]string, signed bool) (*http.Request, error) {
	var bodyData string
	var body io.Reader
	if method == http.MethodPost && params != nil {
		//bodyData = params.Encode()
		//body = strings.NewReader(bodyData)

		payload, err := json.Marshal(params)
		if err != nil {
			return nil, err
		}

		bodyData = string(payload)
		body = bytes.NewReader(payload)
	}

	req, err := http.NewRequest(method, uri, body)
	if err != nil {
		return nil, err
	}

	if method == http.MethodGet && params != nil {
		q := req.URL.Query()
		for k, v := range params {
			q.Set(k, v)
		}
		req.URL.RawQuery = q.Encode()
	}

	var timestamp, signature string
	if signed {
		timestamp = strconv.FormatInt(time.Now().UnixMilli(), 10)

		signHeaders := url.Values{}
		signHeaders.Set("xt-validate-algorithms", "HmacSHA256")
		signHeaders.Set("xt-validate-appkey", a.options.ApiKey)
		signHeaders.Set("xt-validate-recvwindow", "5000")
		signHeaders.Set("xt-validate-timestamp", timestamp)

		signData := signHeaders.Encode()
		signData += "#"
		signData += req.Method
		signData += "#"
		signData += req.URL.Path
		if req.URL.RawQuery != "" {
			signData += "#"
			signData += req.URL.RawQuery
		}
		if bodyData != "" {
			signData += "#"
			signData += bodyData
		}

		h := hmac.New(sha256.New, []byte(a.options.ApiSecret))
		h.Write([]byte(signData))
		signature = hex.EncodeToString(h.Sum(nil))

		req.Header.Set("xt-validate-algorithms", "HmacSHA256")
		req.Header.Set("xt-validate-appkey", a.options.ApiKey)
		req.Header.Set("xt-validate-recvwindow", "5000")
		req.Header.Set("xt-validate-timestamp", timestamp)
		req.Header.Set("xt-validate-signature", signature)
	}

	req.Header.Set("Content-Type", "application/json")

	return req, nil
}

func (a *API) makeHttpRequest(req *http.Request, responseObject interface{}) error {
	res, err := a.httpClient.SendRequest(req)
	if err != nil {
		return err
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}

	var apiRes APIResponse
	err = json.Unmarshal(body, &apiRes)
	if err != nil {
		return fmt.Errorf("failed to unmarshal body [%s] into APIResponse, err: %s", string(body), err)
	}

	if apiRes.MC != "SUCCESS" {
		return fmt.Errorf("request to api [%s] failed, returned error [%s], body [%s]", req.URL, apiRes.MC, string(body))
	}

	if responseObject != nil {
		var data []byte
		if apiRes.Result != nil {
			data = *apiRes.Result
		} else {
			data = body
		}

		err = json.Unmarshal(data, responseObject)
		if err != nil {
			return fmt.Errorf("failed to unmarshal data [%s] into responseObject, err: %s", string(data), err)
		}
	}

	return nil
}
