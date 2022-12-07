package bkex

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

const (
	apiBase        = "https://api.bkex.com"
	apiSymbols     = "/v2/common/symbols"
	apiBalance     = "/v2/u/account/balance"
	apiOrderCreate = "/v2/u/order/create"
	apiOrderCancel = "/v2/u/order/cancel"
	apiOpenOrders  = "/v2/u/order/openOrders"
	apiDepth       = "/v2/q/depth"
)

type API struct {
	baseURL string
	options gateway.Options
	client  *utils.HttpClient
}

func NewAPI(options gateway.Options, baseURL string) *API {
	client := utils.NewHttpClient()
	client.UseProxies(options.Proxies)

	return &API{
		baseURL: baseURL,
		options: options,
		client:  client,
	}
}

type APIResponse struct {
	Code int64           `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data"`
}

type APISymbol struct {
	Symbol             string  `json:"symbol"`
	MinimumOrderSize   float64 `json:"minimumOrderSize"`
	MinimumTradeVolume float64 `json:"minimumTradeVolume"`
	PricePrecision     int64   `json:"pricePrecision"`
	VolumePrecision    int64   `json:"volumePrecision"`
	SupportTrade       bool    `json:"supportTrade"`
}

func (a *API) Symbols() (res []APISymbol, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiSymbols, nil, false)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

type APIBalance struct {
	Currency  string  `json:"currency"`
	Available float64 `json:"available"`
	Total     float64 `json:"total"`
}

func (a *API) Balance() (res map[string][]APIBalance, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiBalance, nil, true)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

func (a *API) CreateOrder(params map[string]string) (res string, err error) {
	req, err := a.newHttpRequest(http.MethodPost, apiOrderCreate, params, true)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

func (a *API) CancelOrder(orderID string) (res string, err error) {
	params := make(map[string]string)
	params["orderId"] = orderID

	req, err := a.newHttpRequest(http.MethodPost, apiOrderCancel, params, true)
	if err != nil {
		return "", err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

type APIPageRes struct {
	Data  json.RawMessage `json:"data"`
	Total int             `json:"total"`
}

type APIOrder struct {
	ID          string  `json:"id"`
	DealVolume  float64 `json:"dealVolume"`
	Direction   string  `json:"direction"`
	Price       float64 `json:"price"`
	Status      int     `json:"status"`
	Symbol      string  `json:"symbol"`
	TotalVolume float64 `json:"totalVolume"`
	Type        string  `json:"type"`
}

func (a *API) OpenOrders(params map[string]string) (orders []APIOrder, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiOpenOrders, params, true)
	if err != nil {
		return orders, err
	}

	var res APIPageRes
	err = a.makeHttpRequest(req, &res)
	if err != nil {
		return orders, err
	}

	err = json.Unmarshal(res.Data, &orders)
	return orders, err
}

type APIDepth struct {
	Bid []gateway.PriceArray `json:"bid"`
	Ask []gateway.PriceArray `json:"ask"`
}

func (a *API) Depth(params map[string]string) (depth APIDepth, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiDepth, params, false)
	if err != nil {
		return depth, err
	}

	err = a.makeHttpRequest(req, &depth)
	return depth, err
}

func (a *API) newHttpRequest(method string, path string, params map[string]string, signed bool) (*http.Request, error) {
	var body io.Reader
	par := url.Values{}
	if method == http.MethodPost && params != nil {
		for k, v := range params {
			par.Set(k, v)
		}
		body = strings.NewReader(par.Encode())
	}

	uri := a.baseURL + path
	req, err := http.NewRequest(method, uri, body)
	if err != nil {
		return nil, err
	}

	if method == http.MethodGet {
		for k, v := range params {
			par.Set(k, v)
		}
		req.URL.RawQuery = par.Encode()
	}

	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}

	if signed {
		data := par.Encode()

		h := hmac.New(sha256.New, []byte(a.options.ApiSecret))
		h.Write([]byte(data))
		signature := hex.EncodeToString(h.Sum(nil))

		req.Header.Set("X_ACCESS_KEY", a.options.ApiKey)
		req.Header.Set("X_SIGNATURE", signature)
	}

	return req, nil
}

func (a *API) makeHttpRequest(req *http.Request, responseObject interface{}) error {
	res, err := a.client.SendRequest(req)
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

	if apiRes.Code != 0 {
		return fmt.Errorf("request to api [%s] failed, returned error code [%d], body [%s]", req.URL, apiRes.Code, string(body))
	}

	if responseObject != nil {
		err = json.Unmarshal(apiRes.Data, responseObject)
		if err != nil {
			return fmt.Errorf("failed to unmarshal data [%s] into responseObject, err: %s", string(body), err)
		}
	}

	return nil
}
