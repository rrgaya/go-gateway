package biconomy

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

const (
	apiBase           = "https://www.biconomy.com"
	apiExchangeInfo   = "/api/v1/exchangeInfo"
	apiBalance        = "/api/v1/private/user"
	apiOrderCreate    = "/api/v1/private/trade/limit"
	apiOrderCancel    = "/api/v1/private/trade/cancel"
	apiOpenOrders     = "/api/v1/private/order/pending"
	apiFinishedOrders = "/api/v1/private/order/finished"
	apiDepth          = "/api/v1/depth?symbol=%s"
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
	Code   int64            `json:"code"`
	Msg    string           `json:"msg"`
	Result *json.RawMessage `json:"result"`
}

type APIExchangeInfo struct {
	Symbol              string `json:"symbol"`
	BaseAsset           string `json:"baseAsset"`
	BaseAssetPrecision  int64  `json:"baseAssetPrecision"`
	QuoteAsset          string `json:"quoteAsset"`
	QuoteAssetPrecision int64  `json:"quoteAssetPrecision"`
}

func (a *API) ExchangeInfo() (res []APIExchangeInfo, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiExchangeInfo, nil, false)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res, false)
	return res, err
}

type APIBalance struct {
	Asset     string
	Available float64 `json:"available"`
	Freeze    float64 `json:"freeze"`
}

type APIUserAsset struct {
	Balances []APIBalance
}

func (a *APIUserAsset) UnmarshalJSON(b []byte) error {
	var res map[string]any

	if err := json.Unmarshal(b, &res); err != nil {
		return err
	}

	balances := make([]APIBalance, 0, len(res))
	for asset, obj := range res {
		switch v := obj.(type) {
		case map[string]interface{}:
			available, err := strconv.ParseFloat(v["available"].(string), 64)
			if err != nil {
				return err
			}
			freeze, err := strconv.ParseFloat(v["freeze"].(string), 64)
			if err != nil {
				return err
			}

			balances = append(balances, APIBalance{
				Asset:     asset,
				Available: available,
				Freeze:    freeze,
			})
		}
	}

	a.Balances = balances

	return nil
}

func (a *API) Balance() (res []APIBalance, err error) {
	req, err := a.newHttpRequest(http.MethodPost, apiBalance, nil, true)
	if err != nil {
		return res, err
	}

	var assetRes APIUserAsset
	err = a.makeHttpRequest(req, &assetRes, true)
	return assetRes.Balances, err
}

func (a *API) CreateOrder(params map[string]string) (res APIOrder, err error) {
	req, err := a.newHttpRequest(http.MethodPost, apiOrderCreate, params, true)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res, true)
	return res, err
}

func (a *API) CancelOrder(params map[string]string) (res APIOrder, err error) {
	req, err := a.newHttpRequest(http.MethodPost, apiOrderCancel, params, true)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res, true)
	return res, err
}

type APIPageRes struct {
	Records json.RawMessage `json:"records"`
}

type APIOrder struct {
	Amount    float64 `json:"amount,string"`
	DealFee   float64 `json:"deal_fee,string"`
	DealMoney float64 `json:"deal_money,string"`
	DealStock float64 `json:"deal_stock,string"`
	ID        int64   `json:"id"`
	Left      float64 `json:"left,string"`
	Market    string  `json:"market"`
	Price     float64 `json:"price,string"`
	Side      int64   `json:"side"`
}

func (a *API) OpenOrders(params map[string]string) (orders []APIOrder, err error) {
	return a.fetchOrders(apiOpenOrders, params)
}

func (a *API) FinishedOrders(params map[string]string) (orders []APIOrder, err error) {
	return a.fetchOrders(apiFinishedOrders, params)
}

func (a *API) fetchOrders(endpoint string, params map[string]string) (orders []APIOrder, err error) {
	req, err := a.newHttpRequest(http.MethodPost, endpoint, params, true)
	if err != nil {
		return orders, err
	}

	var res APIPageRes
	err = a.makeHttpRequest(req, &res, true)
	if err != nil {
		return orders, err
	}

	err = json.Unmarshal(res.Records, &orders)
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

	err = a.makeHttpRequest(req, &depth, true)
	return depth, err
}

func (a *API) newHttpRequest(method string, path string, params map[string]string, signed bool) (*http.Request, error) {
	var body io.Reader

	if signed {
		if params == nil {
			params = make(map[string]string)
		}

		params["api_key"] = a.options.ApiKey

		signParams := url.Values{}
		for k, v := range params {
			signParams.Set(k, v)
		}

		signData := signParams.Encode()
		if signData != "" {
			signData += "&"
		}
		signData += "secret_key=" + a.options.ApiSecret

		hash := md5.Sum([]byte(signData))
		signature := hex.EncodeToString(hash[:])
		signature = strings.ToUpper(signature)

		params["sign"] = signature
	}

	if method == http.MethodPost && params != nil {
		par := url.Values{}
		for k, v := range params {
			par.Set(k, v)
		}
		data := par.Encode()
		body = strings.NewReader(data)
	}

	uri := a.baseURL + path
	req, err := http.NewRequest(method, uri, body)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-SITE-ID", "127")

	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}

	return req, nil
}

func (a *API) makeHttpRequest(req *http.Request, responseObject interface{}, checkAPIRes bool) error {
	res, err := a.client.SendRequest(req)
	if err != nil {
		return err
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}

	var apiRes APIResponse
	if checkAPIRes {
		err = json.Unmarshal(body, &apiRes)
		if err != nil {
			return fmt.Errorf("failed to unmarshal body [%s] into APIResponse, err: %s", string(body), err)
		}

		if apiRes.Code != 0 {
			return fmt.Errorf("request to api [%s] failed, returned error code [%d], body [%s]", req.URL, apiRes.Code, string(body))
		}
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
			return fmt.Errorf("failed to unmarshal data [%s] into responseObject, err: %s", string(body), err)
		}
	}

	return nil
}
