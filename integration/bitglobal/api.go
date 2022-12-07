package bitglobal

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
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

const (
	apiBase            = "https://global-openapi.bithumb.pro/openapi/v1"
	apiSpotConfig      = apiBase + "/spot/config"
	apiSpotAssetList   = apiBase + "/spot/assetList"
	apiSpotOpenOrders  = apiBase + "/spot/openOrders"
	apiSpotPlaceOrder  = apiBase + "/spot/placeOrder"
	apiSpotCancelOrder = apiBase + "/spot/cancelOrder"
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
	Code int64            `json:"code,string"`
	Msg  string           `json:"msg"`
	Data *json.RawMessage `json:"data,omitempty"`
}

type APISpotConfig struct {
	SpotConfig []APISpotMarket `json:"spotConfig"`
}

type APISpotMarket struct {
	Symbol   string        `json:"symbol"`
	Accuracy []json.Number `json:"accuracy"`
}

func (a *API) SpotConfig() (res APISpotConfig, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiSpotConfig, nil, false)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

type APIAsset struct {
	CoinType string  `json:"coinType"`
	Count    float64 `json:"count,string"`
	Frozen   float64 `json:"frozen,string"`
}

type APIAssetList []APIAsset

func (a *API) SpotAssetsList() (res APIAssetList, err error) {
	params := make(map[string]string)
	params["assetType"] = "spot"

	req, err := a.newHttpRequest(http.MethodPost, apiSpotAssetList, params, true)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

type APIPlaceOrderRes struct {
	OrderID string `json:"orderId"`
}

func (a *API) SpotPlaceOrder(params map[string]string) (string, error) {
	req, err := a.newHttpRequest(http.MethodPost, apiSpotPlaceOrder, params, true)
	if err != nil {
		return "", err
	}

	var res APIPlaceOrderRes
	err = a.makeHttpRequest(req, &res)
	if err != nil {
		return "", err
	}

	return res.OrderID, err
}

func (a *API) SpotCancelOrder(symbol string, orderID string) error {
	params := make(map[string]string)
	params["symbol"] = symbol
	params["orderId"] = orderID

	req, err := a.newHttpRequest(http.MethodPost, apiSpotCancelOrder, params, true)
	if err != nil {
		return err
	}

	return a.makeHttpRequest(req, nil)
}

type APIOrderList struct {
	Num  int64      `json:"num"`
	List []APIOrder `json:"list"`
}

type APIOrder struct {
	AvgPrice   json.Number `json:"avgPrice"`
	OrderID    string      `json:"orderId"`
	Price      float64     `json:"price,string"`
	Quantity   float64     `json:"quantity,string"`
	Side       string      `json:"side"`
	Status     string      `json:"status"`
	Symbol     string      `json:"symbol"`
	TradeTotal float64     `json:"tradeTotal,string"`
	TradedNum  float64     `json:"tradedNum,string"`
	Type       string      `json:"type"`
}

func (a *API) SpotOpenOrders(symbol string) ([]APIOrder, error) {
	params := make(map[string]string)
	params["symbol"] = symbol

	req, err := a.newHttpRequest(http.MethodPost, apiSpotOpenOrders, params, true)
	if err != nil {
		return []APIOrder{}, err
	}

	var res APIOrderList
	err = a.makeHttpRequest(req, &res)
	return res.List, err
}

func (a *API) newHttpRequest(method string, uri string, params map[string]string, signed bool) (*http.Request, error) {
	if signed {
		if params == nil {
			params = make(map[string]string)
		}

		timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)

		params["apiKey"] = a.options.ApiKey
		params["timestamp"] = timestamp

		// Generate encoded params signature str
		var keys []string
		for k := range params {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var preSign string
		for _, k := range keys {
			preSign += k + "=" + params[k] + "&"
		}
		preSign = strings.TrimSuffix(preSign, "&")

		h := hmac.New(sha256.New, []byte(a.options.ApiSecret))
		h.Write([]byte(preSign))
		signature := hex.EncodeToString(h.Sum(nil))

		params["signature"] = signature
	}

	var body io.Reader
	if method == http.MethodPost && params != nil {
		dataParams, err := json.Marshal(params)
		if err != nil {
			return nil, fmt.Errorf("marshal params err: %s", err)
		}

		body = bytes.NewReader(dataParams)
	}

	req, err := http.NewRequest(method, uri, body)
	if err != nil {
		return nil, err
	}

	if method == http.MethodGet && params != nil {
		par := url.Values{}
		for k, v := range params {
			par.Set(k, v)
		}
		req.URL.RawQuery = par.Encode()
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

	if apiRes.Code != 0 && apiRes.Code != 200 {
		return fmt.Errorf("request to api [%s] failed, returned error code [%d], body [%s]", req.URL, apiRes.Code, string(body))
	}

	if responseObject != nil {
		var data []byte
		if apiRes.Data != nil {
			data = *apiRes.Data
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
