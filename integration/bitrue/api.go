package bitrue

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
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
	apiBase         = "https://open.bitrue.com"
	apiExchangeInfo = apiBase + "/api/v1/exchangeInfo"
	apiDepth        = apiBase + "/api/v1/depth"
	apiAccountInfo  = apiBase + "/api/v1/account"
	apiOpenOrders   = apiBase + "/api/v1/openOrders"
	apiOrder        = apiBase + "/api/v1/order"
	apiNewListenKey = apiBase + "/poseidon/api/v1/listenKey"
	apiListenKey    = apiBase + "/poseidon/api/v1/listenKey/%s"
)

type API struct {
	options gateway.Options
	client  *utils.HttpClient
}

func NewAPI(options gateway.Options) *API {
	client := utils.NewHttpClient()
	client.UseProxies(options.Proxies)

	return &API{
		options: options,
		client:  client,
	}
}

type APIResponse struct {
	Code int64            `json:"code"`
	Msg  string           `json:"msg"`
	Data *json.RawMessage `json:"data,omitempty"`
}

type APIExchangeInfo struct {
	ServerTime int64       `json:"serverTime"`
	Symbols    []APISymbol `json:"symbols"`
}

type APISymbol struct {
	Symbol              string      `json:"symbol"`
	Status              string      `json:"status"`
	BaseAsset           string      `json:"baseAsset"`
	BaseAssetPrecision  int64       `json:"baseAssetPrecision"`
	QuoteAsset          string      `json:"quoteAsset"`
	QuoteAssetPrecision int64       `json:"quoteAssetPrecision"`
	QuotePrecision      int64       `json:"quotePrecision"`
	Filters             []APIFilter `json:"filters"`
}

type APIFilter struct {
	FilterType string  `json:"filterType"`
	MaxPrice   float64 `json:"maxPrice,string"`
	MaxQty     float64 `json:"maxQty,string"`
	MinPrice   float64 `json:"minPrice,string"`
	MinQty     float64 `json:"minQty,string"`
	StepSize   float64 `json:"stepSize,string"`
	TickSize   float64 `json:"tickSize,string"`
	MinVal     float64 `json:"minVal,string"`
}

func (a *API) ExchangeInfo() (APIExchangeInfo, error) {
	req, err := a.newHttpRequest(http.MethodGet, apiExchangeInfo, nil, false)
	if err != nil {
		return APIExchangeInfo{}, err
	}

	var res APIExchangeInfo
	err = a.makeHttpRequest(req, &res)
	return res, err
}

type APIDepth struct {
	LastUpdateID int64                `json:"lastUpdateId"`
	Bids         []gateway.PriceArray `json:"bids"`
	Asks         []gateway.PriceArray `json:"asks"`
}

func (a *API) Depth(params map[string]string) (APIDepth, error) {
	req, err := a.newHttpRequest(http.MethodGet, apiDepth, params, false)
	if err != nil {
		return APIDepth{}, err
	}

	var res APIDepth
	err = a.makeHttpRequest(req, &res)
	return res, err
}

type APIAccount struct {
	BuyerCommission  float64      `json:"buyerCommission"`
	CanDeposit       bool         `json:"canDeposit"`
	CanTrade         bool         `json:"canTrade"`
	CanWithdraw      bool         `json:"canWithdraw"`
	MakerCommission  float64      `json:"makerCommission"`
	SellerCommission float64      `json:"sellerCommission"`
	TakerCommission  float64      `json:"takerCommission"`
	UpdateTime       int64        `json:"updateTime"`
	Balances         []APIBalance `json:"balances"`
}

type APIBalance struct {
	Asset  string  `json:"asset"`
	Free   float64 `json:"free,string"`
	Locked float64 `json:"locked,string"`
}

func (a *API) AccountInfo() (APIAccount, error) {
	req, err := a.newHttpRequest(http.MethodGet, apiAccountInfo, nil, true)
	if err != nil {
		return APIAccount{}, err
	}

	var res APIAccount
	err = a.makeHttpRequest(req, &res)
	return res, err
}

type APINewOrderRes struct {
	ClientOrderID string `json:"clientOrderId"`
	OrderID       int64  `json:"orderId"`
	Symbol        string `json:"symbol"`
	TransactTime  int64  `json:"transactTime"`
}

func (a *API) NewOrder(params map[string]string) (APINewOrderRes, error) {
	req, err := a.newHttpRequest(http.MethodPost, apiOrder, params, true)
	if err != nil {
		return APINewOrderRes{}, err
	}

	var res APINewOrderRes
	err = a.makeHttpRequest(req, &res)
	return res, err
}

func (a *API) CancelOrder(params map[string]string) error {
	req, err := a.newHttpRequest(http.MethodDelete, apiOrder, params, true)
	if err != nil {
		return err
	}

	return a.makeHttpRequest(req, nil)
}

type APIOrder struct {
	ClientOrderID       string  `json:"clientOrderId"`
	CummulativeQuoteQty float64 `json:"cummulativeQuoteQty,string"`
	ExecutedQty         float64 `json:"executedQty,string"`
	IsWorking           bool    `json:"isWorking"`
	OrderID             string  `json:"orderId"`
	OrigQty             float64 `json:"origQty,string"`
	Price               float64 `json:"price,string"`
	Side                string  `json:"side"`
	Status              string  `json:"status"`
	Symbol              string  `json:"symbol"`
	Time                int64   `json:"time"`
	TimeInForce         string  `json:"timeInForce"`
	Type                string  `json:"type"`
	UpdateTime          int64   `json:"updateTime"`
}

func (a *API) OpenOrders(params map[string]string) (res []APIOrder, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiOpenOrders, params, true)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

type APIListenKey struct {
	ListenKey string `json:"listenKey"`
}

func (a *API) NewListenKey() (res APIListenKey, err error) {
	req, err := a.newHttpRequest(http.MethodPost, apiNewListenKey, nil, true)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

func (a *API) RenewListenKey(listenKey string) (err error) {
	req, err := a.newHttpRequest(http.MethodPut, fmt.Sprintf(apiListenKey, listenKey), nil, true)
	if err != nil {
		return err
	}

	err = a.makeHttpRequest(req, nil)
	return err
}

func (a *API) newHttpRequest(method string, uri string, params map[string]string, signed bool) (*http.Request, error) {
	req, err := http.NewRequest(method, uri, nil)
	if err != nil {
		return nil, err
	}

	if signed {
		if params == nil {
			params = make(map[string]string)
		}

		timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)

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

		req.URL.RawQuery = preSign + "&signature=" + signature
	} else {
		par := url.Values{}
		for k, v := range params {
			par.Set(k, v)
		}
		req.URL.RawQuery = par.Encode()
	}

	if a.options.ApiKey != "" {
		req.Header.Set("X-MBX-APIKEY", a.options.ApiKey)
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

	// In this case, the API is returning us an array, which means
	// it is not in the standard response format, we must try to parse
	// directly into the expected responseObject
	if len(body) > 0 && string(body[0]) == "[" {
		err = json.Unmarshal(body, responseObject)
		if err != nil {
			return fmt.Errorf("failed to unmarshal body [%s] into responseObject, err: %s", string(body), err)
		}
	} else if len(body) > 0 {
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
	} else {
		if res.StatusCode != 200 {
			return fmt.Errorf("Blank body returned error http status code [%d] please check...", res.StatusCode)
		}
	}

	return nil
}
