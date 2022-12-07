package cointiger

import (
	"crypto/hmac"
	"crypto/sha512"
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
	apiBase         = "https://api.cointiger.com/exchange/trading"
	apiCurrencies   = apiBase + "/api/v2/currencys"
	apiDepth        = apiBase + "/api/market/depth"
	apiBalance      = apiBase + "/api/user/balance"
	apiOrder        = apiBase + "/api/v2/order"
	apiOrders       = apiBase + "/api/v2/order/orders"
	apiBulkCancel   = apiBase + "/api/v2/order/batch_cancel"
	apiCancelOrder  = apiBase + "/api/order"
	apiTransactions = apiBase + "/api/v2/order/match_results/v2"
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
	Code    json.Number      `json:"code"`
	Message string           `json:"msg"`
	Data    *json.RawMessage `json:"data,omitempty"`
}

type APICurrency struct {
	BaseCurrency    string  `json:"baseCurrency"`
	QuoteCurrency   string  `json:"quoteCurrency"`
	AmountMin       float64 `json:"amountMin"`
	AmountPrecision int64   `json:"amountPrecision"`
	MinTurnover     float64 `json:"minTurnover"`
	PricePrecision  int64   `json:"pricePrecision"`
}

func (a *API) Currencies() (res map[string][]APICurrency, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiCurrencies, nil, false)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

type APIBalance struct {
	Coin   string  `json:"coin"`
	Lock   float64 `json:"lock,string"`
	Normal float64 `json:"normal,string"` // Available
}

func (a *API) Balances() (res []APIBalance, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiBalance, nil, true)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

type APINewOrderRes struct {
	OrderID int64 `json:"order_id"`
}

func (a *API) CreateOrder(params map[string]string) (res APINewOrderRes, err error) {
	req, err := a.newHttpRequest(http.MethodPost, apiOrder, params, true)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

func (a *API) CancelOrder(params map[string]string) (err error) {
	req, err := a.newHttpRequest(http.MethodDelete, apiCancelOrder, params, true)
	if err != nil {
		return err
	}

	err = a.makeHttpRequest(req, nil)
	return err
}

type APIOrder struct {
	AvgPrice   float64 `json:"avg_price,string"`
	Ctime      int64   `json:"ctime"`
	DealMoney  float64 `json:"deal_money,string"`
	DealVolume float64 `json:"deal_volume,string"`
	Fee        float64 `json:"fee,string"`
	ID         int64   `json:"id"`
	Mtime      int64   `json:"mtime"`
	Price      float64 `json:"price,string"`
	Volume     float64 `json:"volume,string"`
	Status     int64   `json:"status"`
	Symbol     string  `json:"symbol"`
	Type       string  `json:"type"`
	UserID     int64   `json:"user_id"`
}

func (a *API) Orders(params map[string]string) (res []APIOrder, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiOrders, params, true)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

type APIMatch struct {
	ID      int64   `json:"id"`
	Symbol  string  `json:"symbol"`
	OrderID int64   `json:"orderId"`
	Price   float64 `json:"price,string"`
	Volume  float64 `json:"volume,string"`
	Fee     float64 `json:"fee,string"`
	Type    string  `json:"type"`
}

func (a *API) Transactions(params map[string]string) (res []APIMatch, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiTransactions, params, true)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

type APIDepthRes struct {
	Ch        string `json:"ch"`
	DepthData struct {
		Tick APIDepth `json:"tick"`
	} `json:"depth_data"`
}

type APIDepth struct {
	Bids []gateway.PriceArray `json:"buys"`
	Asks []gateway.PriceArray `json:"asks"`
}

func (a *API) Depth(params map[string]string) (res APIDepth, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiDepth, params, false)
	if err != nil {
		return res, err
	}

	var bRes APIDepthRes
	err = a.makeHttpRequest(req, &bRes)
	return bRes.DepthData.Tick, err
}

func (a *API) newHttpRequest(method string, uri string, params map[string]string, signed bool) (*http.Request, error) {
	var reqBody io.Reader

	queryVals := url.Values{}
	paramsVals := url.Values{}
	if params == nil {
		params = make(map[string]string)
	}
	for k, v := range params {
		paramsVals.Set(k, v)
		if method != http.MethodPost {
			queryVals.Set(k, v)
		}
	}

	if signed {
		timestamp := time.Now().UnixMilli()
		timestampStr := strconv.FormatInt(timestamp, 10)

		paramsVals.Set("time", timestampStr)
		signData := paramsVals.Encode()
		signData, _ = url.QueryUnescape(signData)
		signData = strings.Replace(signData, "&", "", -1)
		signData = strings.Replace(signData, "=", "", -1)
		signData = strings.Replace(signData, " ", "", -1)
		signData += a.options.ApiSecret

		h := hmac.New(sha512.New, []byte(a.options.ApiSecret))
		h.Write([]byte(signData))
		signature := hex.EncodeToString(h.Sum(nil))

		if method != http.MethodPost {
			queryVals.Set("time", timestampStr)
			for k, v := range paramsVals {
				queryVals[k] = v
			}
		}
		queryVals.Set("api_key", a.options.ApiKey)
		queryVals.Set("sign", signature)
	}

	if method != http.MethodGet {
		reqData := paramsVals.Encode()
		reqBody = strings.NewReader(reqData)
	}

	req, err := http.NewRequest(method, uri, reqBody)
	if err != nil {
		return nil, err
	}

	if reqBody != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}

	req.URL.RawQuery = queryVals.Encode()

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

	if len(body) > 0 {
		var apiRes APIResponse
		err = json.Unmarshal(body, &apiRes)
		if err != nil {
			return fmt.Errorf("failed to unmarshal body [%s] into APIResponse, err: %s", string(body), err)
		}

		code := apiRes.Code.String()
		if code != "0" && code != "" {
			return fmt.Errorf("request to api [%s] failed, returned error code [%s], body [%s]", req.URL, code, string(body))
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
