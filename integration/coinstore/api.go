package coinstore

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
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

const (
	apiBase            = "https://api.coinstore.com"
	apiConfigsPublic   = "https://futures.coinstore.com/api/configs/public?spotDataState=1"
	apiSpotAccountList = apiBase + "/api/spot/accountList"
	apiCurrentOrders   = apiBase + "/api/trade/order/active"
	apiCreateOrder     = apiBase + "/api/trade/order/place"
	apiCancelOrder     = apiBase + "/api/trade/order/cancel"
	apiAccountMatches  = apiBase + "/api/trade/match/accountMatches"
	apiDepth           = apiBase + "/v2/public/orderbook/market_pair"
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
	Message string           `json:"message"`
	Data    *json.RawMessage `json:"data,omitempty"`
}

type APISpotSymbol struct {
	InstrumentID    int64   `json:"instrumentId"`
	Symbol          string  `json:"symbol"`
	BaseCurrency    string  `json:"baseCurrency"`
	QuoteCurrency   string  `json:"quoteCurrency"`
	LimitMinVolume  float64 `json:"limitMinVolume,string"`
	AmountPrecision int64   `json:"amountPrecision"`
	PricePrecision  int64   `json:"pricePrecision"`
	MakerFeeRate    float64 `json:"makerFeeRate,string"`
	TakerFeeRate    float64 `json:"takerFeeRate,string"`
}

type APIPublicConfig struct {
	SpotSymbols struct {
		List []APISpotSymbol `json:"list"`
	} `json:"spotSymbols"`
}

func (a *API) PublicConfig() (res APIPublicConfig, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiConfigsPublic, nil, false)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

type APIAsset struct {
	Balance  float64 `json:"balance,string"`
	Currency string  `json:"currency"`
	Type     int64   `json:"type"`
	TypeName string  `json:"typeName"`
}

func (a *API) AssetList() (res []APIAsset, err error) {
	req, err := a.newHttpRequest(http.MethodPost, apiSpotAccountList, nil, true)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

type APINewOrderRes struct {
	OrderID int64 `json:"ordId"`
}

func (a *API) CreateOrder(params map[string]string) (res APINewOrderRes, err error) {
	req, err := a.newHttpRequest(http.MethodPost, apiCreateOrder, params, true)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

type APICancelOrderRes struct {
	OrderID int64  `json:"ordId"`
	State   string `json:"state"`
}

func (a *API) CancelOrder(params map[string]string) (res APICancelOrderRes, err error) {
	req, err := a.newHttpRequest(http.MethodPost, apiCancelOrder, params, true)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

type APIOrder struct {
	BaseCurrency  string  `json:"baseCurrency"`
	ClOrdID       string  `json:"clOrdId"`
	CumAmt        float64 `json:"cumAmt,string"`
	CumQty        float64 `json:"cumQty,string"`
	LeavesQty     float64 `json:"leavesQty,string"`
	OrdID         string  `json:"ordId"`
	OrdPrice      float64 `json:"ordPrice,string"`
	OrdQty        float64 `json:"ordQty,string"`
	OrdStatus     string  `json:"ordStatus"`
	OrdType       string  `json:"ordType"`
	QuoteCurrency string  `json:"quoteCurrency"`
	Side          string  `json:"side"`
	Symbol        string  `json:"symbol"`
	TimeInForce   string  `json:"timeInForce"`
	Timestamp     int64   `json:"timestamp"`
}

func (a *API) CurrentOrders(params map[string]string) (res []APIOrder, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiCurrentOrders, params, true)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

type APIMatch struct {
	ID         int64   `json:"id"`
	ExecAmt    float64 `json:"execAmt"`
	ExecQty    float64 `json:"execQty"`
	Fee        float64 `json:"fee"`
	MatchID    int64   `json:"matchId"`
	OrderID    int64   `json:"orderId"`
	OrderState int64   `json:"orderState"`
	Side       int64   `json:"side"`
}

func (a *API) AccountMatches(params map[string]string) (res []APIMatch, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiAccountMatches, params, true)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

type APIDepth struct {
	Bids []gateway.PriceArray `json:"bids"`
	Asks []gateway.PriceArray `json:"asks"`
}

func (a *API) Depth(params map[string]string) (res APIDepth, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiDepth, params, false)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

func (a *API) newHttpRequest(method string, uri string, params map[string]string, signed bool) (*http.Request, error) {
	var reqBody io.Reader
	var reqData []byte
	var queryString string
	var expiresStr string
	var signature string

	if method == http.MethodGet {
		var keys []string
		for k := range params {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			queryString += k + "=" + params[k] + "&"
		}
		queryString = strings.TrimSuffix(queryString, "&")
	} else {
		data, err := json.Marshal(params)
		if err != nil {
			return nil, fmt.Errorf("params marshal err: %s", err)
		}

		reqData = data
		reqBody = bytes.NewReader(data)
	}

	if signed {
		if params == nil {
			params = make(map[string]string)
		}

		expires := time.Now().UnixMilli()
		expiresStr = strconv.FormatInt(expires, 10)
		expiresKey := expires / 30000 // As documented by the docs
		expiresKeyStr := strconv.FormatInt(expiresKey, 10)

		h := hmac.New(sha256.New, []byte(a.options.ApiSecret))
		h.Write([]byte(expiresKeyStr))
		key := hex.EncodeToString(h.Sum(nil))

		// Generate encoded params signature str
		var signData string
		if method == http.MethodGet {
			signData = queryString
		} else {
			signData = string(reqData)
		}

		h = hmac.New(sha256.New, []byte(key))
		h.Write([]byte(signData))
		signature = hex.EncodeToString(h.Sum(nil))
	}

	req, err := http.NewRequest(method, uri, reqBody)
	if err != nil {
		return nil, err
	}

	if queryString != "" {
		req.URL.RawQuery = queryString
	}

	if signed {
		req.Header.Set("X-CS-APIKEY", a.options.ApiKey)
		req.Header.Set("X-CS-EXPIRES", expiresStr)
		req.Header.Set("X-CS-SIGN", signature)
		req.Header.Set("Content-Type", "application/json")
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
