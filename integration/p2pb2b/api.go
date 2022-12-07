package p2pb2b

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

const (
	apiBase                   = "https://api.p2pb2b.com/api"
	apiMarkets                = apiBase + "/v2/public/markets"
	apiDepth                  = apiBase + "/v2/public/depth/result"
	apiAccountBalance         = apiBase + "/v2/account/balances"
	apiOpenOrders             = apiBase + "/v2/orders"
	apiMarketExecutionHistory = apiBase + "/v2/account/executed_history"
	apiOrderDeals             = apiBase + "/v2/account/order"
	apiCreateOrder            = apiBase + "/v2/order/new"
	apiOrderDelete            = apiBase + "/v2/order/cancel"
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
	Success bool            `json:"success"`
	Message string          `json:"message"`
	Result  json.RawMessage `json:"result"`
}

type APIMarket struct {
	Name      string `json:"name"`
	Money     string `json:"money"`
	Stock     string `json:"stock"`
	Precision struct {
		Fee   int `json:"fee,string"`
		Money int `json:"money,string"`
		Stock int `json:"stock,string"`
	} `json:"precision"`
	Limits struct {
		MaxAmount float64 `json:"max_amount,string"`
		MaxPrice  float64 `json:"max_price,string"`
		MinAmount float64 `json:"min_amount,string"`
		MinPrice  float64 `json:"min_price,string"`
		MinTotal  float64 `json:"min_total,string"`
		StepSize  float64 `json:"step_size,string"`
		TickSize  float64 `json:"tick_size,string"`
	} `json:"limits"`
}

func (a *API) Markets() ([]APIMarket, error) {
	var res []APIMarket
	req, err := a.newPublicAPIRequest(http.MethodGet, apiMarkets)
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

func (a *API) Depth(market string) (APIDepth, error) {
	var res APIDepth
	req, err := a.newPublicAPIRequest(http.MethodGet, apiDepth)
	if err != nil {
		return res, err
	}

	params := req.URL.Query()
	params.Set("market", market)
	params.Set("limit", "100")
	req.URL.RawQuery = params.Encode()

	err = a.makeHttpRequest(req, &res)
	return res, err
}

type APIBalance struct {
	Available float64 `json:"available,string"`
	Freeze    float64 `json:"freeze,string"`
}

func (a *API) AccountBalances() (map[string]APIBalance, error) {
	var res map[string]APIBalance
	req, err := a.newAPIRequest(http.MethodPost, apiAccountBalance, nil, nil)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

type APIOrder struct {
	Amount    float64     `json:"amount,string"`
	DealFee   float64     `json:"dealFee,string"`
	DealMoney float64     `json:"dealMoney,string"`
	DealStock float64     `json:"dealStock,string"`
	Left      float64     `json:"left,string"`
	MakerFee  float64     `json:"makerFee,string"`
	Market    string      `json:"market"`
	Price     float64     `json:"price,string"`
	Side      string      `json:"side"`
	TakerFee  float64     `json:"takerFee,string"`
	Timestamp float64     `json:"timestamp"`
	Type      string      `json:"type"`
	ID        json.Number `json:"id,Number"`
	OrderID   json.Number `json:"orderId,Number"`
}

func (a *API) OpenOrders(market string) ([]APIOrder, error) {
	params := make(map[string]interface{})
	params["market"] = market
	params["limit"] = "100"

	var res []APIOrder
	req, err := a.newAPIRequest(http.MethodPost, apiOpenOrders, nil, params)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

type APIDeal struct {
	Amount      float64     `json:"amount,string"`
	Deal        float64     `json:"deal,string"`
	Fee         float64     `json:"fee,string"`
	ID          json.Number `json:"id,Number"`
	DealOrderID json.Number `json:"dealOrderId,Number"`
	Price       float64     `json:"price,string"`
	Role        int         `json:"role"`
	Time        float64     `json:"time"`
}

type APIOrderDeals struct {
	Records []APIDeal `json:"records"`
}

func (a *API) OrderDeals(orderId string) ([]APIDeal, error) {
	params := make(map[string]interface{})
	params["orderId"] = orderId

	var res APIOrderDeals
	req, err := a.newAPIRequest(http.MethodPost, apiOrderDeals, nil, params)
	if err != nil {
		return res.Records, err
	}

	err = a.makeHttpRequest(req, &res)
	return res.Records, err
}

type APIExecution struct {
	Amount float64     `json:"amount,string"`
	Deal   float64     `json:"deal,string"`
	Fee    float64     `json:"fee,string"`
	ID     json.Number `json:"id,Number"`
	Price  float64     `json:"price,string"`
	Role   int         `json:"role"`
	Side   string      `json:"side"`
	Time   float64     `json:"time"`
}

func (a *API) MarketExecutionHistory(market string) ([]APIExecution, error) {
	params := make(map[string]interface{})
	params["market"] = market
	params["limit"] = "100"

	var res []APIExecution
	req, err := a.newAPIRequest(http.MethodPost, apiMarketExecutionHistory, nil, params)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

func (a *API) CreateOrder(side, market, price, amount string) (APIOrder, error) {
	params := make(map[string]interface{})
	params["market"] = market
	params["side"] = side
	params["price"] = price
	params["amount"] = amount

	var res APIOrder
	req, err := a.newAPIRequest(http.MethodPost, apiCreateOrder, nil, params)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

func (a *API) CancelOrder(market, orderID string) (APIOrder, error) {
	params := make(map[string]interface{})
	params["market"] = market
	params["orderId"] = orderID

	var res APIOrder
	req, err := a.newAPIRequest(http.MethodPost, apiOrderDelete, nil, params)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

func (a *API) newPublicAPIRequest(method, uri string) (*http.Request, error) {
	req, err := http.NewRequest(method, uri, nil)
	return req, err
}

func (a *API) newAPIRequest(method, url string, params *url.Values, bodyParams map[string]interface{}) (*http.Request, error) {
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return nil, err
	}

	timestamp := time.Now().Unix()
	nonce := timestamp + rand.Int63()

	if bodyParams == nil {
		bodyParams = make(map[string]interface{})
	}

	bodyParams["request"] = req.URL.Path
	bodyParams["nonce"] = strconv.FormatInt(nonce, 10)

	data, err := json.Marshal(bodyParams)
	if err != nil {
		return nil, fmt.Errorf("failed marshal body params, err: %s", err)
	}

	req.Body = ioutil.NopCloser(bytes.NewReader(data))
	req.ContentLength = int64(len(data))

	payload := base64.StdEncoding.EncodeToString(data)
	hmac := hmac.New(sha512.New, []byte(a.options.ApiSecret))
	hmac.Write([]byte(payload))
	signature := hex.EncodeToString(hmac.Sum(nil))

	req.Header.Set("Content-Type", "application/json;charset=UTF-8")
	req.Header.Set("X-TXC-APIKEY", a.options.ApiKey)
	req.Header.Set("X-TXC-PAYLOAD", payload)
	req.Header.Set("X-TXC-SIGNATURE", signature)

	return req, nil
}

func (a *API) makeHttpRequest(req *http.Request, responseObject interface{}) error {
	var res APIResponse
	body, err := a.sendHttpRequest(req, &res)
	if err != nil {
		return err
	}

	if res.Success != true {
		return fmt.Errorf("api responded with unsuccesfull response: %s", string(body))
	}

	if responseObject != nil {
		err = json.Unmarshal(res.Result, responseObject)
		if err != nil {
			return fmt.Errorf("failed to unmarshal json, res.Result: %s, unmarshal err: %s", string(res.Result), err)
		}
	}

	return nil
}

func (a *API) sendHttpRequest(req *http.Request, responseObject interface{}) ([]byte, error) {
	res, err := a.client.SendRequest(req)
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(body, responseObject)
	if err != nil {
		return nil, err
	}

	return body, nil
}
