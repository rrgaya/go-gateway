package bitforex

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

const (
	apiBase            = "https://api.bitforex.com"
	apiHost            = "api.bitforex.com"
	apiSymbolsEndpoint = "/api/v1/market/symbols"
	apiFundsInfo       = "/api/v1/fund/allAccount"
	apiPlaceOrder      = "/api/v1/trade/placeOrder"
	apiCancelOrder     = "/api/v1/trade/cancelOrder"
	apiOrderInfos      = "/api/v1/trade/orderInfos"
)

type Api struct {
	options    gateway.Options
	httpClient *utils.HttpClient
}

func NewApi(options gateway.Options) *Api {
	client := utils.NewHttpClient()
	client.UseProxies(options.Proxies)

	return &Api{
		options:    options,
		httpClient: client,
	}
}

type ApiResponseV1 struct {
	Data    json.RawMessage `json:"data"`
	Success bool            `json:"success"`
	Time    int64           `json:"time"`
	Code    string          `json:"code"`    // Error code
	Message string          `json:"message"` // Error message
}

func (a *Api) Symbols() ([]Symbol, error) {
	var symbols []Symbol

	req, err := a.newHttpRequest(http.MethodGet, apiSymbolsEndpoint, nil)
	if err != nil {
		return symbols, err
	}

	err = a.makeHttpRequest(req, &symbols)

	return symbols, err
}

type FundInfo struct {
	Currency string  `json:"currency"`
	Fix      float64 `json:"fix"`    // Total
	Active   float64 `json:"active"` // Available
	Frozen   float64 `json:"frozen"`
}

func (a *Api) FundsInfo() ([]FundInfo, error) {
	var funds []FundInfo

	req, err := a.newHttpRequest(http.MethodPost, apiFundsInfo, nil)
	if err != nil {
		return funds, err
	}

	err = a.makeHttpRequest(req, &funds)
	return funds, err
}

type tradeType int

var buyTradeType tradeType = 1
var sellTradeType tradeType = 2

type PlaceOrderRes struct {
	OrderId string `json:"orderId"`
}

func (a *Api) PlaceOrder(side tradeType, symbol, price, amount string) (string, error) {
	req, err := a.newHttpRequest(http.MethodPost, apiPlaceOrder, nil)
	if err != nil {
		return "", err
	}

	q := req.URL.Query()
	q.Set("symbol", symbol)
	q.Set("price", price)
	q.Set("amount", amount)
	q.Set("tradeType", strconv.FormatInt(int64(side), 10))
	req.URL.RawQuery = q.Encode()

	var res PlaceOrderRes
	err = a.makeHttpRequest(req, &res)
	return res.OrderId, err
}

func (a *Api) CancelOrder(symbol string, orderId string) error {
	req, err := a.newHttpRequest(http.MethodPost, apiCancelOrder, nil)
	if err != nil {
		return err
	}

	q := req.URL.Query()
	q.Set("symbol", symbol)
	q.Set("orderId", orderId)
	req.URL.RawQuery = q.Encode()

	var success bool
	err = a.makeHttpRequest(req, &success)
	if err != nil {
		return err
	}
	if success == false {
		return fmt.Errorf("expected cancel order res.Data to be true, was false.")
	}

	return nil
}

type OrderInfo struct {
	OrderID     string  `json:"orderId"`
	Symbol      string  `json:"symbol"`
	TradeType   int64   `json:"tradeType"`
	OrderState  int64   `json:"orderState"`
	OrderPrice  float64 `json:"orderPrice"`
	OrderAmount float64 `json:"orderAmount"`
	AvgPrice    float64 `json:"avgPrice"`
	DealAmount  float64 `json:"dealAmount"`
	TradeFee    float64 `json:"tradeFee"`
	CreateTime  int64   `json:"createTime"`
	LastTime    int64   `json:"lastTime"`
}

var apiDelegatedOrderState = 0
var apiCompletedOrderState = 1

func (a *Api) OrderInfos(symbol string, state int) ([]OrderInfo, error) {
	req, err := a.newHttpRequest(http.MethodPost, apiOrderInfos, nil)
	if err != nil {
		return []OrderInfo{}, err
	}

	q := req.URL.Query()
	q.Set("symbol", symbol)
	q.Set("state", strconv.FormatInt(int64(state), 10))
	req.URL.RawQuery = q.Encode()

	var infos []OrderInfo
	err = a.makeHttpRequest(req, &infos)
	return infos, err
}

func (a *Api) makeHttpRequest(req *http.Request, responseObject interface{}) error {
	var res ApiResponseV1

	err := a.sendHttpRequest(req, &res)
	if err != nil {
		return err
	}

	if res.Success != true {
		return errors.New(fmt.Sprintf("Bitforex http request to %s failed. Code: %s. Msg: %s.", req.URL.String(), res.Code, res.Message))
	}

	err = json.Unmarshal(res.Data, responseObject)
	if err != nil {
		return err
	}

	return nil
}

func (a *Api) newHttpRequest(method string, path string, data io.Reader) (*http.Request, error) {
	if string(path[0]) != "/" {
		path = "/" + path
	}

	url := apiBase + path

	req, err := http.NewRequest(method, url, data)
	if err != nil {
		return nil, err
	}

	req.Host = apiHost
	req.Header.Set("Content-Type", "application/json;charset=UTF-8")

	return req, nil
}

func (a *Api) sendHttpRequest(req *http.Request, responseObject interface{}) error {
	isAuthenticated := a.options.ApiKey != ""

	if isAuthenticated {
		nonce := time.Now().UnixNano() / int64(time.Millisecond)

		q := req.URL.Query()
		q.Set("accessKey", a.options.ApiKey)
		q.Set("nonce", strconv.FormatInt(nonce, 10))

		// Sign request
		data := fmt.Sprintf("%s?%s", req.URL.Path, q.Encode())
		h := hmac.New(sha256.New, []byte(a.options.ApiSecret))
		h.Write([]byte(data))
		signature := hex.EncodeToString(h.Sum(nil))
		q.Set("signData", signature)

		req.URL.RawQuery = q.Encode()
	}

	res, err := a.httpClient.SendRequest(req)
	if err != nil {
		return err
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}

	err = json.Unmarshal(body, responseObject)
	if err != nil {
		return err
	}

	return nil
}
