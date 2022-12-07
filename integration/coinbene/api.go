package coinbene

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
	"time"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

const (
	apiBase         = "https://openapi-exchange.coinbene.com"
	apiPairs        = "/api/exchange/v2/market/tradePair/list"
	apiBalance      = "/api/exchange/v2/account/list"
	apiOpenOrders   = "/api/exchange/v2/order/openOrders"
	apiClosedOrders = "/api/exchange/v2/order/closedOrders"
	apiPlaceOrder   = "/api/exchange/v2/order/place"
	apiCancelOrder  = "/api/exchange/v2/order/cancel"
	apiOrderInfo    = "/api/exchange/v2/order/info"
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
	Code    int64  `json:"code"`
	Message string `json:"message"`
	Data    json.RawMessage
}

type TradePair struct {
	Symbol           string  `json:"symbol"`
	BaseAsset        string  `json:"baseAsset"`
	QuoteAsset       string  `json:"quoteAsset"`
	MakerFeeRate     float64 `json:"makerFeeRate,string"`
	TakerFeeRate     float64 `json:"takerFeeRate,string"`
	PricePrecision   int64   `json:"pricePrecision,string"`
	AmountPrecision  int64   `json:"amountPrecision,string"`
	MinAmount        float64 `json:"minAmount,string"`
	PriceFluctuation float64 `json:"priceFluctuation,string"`
	Site             string  `json:"site"`
}

func (a *API) TradePairsList() ([]TradePair, error) {
	var pairs []TradePair

	req, err := a.newHttpRequest(http.MethodGet, apiPairs, nil)
	if err != nil {
		return pairs, err
	}

	err = a.makeHttpRequest(req, &pairs)
	return pairs, err
}

type Balance struct {
	Asset         string  `json:"asset"`
	Available     float64 `json:"available,string"`
	FrozenBalance float64 `json:"frozenBalance,string"`
	TotalBalance  float64 `json:"totalBalance,string"`
}

func (a *API) AccountBalance() ([]Balance, error) {
	var balances []Balance

	req, err := a.newHttpRequest(http.MethodGet, apiBalance, nil)
	if err != nil {
		return balances, err
	}

	err = a.makeHttpRequest(req, &balances)
	return balances, err
}

type Order struct {
	Amount         float64     `json:"amount,string"` // Money value, not sure why they call this the "amount"
	AvgPrice       json.Number `json:"avgPrice"`
	BaseAsset      string      `json:"baseAsset"`
	FilledAmount   float64     `json:"filledAmount,string"`
	FilledQuantity float64     `json:"filledQuantity,string"`
	MakerFeeRate   float64     `json:"makerFeeRate,string"`
	OrderDirection string      `json:"orderDirection"`
	OrderID        string      `json:"orderId"`
	OrderPrice     float64     `json:"orderPrice,string"`
	OrderStatus    string      `json:"orderStatus"`
	OrderTime      time.Time   `json:"orderTime,string"`
	Quantity       float64     `json:"quantity,string"`
	QuoteAsset     string      `json:"quoteAsset"`
	TakerFeeRate   float64     `json:"takerFeeRate,string"`
	TotalFee       float64     `json:"totalFee,string"`
}

func (a *API) OpenOrders(symbol string) ([]Order, error) {
	var orders []Order

	req, err := a.newHttpRequest(http.MethodGet, apiOpenOrders, nil)
	if err != nil {
		return orders, err
	}

	if symbol != "" {
		q := req.URL.Query()
		q.Set("symbol", symbol)
		req.URL.RawQuery = q.Encode()
	}

	err = a.makeHttpRequest(req, &orders)
	return orders, err
}

func (a *API) ClosedOrders(symbol string) ([]Order, error) {
	var orders []Order

	req, err := a.newHttpRequest(http.MethodGet, apiClosedOrders, nil)
	if err != nil {
		return orders, err
	}

	if symbol != "" {
		q := req.URL.Query()
		q.Set("symbol", symbol)
		req.URL.RawQuery = q.Encode()
	}

	err = a.makeHttpRequest(req, &orders)
	return orders, err
}

func (a *API) OrderInfo(orderID string) (Order, error) {
	req, err := a.newHttpRequest(http.MethodGet, apiOrderInfo, nil)
	if err != nil {
		return Order{}, err
	}

	q := req.URL.Query()
	q.Set("orderId", orderID)
	req.URL.RawQuery = q.Encode()

	var order = Order{}
	err = a.makeHttpRequest(req, &order)
	return order, err
}

type PlaceOrder struct {
	Symbol    string `json:"symbol"`
	Direction string `json:"direction"`
	Price     string `json:"price"`
	Quantity  string `json:"quantity"`
	OrderType string `json:"orderType"`
	Notional  string `json:"notional"`
	ClientID  string `json:"clientId"`
}

func (a *API) PlaceOrder(order PlaceOrder) (string, error) {
	data, err := json.Marshal(order)
	if err != nil {
		return "", err
	}

	req, err := a.newHttpRequest(http.MethodPost, apiPlaceOrder, bytes.NewReader(data))
	if err != nil {
		return "", err
	}

	var res = struct {
		OrderID string `json:"orderId"`
	}{}

	err = a.makeHttpRequest(req, &res)
	return res.OrderID, err
}

func (a *API) CancelOrder(orderID string) error {
	var reqData = struct {
		OrderID string `json:"orderId"`
	}{orderID}

	data, err := json.Marshal(reqData)
	if err != nil {
		return err
	}

	req, err := a.newHttpRequest(http.MethodPost, apiCancelOrder, bytes.NewReader(data))
	if err != nil {
		return err
	}

	err = a.makeHttpRequest(req, nil)
	return err
}

func (a *API) makeHttpRequest(req *http.Request, responseObject interface{}) error {
	body, err := a.sendHttpRequest(req)
	if err != nil {
		return err
	}

	var res APIResponse
	err = json.Unmarshal(body, &res)
	if err != nil {
		return fmt.Errorf("makeHttpRequest failed to unmarshal body, err: %s", err)
	}

	if res.Code != 200 {
		return fmt.Errorf("api returned error code %d, msg %s, body: %s", res.Code, res.Message, string(body))
	}

	if responseObject != nil {
		err = json.Unmarshal(res.Data, responseObject)
		if err != nil {
			return fmt.Errorf("failed to unmarshal json, body: %s, unmarshal err: %s", string(res.Data), err)
		}
	}

	return nil
}

func (a *API) newHttpRequest(method string, path string, data io.Reader) (*http.Request, error) {
	if string(path[0]) != "/" {
		path = "/" + path
	}

	url := apiBase + path
	req, err := http.NewRequest(method, url, data)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json;charset=UTF-8")

	return req, nil
}

func (a *API) sendHttpRequest(req *http.Request) ([]byte, error) {
	isAuthenticated := a.options.ApiKey != ""

	if isAuthenticated {
		timestamp := time.Now().
			UTC().
			Format("2006-01-02T15:04:05.999Z")

		req.Header.Set("ACCESS-KEY", a.options.ApiKey)
		req.Header.Set("ACCESS-TIMESTAMP", timestamp)

		// Sign request
		data := timestamp + req.Method + req.URL.RequestURI()
		if req.Body != nil {
			reqBody, _ := req.GetBody()
			body, err := ioutil.ReadAll(reqBody)
			if err != nil {
				return nil, fmt.Errorf("failed to read request body to generate request signature, err %s", err)
			}
			data += string(body)
		}
		h := hmac.New(sha256.New, []byte(a.options.ApiSecret))
		h.Write([]byte(data))
		signature := hex.EncodeToString(h.Sum(nil))

		req.Header.Set("ACCESS-SIGN", signature)
	}

	res, err := a.httpClient.SendRequest(req)
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(res.Body)

	return body, err
}
