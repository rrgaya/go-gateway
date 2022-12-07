package ripio

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/buger/jsonparser"
	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

const (
	apiBase            = "https://api.exchange.ripio.com"
	apiBaseHost        = "api.exchange.ripio.com"
	apiBaseStaging     = "http://apistagingexchange.ripio.com"
	apiBaseStagingHost = "apistagingexchange.ripio.com"
	apiOrderbook       = "/api/v1/orderbook/%s/"
	apiOrders          = "/api/v1/order/%s/"
	apiOpenOrder       = "/api/v1/order/%s/"
	apiCancelOrder     = "/api/v1/order/%s/%s/cancel/"
	apiBalance         = "/api/v1/balances/exchange_balances/"
	apiPairs           = "/api/v1/pair/"
)

type Api struct {
	options gateway.Options
	client  *utils.HttpClient
}

func NewApi(options gateway.Options) *Api {
	client := utils.NewHttpClient()
	client.UseProxies(options.Proxies)

	return &Api{
		options: options,
		client:  client,
	}
}

type Pair struct {
	Symbol          string  `json:"symbol"`
	Enabled         bool    `json:"enabled"`
	Base            string  `json:"base"`
	Quote           string  `json:"quote"`
	Country         string  `json:"country"`
	TakerFee        float64 `json:"taker_fee"`
	MakerFee        float64 `json:"maker_fee"`
	CancellationFee float64 `json:"cancellation_fee"`
}

type PairsRes struct {
	Count    int
	Next     string
	Previous string
	Results  []Pair
}

type Balance struct {
	ID          int64   `json:"id"`
	Available   float64 `json:"available,string"`
	Locked      float64 `json:"locked,string"`
	Currency    string  `json:"currency"`
	Symbol      string  `json:"symbol"`
	Code        string  `json:"code"`
	BalanceType string  `json:"balance_type"`
}

type Order struct {
	OrderID    string  `json:"order_id"`
	OrderType  string  `json:"order_type"`
	Side       string  `json:"side"`
	Pair       string  `json:"pair"`
	Status     string  `json:"status"`
	LimitPrice float64 `json:"limit_price,string"`
	Amount     float64 `json:"amount,string"`
	Filled     float64 `json:"filled,string"`
	Notional   float64 `json:"notional,string"`
	FillOrKill bool    `json:"fill_or_kill"`
	AllOrNone  bool    `json:"all_or_none"`
	CreatedAt  int64   `json:"created_at"`
}

type GetOrdersRes struct {
	Count    int64  `json:"count"`
	Next     string `json:"next"`
	Previous string `json:"previous"`
	Results  json.RawMessage
}

func (a *Api) GetPairs() ([]Pair, error) {
	var res PairsRes
	req, err := a.newHttpRequest(http.MethodGet, apiPairs, nil)
	if err != nil {
		return []Pair{}, err
	}
	err = a.makeHttpRequest(req, &res)
	if err != nil {
		return []Pair{}, err
	}

	return res.Results, err
}

func (a *Api) GetBalances() ([]Balance, error) {
	var res []Balance
	req, err := a.newHttpRequest(http.MethodGet, apiBalance, nil)
	if err != nil {
		return res, err
	}
	err = a.makeHttpRequest(req, &res)
	return res, err
}

type OrderBookRes struct {
	Buy  []BookPrice
	Sell []BookPrice
}

type BookPrice struct {
	Price  float64 `json:"price,string"`
	Amount float64 `json:"amount,string"`
	Total  float64 `json:"total,string"`
	Volume float64 `json:"volume,string"`
}

func (a *Api) GetOrderBook(symbol string) (OrderBookRes, error) {
	var res OrderBookRes
	path := fmt.Sprintf(apiOrderbook, symbol)
	req, err := a.newHttpRequest(http.MethodGet, path, nil)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequestWithRetry(req, &res, 3)
	return res, err
}

func (a *Api) GetOrders(symbol string, status []string) ([]Order, error) {
	var res GetOrdersRes
	path := fmt.Sprintf(apiOrders, symbol)
	req, err := a.newHttpRequest(http.MethodGet, path, nil)
	if err != nil {
		return []Order{}, err
	}

	q := req.URL.Query()
	if len(status) > 0 {
		q.Set("status", strings.Join(status, ","))
	}
	req.URL.RawQuery = q.Encode()

	err = a.makeHttpRequestWithRetry(req, &res, 3)
	if err != nil {
		return []Order{}, err
	}

	var results map[string]json.RawMessage
	err = json.Unmarshal(res.Results, &results)
	if err != nil {
		return []Order{}, fmt.Errorf("failed to unmarshal results data, err: %s", err)
	}

	data, ok := results["data"]
	if !ok {
		return []Order{}, fmt.Errorf("expected get orders res results to have \"data\" key")
	}

	var orders []Order
	err = json.Unmarshal(data, &orders)
	if err != nil {
		return []Order{}, fmt.Errorf("failed to unmarshal orders data into, err: %s", err)
	}

	return orders, nil
}

func (a *Api) CreateOrder(symbol, side, _type, amount, price string) (Order, error) {
	var order Order

	requestBody := struct {
		Side       string `json:"side"`
		OrderType  string `json:"order_type"`
		Amount     string `json:"amount"`
		LimitPrice string `json:"limit_price"`
	}{side, _type, amount, price}

	data, err := json.Marshal(requestBody)
	if err != nil {
		return order, err
	}

	path := fmt.Sprintf(apiOpenOrder, symbol)
	req, err := a.newHttpRequest(http.MethodPost, path, bytes.NewBuffer(data))
	if err != nil {
		return order, err
	}

	err = a.makeHttpRequestWithRetry(req, &order, 3)
	return order, err
}

func (a *Api) CancelOrder(symbol, orderId string) error {
	path := fmt.Sprintf(apiCancelOrder, symbol, orderId)
	req, err := a.newHttpRequest(http.MethodPost, path, nil)
	if err != nil {
		return err
	}

	var resOrder Order
	err = a.makeHttpRequestWithRetry(req, &resOrder, 3)
	if err != nil {
		return err
	}

	if resOrder.Status == "OPEN" || resOrder.Status == "PART" {
		return fmt.Errorf("faied to cancel order, status remained %s instead of cancelled", resOrder.Status)
	}

	return nil
}

func (a *Api) newHttpRequest(method string, path string, data io.Reader) (*http.Request, error) {
	if string(path[0]) != "/" {
		path = "/" + path
	}

	var url string
	var host string
	if a.options.Staging {
		url = apiBaseStaging + path
		host = apiBaseStagingHost
	} else {
		url = apiBase + path
		host = apiBaseHost
	}

	req, err := http.NewRequest(method, url, data)
	if err != nil {
		return nil, err
	}

	req.Host = host
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", a.options.Token))
	req.Header.Set("Content-Type", "application/json")

	return req, nil
}

var ErrTooManyRequests = errors.New("too many requests, throttled")

func (a *Api) makeHttpRequestWithRetry(req *http.Request, responseObject interface{}, maxRetries int) error {
	retryCount := 0

L:
	for retryCount < maxRetries {
		err := a.makeHttpRequest(req, responseObject)
		if err != nil {
			switch err {
			case ErrTooManyRequests:
				log.Printf("Ripio request to %s throttled, retrying in 1 second, retry count: %d", req.URL, retryCount)
				retryCount += 1
				time.Sleep(1 * time.Second)
				continue L
			default:
				return err
			}
		}

		return nil
	}

	return ErrTooManyRequests
}

func (a *Api) makeHttpRequest(req *http.Request, responseObject interface{}) error {
	body, err := a.sendHttpRequest(req)
	if err != nil {
		return err
	}

	errVal, dataType, _, _ := jsonparser.Get(body, "errors")
	if dataType != jsonparser.NotExist {
		return fmt.Errorf("api responded with error message: %s\nresponse body: %s", string(errVal), string(body))
	}

	detailVal, dataType, _, _ := jsonparser.Get(body, "detail")
	if dataType != jsonparser.NotExist {
		if strings.Contains(string(detailVal), "was throttled") {
			return ErrTooManyRequests
		} else {
			return fmt.Errorf("api responded with error (detail) message: %s\nresponse body: %s", string(detailVal), string(body))
		}
	}

	if responseObject != nil {
		err = json.Unmarshal(body, responseObject)
		if err != nil {
			return fmt.Errorf("failed to unmarshal json, body: %s, unmarshal err: %s", body, err)
		}
	}

	return nil
}

func (a *Api) sendHttpRequest(req *http.Request) ([]byte, error) {
	res, err := a.client.SendRequest(req)
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	if res.StatusCode == 401 {
		return body, fmt.Errorf("Unauthorized request, msg: %s", string(body))
	}

	return body, nil
}
