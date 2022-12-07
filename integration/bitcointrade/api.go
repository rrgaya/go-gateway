package bitcointrade

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

const (
	apiBase           = "https://api.bitcointrade.com.br"
	apiBalances       = apiBase + "/v3/wallets/balance"
	apiPairs          = apiBase + "/v3/public/pairs"
	apiUserOrders     = apiBase + "/v3/market/user_orders"
	apiListUserOrders = apiBase + "/v3/market/user_orders/list"
	apiCreateOrder    = apiBase + "/v3/market/create_order"
	apiBookOrders     = apiBase + "/v3/market"
	apiCoreBase       = "https://core-base.bitcointrade.com.br"
	apiCountryList    = apiCoreBase + "/v2/country"
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
	Code    int64           `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

type APIBalance struct {
	CurrencyCode    string  `json:"currency_code"`
	AvailableAmount float64 `json:"available_amount"`
	LockedAmount    float64 `json:"locked_amount"`
	LastUpdate      string  `json:"last_update"`
}

func (a *API) WalletsBalance() ([]APIBalance, error) {
	req, err := a.newAPIRequest(http.MethodGet, apiBalances, nil)
	if err != nil {
		return []APIBalance{}, err
	}

	var res []APIBalance
	err = a.makeHttpRequestWithRetry(req, &res, 3)
	return res, err
}

type APIOrder struct {
	Code            string  `json:"code"`
	CreateDate      string  `json:"create_date"`
	ExecutedAmount  float64 `json:"executed_amount"`
	ID              string  `json:"id"`
	Pair            string  `json:"pair"`
	RemainingAmount float64 `json:"remaining_amount"`
	RemainingPrice  float64 `json:"remaining_price"`
	RequestedAmount float64 `json:"requested_amount"`
	Status          string  `json:"status"`
	Subtype         string  `json:"subtype"`
	TotalPrice      float64 `json:"total_price"`
	Type            string  `json:"type"`
	UnitPrice       float64 `json:"unit_price"`
	Amount          float64 `json:"amount"`
	UpdateDate      string  `json:"update_date"`
}

func (a *API) ListUserOrders(params url.Values) ([]APIOrder, error) {
	req, err := a.newAPIRequest(http.MethodGet, apiListUserOrders, nil)
	if err != nil {
		return []APIOrder{}, err
	}

	if len(params) > 0 {
		req.URL.RawQuery = params.Encode()
	}

	var res struct {
		Orders []APIOrder `json:"orders"`
	}
	err = a.makeHttpRequestWithRetry(req, &res, 3)

	return res.Orders, err
}

type APICreateOrder struct {
	Pair      string  `json:"pair"`
	Subtype   string  `json:"subtype"`
	Type      string  `json:"type"`
	Amount    float64 `json:"amount"`
	UnitPrice float64 `json:"unit_price"`
}

func (a *API) CreateOrder(params APICreateOrder) (APIOrder, error) {
	data, err := json.Marshal(params)
	if err != nil {
		return APIOrder{}, err
	}

	req, err := a.newAPIRequest(http.MethodPost, apiCreateOrder, bytes.NewReader(data))
	if err != nil {
		return APIOrder{}, err
	}

	var res APIOrder
	err = a.makeHttpRequestWithRetry(req, &res, 3)

	return res, err
}

type APICancelOrder struct {
	ID   string `json:"id"`
	Code string `json:"code"`
}

func (a *API) CancelOrder(params APICancelOrder) (APIOrder, error) {
	data, err := json.Marshal(params)
	if err != nil {
		return APIOrder{}, err
	}

	req, err := a.newAPIRequest(http.MethodDelete, apiUserOrders, bytes.NewReader(data))
	if err != nil {
		return APIOrder{}, err
	}

	var res APIOrder
	err = a.makeHttpRequestWithRetry(req, &res, 3)

	return res, err
}

type APIBookOrder struct {
	ID        string  `json:"id"`
	Code      string  `json:"code"`
	UserCode  string  `json:"user_code"`
	Amount    float64 `json:"amount"`
	UnitPrice float64 `json:"unit_price"`
}

type APIBookExecution struct {
	CreateDate           time.Time `json:"create_date"`
	Type                 string    `json:"type"`
	Amount               float64   `json:"amount"`
	UnitPrice            float64   `json:"unit_price"`
	ActiveOrderCode      string    `json:"active_order_code"`
	ActiveOrderUserCode  string    `json:"active_order_user_code"`
	PassiveOrderCode     string    `json:"passive_order_code"`
	PassiveOrderUserCode string    `json:"passive_order_user_code"`
}

type APIBookOrders struct {
	Buying   []APIBookOrder     `json:"buying"`
	Selling  []APIBookOrder     `json:"selling"`
	Executed []APIBookExecution `json:"executed"`
}

func (a *API) GetBookOrders(params url.Values) (APIBookOrders, error) {
	req, err := a.newAPIRequest(http.MethodGet, apiBookOrders, nil)
	if err != nil {
		return APIBookOrders{}, err
	}

	if len(params) > 0 {
		req.URL.RawQuery = params.Encode()
	}

	var res APIBookOrders
	err = a.makeHttpRequest(req, &res)
	return res, err
}

type APIPair struct {
	Base      string  `json:"base"`
	BaseName  string  `json:"base_name"`
	Enabled   bool    `json:"enabled"`
	MinAmount float64 `json:"min_amount"`
	MinValue  float64 `json:"min_value"`
	PriceTick float64 `json:"price_tick"`
	Quote     string  `json:"quote"`
	QuoteName string  `json:"quote_name"`
	Symbol    string  `json:"symbol"`
}

func (a *API) GetPairs() ([]APIPair, error) {
	req, err := a.newAPIRequest(http.MethodGet, apiPairs, nil)
	if err != nil {
		return []APIPair{}, err
	}

	var res []APIPair
	err = a.makeHttpRequest(req, &res)
	return res, err
}

type APICountryList struct {
	Countries []APICountry `json:"countries"`
}

type APICountry struct {
	Pairs []APICountryPair `json:"pairs"`
}

type APICountryPair struct {
	Base               string `json:"base"`
	Code               string `json:"code"`
	DecimalPlacesBase  int64  `json:"decimal_places_base"`
	DecimalPlacesQuote int64  `json:"decimal_places_quote"`
	Order              int64  `json:"order"`
	Quote              string `json:"quote"`
}

func (a *API) GetCountryList() (APICountryList, error) {
	req, err := a.newAPIRequest(http.MethodGet, apiCountryList, nil)
	if err != nil {
		return APICountryList{}, err
	}

	var res APICountryList
	err = a.makeHttpRequest(req, &res)
	return res, err
}

func (a *API) newAPIRequest(method, url string, data io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, url, data)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-Api-Key", a.options.Token)

	return req, nil
}

var ErrTooManyRequests = errors.New("too many requests, throttled")
var ErrFindingOrder = errors.New("error searcing for order")

func (a *API) makeHttpRequestWithRetry(req *http.Request, responseObject interface{}, maxRetries int) error {
	retryCount := 0

L:
	for retryCount < maxRetries {
		err := a.makeHttpRequest(req, responseObject)
		if err != nil {
			switch err {
			case ErrTooManyRequests:
				log.Printf("BitcoinTrade request to %s throttled, retrying in 1 second, retry count: %d", req.URL, retryCount)
				retryCount += 1
				time.Sleep(1 * time.Second)
				continue L
			case ErrFindingOrder:
				log.Printf("BitcoinTrade request to %s failed, retrying in 1 second, retry count: %d", req.URL, retryCount)
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

func (a *API) makeHttpRequest(req *http.Request, responseObject interface{}) error {
	body, err := a.sendHttpRequest(req)
	if err != nil {
		return err
	}

	var res APIResponse
	err = json.Unmarshal(body, &res)

	if res.Code != 0 {
		return fmt.Errorf("api responded with error code: %d\nresponse body: %s", res.Code, string(body))
	}

	if strings.Contains(res.Message, "Too Many Requests") {
		return ErrTooManyRequests
	}

	if strings.Contains(res.Message, "Erro ao buscar ordem") {
		return ErrFindingOrder
	}

	if responseObject != nil {
		err = json.Unmarshal(res.Data, responseObject)
		if err != nil {
			return fmt.Errorf("failed to unmarshal json, res.Data, body: %s, unmarshal err: %s", string(body), err)
		}
	}

	return nil
}

func (a *API) sendHttpRequest(req *http.Request) ([]byte, error) {
	res, err := a.client.SendRequest(req)
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	return body, nil
}
