package probit

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

const (
	apiBase        = "https://api.probit.com"
	apiMarkets     = apiBase + "/api/exchange/v1/market"
	apiBalance     = apiBase + "/api/exchange/v1/balance"
	apiOpenOrder   = apiBase + "/api/exchange/v1/open_order"
	apiNewOrder    = apiBase + "/api/exchange/v1/new_order"
	apiCancelOrder = apiBase + "/api/exchange/v1/cancel_order"
	apiToken       = "https://accounts.probit.com/token"
)

type API struct {
	options          gateway.Options
	httpClient       *utils.HttpClient
	activeToken      string
	activeTokenMutex *sync.RWMutex
}

func NewAPI(options gateway.Options) *API {
	client := utils.NewHttpClient()
	client.UseProxies(options.Proxies)

	return &API{
		options:          options,
		httpClient:       client,
		activeTokenMutex: &sync.RWMutex{},
	}
}

type APIResponse struct {
	ErrorCode string `json:"errorCode"`
	Data      json.RawMessage
}

type APIMarket struct {
	ID                string  `json:"id"`
	BaseCurrencyID    string  `json:"base_currency_id"`
	QuoteCurrencyID   string  `json:"quote_currency_id"`
	Closed            bool    `json:"closed"`
	MakerFeeRate      float64 `json:"maker_fee_rate,string"`
	TakerFeeRate      float64 `json:"taker_fee_rate,string"`
	MinCost           float64 `json:"min_cost,string"`
	MinPrice          float64 `json:"min_price,string"`
	MinQuantity       float64 `json:"min_quantity,string"`
	PriceIncrement    float64 `json:"price_increment,string"`
	QuantityPrecision int64   `json:"quantity_precision"`
	ShowInUI          bool    `json:"show_in_ui"`
}

func (a *API) Markets() (markets []APIMarket, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiMarkets, nil)
	if err != nil {
		return markets, err
	}

	err = a.makeHttpRequest(req, &markets)
	return markets, err
}

type APIToken struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int64  `json:"expires_in"`
	TokenType   string `json:"token_type"`
	Error       string `json:"error"`
}

func (a *API) Token() (res APIToken, err error) {
	if a.options.ApiKey == "" || a.options.ApiSecret == "" {
		return res, fmt.Errorf("requires api key and api secret to init auth")
	}

	reqData := strings.NewReader("{\"grant_type\":\"client_credentials\"}")
	req, err := a.newHttpRequest(http.MethodPost, apiToken, reqData)
	if err != nil {
		return res, err
	}

	authStr := []byte(a.options.ApiKey + ":" + a.options.ApiSecret)
	authEncoded := base64.StdEncoding.EncodeToString(authStr)
	req.Header.Set("Authorization", "Basic "+authEncoded)

	body, err := a.sendHttpRequest(req)
	if err != nil {
		return res, err
	}

	err = json.Unmarshal(body, &res)
	if err != nil {
		return res, fmt.Errorf("failed to unmarshal body: %s, err: %s", string(body), err)
	}

	if res.Error != "" {
		return res, fmt.Errorf("error response, body: %s", string(body))
	}

	return res, err
}

type APIBalance struct {
	CurrencyID string  `json:"currency_id"`
	Total      float64 `json:"total,string"`
	Available  float64 `json:"available,string"`
}

func (a *API) Balance() (balances []APIBalance, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiBalance, nil)
	if err != nil {
		return balances, err
	}

	err = a.makeHttpRequest(req, &balances)
	return balances, err
}

type APIOrder struct {
	CancelledQuantity float64 `json:"cancelled_quantity,string"`
	ClientOrderID     string  `json:"client_order_id"`
	FilledCost        float64 `json:"filled_cost,string"`
	FilledQuantity    float64 `json:"filled_quantity,string"`
	ID                string  `json:"id"`
	LimitPrice        float64 `json:"limit_price,string"`
	MarketID          string  `json:"market_id"`
	OpenQuantity      float64 `json:"open_quantity,string"`
	Quantity          float64 `json:"quantity,string"`
	Side              string  `json:"side"`
	Status            string  `json:"status"`
	Time              string  `json:"time"`
	TimeInForce       string  `json:"time_in_force"`
	Type              string  `json:"type"`
	UserID            string  `json:"user_id"`
}

type APINewOrder struct {
	MarketID    string `json:"market_id"`
	LimitPrice  string `json:"limit_price"`
	Quantity    string `json:"quantity"`
	Side        string `json:"side"`
	TimeInForce string `json:"time_in_force"`
	Type        string `json:"type"`
}

const notEnoughtBalanceErrMatch = "NOT_ENOUGH_BALANCE"

func (a *API) NewOrder(params APINewOrder) (order APIOrder, err error) {
	if params.Type == "" {
		params.Type = "limit"
	}

	if params.TimeInForce == "" {
		params.TimeInForce = "gtc"
	}

	reqData, err := json.Marshal(params)
	if err != nil {
		return order, err
	}

	req, err := a.newHttpRequest(http.MethodPost, apiNewOrder, bytes.NewReader(reqData))
	if err != nil {
		return order, err
	}

	err = a.makeHttpRequest(req, &order)
	if err != nil && strings.Contains(err.Error(), notEnoughtBalanceErrMatch) {
		maxRetries := 5
		tries := 0

		log.Printf("ProBit new order NOT_ENOUGH_BALANCE error, retrying...")

		for tries < maxRetries {
			// Sometimes we receive this error on HFT trades, because the engine has
			// not yet credited us with balance
			err = a.makeHttpRequest(req, &order)
			if err != nil {
				tries += 1
				log.Printf("ProBit retry %d err %s", tries, err)
				continue
			}

			break
		}
	}

	return order, err
}

type APICancelOrder struct {
	MarketID string `json:"market_id"`
	OrderID  string `json:"order_id"`
}

func (a *API) CancelOrder(params APICancelOrder) (order APIOrder, err error) {
	reqData, err := json.Marshal(params)
	if err != nil {
		return order, err
	}

	req, err := a.newHttpRequest(http.MethodPost, apiCancelOrder, bytes.NewReader(reqData))
	if err != nil {
		return order, err
	}

	err = a.makeHttpRequest(req, &order)
	return order, err
}

func (a *API) OpenOrder(market_id string) (orders []APIOrder, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiOpenOrder, nil)
	if err != nil {
		return orders, err
	}

	q := req.URL.Query()
	q.Set("market_id", market_id)
	req.URL.RawQuery = q.Encode()

	err = a.makeHttpRequest(req, &orders)
	return orders, err
}

func (a *API) InitAuthentication() (err error) {
	if a.activeToken != "" {
		return fmt.Errorf("api session already has active token set, can't init authentication handler twice")
	}

	token, err := a.updateAccessToken()
	if err != nil {
		return err
	}

	// Update token before expire
	go func() {
		for {
			time.Sleep((time.Second * time.Duration(token.ExpiresIn)) - 10)

			log.Printf("ProBit API updating access token...")

			_, err := a.updateAccessToken()
			if err != nil {
				err = fmt.Errorf("failed to update access token, err: %s", err)
				panic(err)
			}
		}
	}()

	return nil
}

func (a *API) updateAccessToken() (token APIToken, err error) {
	token, err = a.Token()
	a.activeTokenMutex.Lock()
	a.activeToken = token.AccessToken
	a.activeTokenMutex.Unlock()
	return
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

	if res.ErrorCode != "" {
		return fmt.Errorf("api returned error code %s, body: %s", res.ErrorCode, string(body))
	}

	if responseObject != nil {
		err = json.Unmarshal(res.Data, responseObject)
		if err != nil {
			return fmt.Errorf("failed to unmarshal json, body: %s, unmarshal err: %s", string(body), err)
		}
	}

	return nil
}

func (a *API) newHttpRequest(method string, url string, data io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, url, data)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json;charset=UTF-8")

	if a.activeToken != "" {
		a.activeTokenMutex.RLock()
		req.Header.Set("Authorization", "Bearer "+a.activeToken)
		a.activeTokenMutex.RUnlock()
	}

	return req, nil
}

func (a *API) sendHttpRequest(req *http.Request) ([]byte, error) {
	res, err := a.httpClient.SendRequest(req)
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(res.Body)

	return body, err
}

func apiOrderToCommon(o APIOrder) gateway.Order {
	avgPx := 0.0
	if o.FilledQuantity > 0 {
		avgPx = o.FilledCost / o.FilledQuantity
	}

	return gateway.Order{
		Market: gateway.Market{
			Exchange: Exchange,
			Symbol:   o.MarketID,
		},
		ID:               o.ID,
		Side:             mapOrderSideToCommon(o.Side),
		State:            mapOrderStatusToCommon(o.Status, o.OpenQuantity),
		Amount:           o.Quantity,
		Price:            o.LimitPrice,
		FilledAmount:     o.FilledQuantity,
		FilledMoneyValue: o.FilledCost,
		AvgPrice:         avgPx,
	}
}
