package digitra

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

const (
	apiProductionSubdomain = "api"
	apiStagingSubdomain    = "stg-hb.cloud"
	apiTradeBase           = "https://trade.%s.digitra.com"
	apiBalanceBase         = "https://balance.%s.digitra.com"
	apiMarkets             = "/v1/markets"
	apiMarket              = "/v1/markets/%s"
	apiOrders              = "/v1/orders"
	apiOrder               = "/v1/orders/%s"
	apiBalances            = "/v1/balances"
)

func (a *API) buildURL(base, path string) string {
	var baseStr string
	if a.options.Staging {
		baseStr = fmt.Sprintf(base, apiStagingSubdomain)
	} else {
		baseStr = fmt.Sprintf(base, apiProductionSubdomain)
	}

	return baseStr + path
}

type APIMarket struct {
	ID                   string       `json:"id"`
	BaseCurrency         string       `json:"base_currency"`
	QuoteCurrency        string       `json:"quote_currency"`
	Enabled              bool         `json:"enabled"`
	IncrementSize        float64      `json:"increment_size"`
	MarketOrderTolerance float64      `json:"market_order_tolerance"`
	MinimumOrderSize     float64      `json:"minimum_order_size"`
	PriceIncrementSize   float64      `json:"price_increment_size"`
	OrderBook            APIOrderBook `json:"order_book"`
}

type APIOrderBook struct {
	Bids []APIOrderBookPrice `json:"bids"`
	Asks []APIOrderBookPrice `json:"asks"`
}

type APIOrderBookPrice struct {
	Price float64 `json:"price"`
	Size  float64 `json:"size"`
}

type APIOrder struct {
	CancelReason        string  `json:"cancel_reason"`
	CreatedAt           string  `json:"created_at"`
	CustomID            string  `json:"custom_id"`
	Fee                 float64 `json:"fee"`
	Filled              float64 `json:"filled"`
	FilledWeightedPrice float64 `json:"filled_weighted_price"`
	ID                  string  `json:"id"`
	Market              string  `json:"market"`
	Price               float64 `json:"price"`
	Side                string  `json:"side"`
	Size                float64 `json:"size"`
	Status              string  `json:"status"`
	TimeInForce         string  `json:"time_in_force"`
	TradingAccount      string  `json:"trading_account"`
	Type                string  `json:"type"`
	UpdatedAt           string  `json:"updated_at"`
}

type APIBalance struct {
	Amount        float64 `json:"amount"`
	AmountTrading float64 `json:"amount_trading"`
	Asset         string  `json:"asset"`
	UpdatedAt     string  `json:"updated_at"`
}

type APIResponse struct {
	Result *json.RawMessage `json:"result,omitempty"`
	Errors []APIError       `json:"errors"`
	Msg    string           `json:"msg"`
}

type APIError struct {
	Field string `json:"field"`
	Msg   string `json:"msg"`
}

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

func (a *API) Markets() (res []APIMarket, err error) {
	req, err := a.newHttpRequest(http.MethodGet, a.buildURL(apiTradeBase, apiMarkets), nil, false)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

func (a *API) Market(symbol string, params map[string]interface{}) (res APIMarket, err error) {
	req, err := a.newHttpRequest(
		http.MethodGet,
		fmt.Sprintf(a.buildURL(apiTradeBase, apiMarket), symbol),
		params,
		false,
	)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

func (a *API) Balances() (res []APIBalance, err error) {
	req, err := a.newHttpRequest(http.MethodGet, a.buildURL(apiBalanceBase, apiBalances), nil, true)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

func (a *API) CreateOrder(params map[string]interface{}) (res APIOrder, err error) {
	req, err := a.newHttpRequest(http.MethodPost, a.buildURL(apiTradeBase, apiOrders), params, true)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

func (a *API) CancelOrder(orderID string) (err error) {
	req, err := a.newHttpRequest(
		http.MethodDelete,
		fmt.Sprintf(a.buildURL(apiTradeBase, apiOrder), orderID),
		nil,
		true,
	)
	if err != nil {
		return err
	}

	err = a.makeHttpRequest(req, nil)
	return err
}

func (a *API) GetOrders(params map[string]interface{}) (res []APIOrder, err error) {
	req, err := a.newHttpRequest(http.MethodGet, a.buildURL(apiTradeBase, apiOrders), params, true)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

func (a *API) GetOrder(orderID string, params map[string]interface{}) (res APIOrder, err error) {
	req, err := a.newHttpRequest(
		http.MethodGet,
		fmt.Sprintf(a.buildURL(apiTradeBase, apiOrder), orderID),
		params,
		true,
	)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

func (a *API) newHttpRequest(method string, uri string, params map[string]interface{}, signed bool) (*http.Request, error) {
	var reqBody io.Reader

	if method != http.MethodGet && params != nil {
		reqData, err := json.Marshal(params)
		if err != nil {
			return nil, fmt.Errorf("params json marshal err: %s", err)
		}
		reqBody = bytes.NewReader(reqData)
	}

	req, err := http.NewRequest(method, uri, reqBody)
	if err != nil {
		return nil, err
	}

	if method == http.MethodGet && params != nil {
		queryParams := url.Values{}
		for k, v := range params {
			queryParams.Set(k, fmt.Sprintf("%v", v))
		}
		req.URL.RawQuery = queryParams.Encode()
	}

	if signed {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", a.options.Token))
	}

	if reqBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	return req, nil
}

func (a *API) makeHttpRequest(req *http.Request, responseObject interface{}) error {
	res, err := a.client.SendRequest(req)
	if err != nil {
		return err
	}

	if res.StatusCode < 200 && res.StatusCode >= 300 {
		return fmt.Errorf("Blank body returned error http status code [%d] please check...", res.StatusCode)
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

		if len(apiRes.Errors) > 0 {
			return fmt.Errorf("api errors res, %s", string(body))
		}

		if responseObject != nil {
			var data []byte
			if apiRes.Result != nil {
				data = *apiRes.Result
			} else {
				data = body
			}

			err = json.Unmarshal(data, responseObject)
			if err != nil {
				return fmt.Errorf("failed to unmarshal data [%s] into responseObject, err: %s", string(data), err)
			}
		}
	}

	return nil
}
