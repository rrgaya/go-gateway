package whitebit

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

const (
	apiWhiteBIT = "https://whitebit.com/api/v1/public/"
	apiHitBTC   = "" // They have the same backend
	apiSymbols  = "markets"
	apiMarkets  = "markets"
	// Old
	apiSpotBalance  = "/api/3/spot/balance"
	apiActiveOrders = "/api/3/spot/order"
	apiCreateOrder  = "/api/3/spot/order"
	apiCancelOrder  = "/api/3/spot/order/%s"
)

type API struct {
	baseURL    string
	authHeader string
	options    gateway.Options
	client     *utils.HttpClient
}

func NewAPI(options gateway.Options, baseURL string) *API {
	client := utils.NewHttpClient()
	client.UseProxies(options.Proxies)

	authStr := fmt.Sprintf("%s:%s", options.ApiKey, options.ApiSecret)
	authHeader := fmt.Sprintf("Basic %s", base64.StdEncoding.EncodeToString([]byte(authStr)))

	return &API{
		baseURL:    baseURL,
		authHeader: authHeader,
		options:    options,
		client:     client,
	}
}

type APIError struct {
	Code        int64  `json:"code"`
	Description string `json:"description"`
	Message     string `json:"message"`
}

type MarketWhiteBIT struct {
	Name      string `json:"name"`
	Stock     string `json:"stock"`
	Money     string `json:"money"`
	StockPrec string `json:"stockPrec"`
	MoneyPrec string `json:"moneyPrec"`
	FeePrec   string `json:"feePrec"`
	MakerFee  string `json:"makerFee"`
	TakerFee  string `json:"takerFee"`
	MinAmount string `json:"minAmount"`
}

// TODO: Remove or rename this struct
type APISymbol struct {
	BaseCurrency       string  `json:"base_currency"`
	FeeCurrency        string  `json:"fee_currency"`
	MakeRate           float64 `json:"make_rate,string"`
	MarginTrading      bool    `json:"margin_trading"`
	MaxInitialLeverage string  `json:"max_initial_leverage"`
	QuantityIncrement  float64 `json:"quantity_increment,string"`
	QuoteCurrency      string  `json:"quote_currency"`
	Status             string  `json:"status"`
	TakeRate           float64 `json:"take_rate,string"`
	TickSize           float64 `json:"tick_size,string"`
	Type               string  `json:"type"`
}

type ExchangeInfoWhiteBIT struct {
	Name          string `json:"name"`
	Stock         string `json:"stock"`
	Money         string `json:"money"`
	StockPrec     string `json:"stockPrec"`
	MoneyPrec     string `json:"moneyPrec"`
	FeePrec       string `json:"feePrec"`
	MakerFee      string `json:"makerFee"`
	TakerFee      string `json:"takerFee"`
	MinAmount     string `json:"minAmount"`
	MinTotal      string `json:"minTotal"`
	TradesEnabled bool   `json:"tradesEnabled"`
}

type APIResponse struct {
	Success bool                   `json:"success"`
	Message string                 `json:"message"`
	Result  []ExchangeInfoWhiteBIT `json:"result"`
}

type MarketInfoWhiteBit struct {
	Success bool                   `json:"success"`
	Message string                 `json:"message"`
	Result  []ExchangeInfoWhiteBIT `json:"result"`
}

func (a *API) ExchangeInfo() (res []ExchangeInfoWhiteBIT, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiMarkets, nil, false)
	if err != nil {
		return res, err
	}

	res, err = a.makeHttpRequestWhiteBit(req, &res)
	if err != nil {
		return nil, fmt.Errorf("error in send request %v", err)
	}
	return res, err
}

type APIBalance struct {
	Currency       string  `json:"currency"`
	Available      float64 `json:"available,string"`
	Reserved       float64 `json:"reserved,string"`
	ReservedMargin float64 `json:"reserved_margin,string"`
}

func (a *API) SpotBalance() (res []APIBalance, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiSpotBalance, nil, true)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res, false)
	return res, err
}

type APIOrder struct {
	ClientOrderID      string  `json:"client_order_id"`
	CreatedAt          string  `json:"created_at"`
	ID                 int64   `json:"id"`
	PostOnly           bool    `json:"post_only"`
	Price              float64 `json:"price,string"`
	Quantity           float64 `json:"quantity,string"`
	QuantityCumulative float64 `json:"quantity_cumulative,string"`
	Side               string  `json:"side"`
	Status             string  `json:"status"`
	Symbol             string  `json:"symbol"`
	TimeInForce        string  `json:"time_in_force"`
	Type               string  `json:"type"`
	UpdatedAt          string  `json:"updated_at"`
}

func (a *API) CreateOrder(params map[string]string) (res APIOrder, err error) {
	req, err := a.newHttpRequest(http.MethodPost, apiCreateOrder, params, true)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res, false)
	return res, err
}

func (a *API) CancelOrder(CID string) error {
	cancelURL := fmt.Sprintf(apiCancelOrder, CID)
	req, err := a.newHttpRequest(http.MethodDelete, cancelURL, nil, true)
	if err != nil {
		return err
	}

	return a.makeHttpRequest(req, nil, false)
}

func (a *API) ActiveOrders(params map[string]string) (res []APIOrder, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiActiveOrders, params, true)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res, false)
	return res, err
}

func (a *API) newHttpRequest(method string, path string, params map[string]string, signed bool) (*http.Request, error) {
	var body io.Reader
	if method == http.MethodPost && params != nil {
		par := url.Values{}
		for k, v := range params {
			par.Set(k, v)
		}
		body = strings.NewReader(par.Encode())
	}

	uri := a.baseURL + path
	req, err := http.NewRequest(method, uri, body)
	if err != nil {
		return nil, err
	}

	if method == http.MethodGet {
		par := url.Values{}
		for k, v := range params {
			par.Set(k, v)
		}
		req.URL.RawQuery = par.Encode()
	}

	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}

	if signed {
		req.Header.Set(
			"Authorization",
			a.authHeader,
		)
	}

	return req, nil
}

func (a *API) makeHttpRequestWhiteBit(req *http.Request, responseObject interface{}) (response []ExchangeInfoWhiteBIT, err error) {
	res, err := a.client.SendRequest(req)
	if err != nil {
		return response, fmt.Errorf("error in send request %v", err)
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return response, fmt.Errorf("error in send request %v", err)
	}

	var apiRes APIResponse

	err = json.Unmarshal(body, &apiRes)

	if err != nil {
		return response, fmt.Errorf("error in send request %v", err)
	}

	if err != nil {
		return response, fmt.Errorf("failed to unmarshal body [%s] into APIResponse, err: %s", string(body), err)
	}

	if !apiRes.Success {
		return response, fmt.Errorf("request to api [%s] failed, returned error code [%t], body [%s]", req.URL, apiRes.Success, string(body))
	}

	return apiRes.Result, nil
}

func (a *API) makeHttpRequest(req *http.Request, responseObject interface{}, checkAPIRes bool) error {
	res, err := a.client.SendRequest(req)
	if err != nil {
		return err
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	//TODO Remove this log
	log.Println(string(body))

	var apiRes APIResponse

	if checkAPIRes {
		err = json.Unmarshal(body, &apiRes)

		if err != nil {
			return fmt.Errorf("failed to unmarshal body [%s] into APIResponse, err: %s", string(body), err)
		}

		if !apiRes.Success {
			return fmt.Errorf(
				"request to api [%s] failed, returned error code [%t], body [%s]",
				req.URL,
				apiRes.Success,
				string(body),
			)
		}
	}

	if responseObject != nil {
		log.Printf("%v\n", responseObject)
		if err := json.Unmarshal(body, &responseObject); err != nil {
			log.Fatalln(err)
		}

		log.Println(responseObject)

		// if apiRes.Result != nil {
		// 	data := apiRes.Result
		// 	log.Println(data)

		// }
		// erro no mapping
		// err = json.Unmarshal(data, responseObject)
		// if err != nil {
		// 	return fmt.Errorf("failed to unmarshal data [%s] into responseObject, err: %s", string(body), err)
		// }
	}
	return nil
}
