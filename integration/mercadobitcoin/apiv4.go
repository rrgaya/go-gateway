package mercadobitcoin

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/buger/jsonparser"
	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

const (
	apiV4Base      = "https://api.mercadobitcoin.net/api/v4"
	apiV4Authorize = apiV4Base + "/authorize"
	apiV4Symbols   = apiV4Base + "/symbols"
	apiV4Orderbook = apiV4Base + "/%s/orderbook"
	apiV4Trades    = apiV4Base + "/%s/trades"
	apiV4Accounts  = apiV4Base + "/accounts"
	apiV4Orders    = apiV4Base + "/accounts/%s/%s/orders"
)

type APIV4 struct {
	options     gateway.Options
	client      *utils.HttpClient
	accessToken string
}

func NewAPIV4(options gateway.Options) *APIV4 {
	client := utils.NewHttpClient()
	client.UseProxies(options.Proxies)

	return &APIV4{
		options: options,
		client:  client,
	}
}

func (a *APIV4) SetAccessToken(token string) {
	a.accessToken = token
}

type APIAuthorize struct {
	AccessToken string `json:"access_token"`
	Expiration  int64  `json:"expiration"`
}

func (a *APIV4) Authorize(login, password string) (APIAuthorize, error) {
	var res APIAuthorize

	bodyStr := fmt.Sprintf("{\"login\": \"%s\", \"password\": \"%s\"}", login, password)
	req, err := a.newAPIRequest(http.MethodPost, apiV4Authorize, bytes.NewBufferString(bodyStr))
	if err != nil {
		return res, err
	}

	req.Header.Set("Content-Type", "application/json")

	err = a.makeHttpRequest(req, &res)
	return res, err
}

type APIV4Symbols struct {
	BaseCurrency []string `json:"base-currency"`
	Currency     []string `json:"currency"`
	PriceScale   []int    `json:"pricescale"`
	Type         []string `json:"type"`
}

func (a *APIV4) Symbols() (APIV4Symbols, error) {
	var res APIV4Symbols

	req, err := a.newAPIRequest(http.MethodGet, apiV4Symbols, nil)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

type APIV4BookRow [2]json.Number

type APIV4OrderBook struct {
	Asks []APIV4BookRow `json:"asks"`
	Bids []APIV4BookRow `json:"bids"`
}

func (a *APIV4) OrderBook(asset string) (APIV4OrderBook, error) {
	var res APIV4OrderBook

	url := fmt.Sprintf(apiV4Orderbook, asset)
	req, err := a.newAPIRequest(http.MethodGet, url, nil)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

type APIV4Trade struct {
	TID    int64       `json:"tid"`
	Date   int64       `json:"date"`
	Type   string      `json:"type"`
	Price  json.Number `json:"price"`
	Amount json.Number `json:"amount"`
}

func (a *APIV4) Trades(asset string, params *url.Values) ([]APIV4Trade, error) {
	var res []APIV4Trade

	url := fmt.Sprintf(apiV4Trades, asset)
	req, err := a.newAPIRequest(http.MethodGet, url, nil)
	if err != nil {
		return res, err
	}

	if params != nil {
		req.URL.RawQuery = params.Encode()
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

type APIV4Account struct {
	Currency     string `json:"currency"`
	CurrencySign string `json:"currencySign"`
	ID           string `json:"id"`
	Name         string `json:"name"`
	Type         string `json:"type"`
}

func (a *APIV4) ListAccounts() ([]APIV4Account, error) {
	var res []APIV4Account

	req, err := a.newAPIRequest(http.MethodGet, apiV4Accounts, nil)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

type APIV4Execution struct {
	ExecutedAt int64       `json:"executed_at"`
	ID         json.Number `json:"id"`
	Instrument string      `json:"instrument"`
	Price      json.Number `json:"price"`
	Qty        json.Number `json:"qty"`
	Side       string      `json:"side"`
}

type APIV4Order struct {
	AvgPrice   json.Number      `json:"avgPrice"`
	CreatedAt  int64            `json:"created_at"`
	FilledQty  json.Number      `json:"filledQty"`
	ID         json.Number      `json:"id"`
	Instrument string           `json:"instrument"`
	LimitPrice json.Number      `json:"limitPrice"`
	Qty        json.Number      `json:"qty"`
	Side       string           `json:"side"`
	Status     string           `json:"status"`
	Type       string           `json:"type"`
	UpdatedAt  int64            `json:"updated_at"`
	Executions []APIV4Execution `json:"executions"`
}

func (a *APIV4) ListOrders(accountID, symbol string, params *url.Values) ([]APIV4Order, error) {
	var res []APIV4Order

	url := fmt.Sprintf(apiV4Orders, accountID, symbol)
	req, err := a.newAPIRequest(http.MethodGet, url, nil)
	if err != nil {
		return res, err
	}

	if params != nil {
		req.URL.RawQuery = params.Encode()
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

func (a *APIV4) newAPIRequest(method, url string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}

	if a.accessToken != "" {
		req.Header.Set("Authorization", a.accessToken)
	}

	return req, nil
}

func (a *APIV4) makeHttpRequest(req *http.Request, responseObject interface{}) error {
	body, err := a.sendHttpRequest(req)
	if err != nil {
		return err
	}

	errVal, dataType, _, _ := jsonparser.Get(body, "code")
	if dataType != jsonparser.NotExist {
		return fmt.Errorf("MercadoBitcoin V4 %s %s responded with error message: %s\nresponse body: %s", req.Method, req.URL.String(), string(errVal), string(body))
	}

	if responseObject != nil {
		err = json.Unmarshal(body, responseObject)
		if err != nil {
			return fmt.Errorf("failed to unmarshal json, body: %s, unmarshal err: %s", string(body), err)
		}
	}

	return nil
}

func (a *APIV4) sendHttpRequest(req *http.Request) ([]byte, error) {
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
