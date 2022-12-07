package exmarkets

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/buger/jsonparser"
	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

const (
	apiBase            = "https://exmarkets.com"
	apiGeneralInfo     = apiBase + "/api/v1/general/info"
	apiBook            = apiBase + "/api/trade/v1/market/order-book"
	apiWalletBalance   = apiBase + "/api/trade/v1/account/balance"
	apiCompletedOrders = apiBase + "/api/trade/v1/orders"
	apiOpenOrders      = apiBase + "/api/trade/v1/orders/open"
	apiNewBuyOrder     = apiBase + "/api/trade/v1/orders/buy"
	apiNewSellOrder    = apiBase + "/api/trade/v1/orders/sell"
	apiCancelOrder     = apiBase + "/api/trade/v1/orders/%s"
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

type APIMarket struct {
	Active         bool    `json:"active"`
	Slug           string  `json:"slug"`
	PricePrecision int     `json:"price_precision"`
	BasePrecision  int     `json:"base_precision"`
	MinAmount      float64 `json:"min_amount"`
}

type APIGeneralInfo struct {
	Markets []APIMarket
}

func (a *API) GeneralInfo() (APIGeneralInfo, error) {
	var res APIGeneralInfo
	req, err := a.newPublicAPIRequest(http.MethodGet, apiGeneralInfo)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

type APIBook struct {
	Bids []APIPrice `json:"bids"`
	Asks []APIPrice `json:"asks"`
}

type APIPrice struct {
	Price  float64 `json:"price,string"`
	Amount float64 `json:"amount,string"`
}

func (a *API) Book(market string) (APIBook, error) {
	var res APIBook
	req, err := a.newPublicAPIRequest(http.MethodGet, apiBook)
	if err != nil {
		return res, err
	}

	params := req.URL.Query()
	params.Set("market", market)
	req.URL.RawQuery = params.Encode()

	err = a.makeHttpRequest(req, &res)
	return res, err
}

type APIWallet struct {
	Wallets []APIBalance `json:"wallets"`
}

type APIBalance struct {
	Currency  string  `json:"currency"`
	Total     float64 `json:"total,string"`
	Available float64 `json:"available,string"`
	Reserved  float64 `json:"reserved,string"`
}

func (a *API) WalletsBalance() ([]APIBalance, error) {
	var res APIWallet
	req, err := a.newAPIRequest(http.MethodGet, apiWalletBalance, nil, nil)
	if err != nil {
		return res.Wallets, err
	}

	err = a.makeHttpRequest(req, &res)
	return res.Wallets, err
}

type APIOrders struct {
	Buy  []APIOrder `json:"buy"`
	Sell []APIOrder `json:"sell"`
}

type APIOrder struct {
	ID            json.Number `json:"id,Number"`
	Type          string      `json:"type"`
	Amount        float64     `json:"amount,string"`
	CurrentAmount float64     `json:"current_amount,string"`
	Price         float64     `json:"price,string"`
}

func (a *API) UserOrders(market, state string) (APIOrders, error) {
	var uri string
	if state == "open" {
		uri = apiOpenOrders
	} else {
		uri = apiCompletedOrders
	}

	params := &url.Values{}
	params.Set("market", market)
	params.Set("limit", "100")

	var res APIOrders
	req, err := a.newAPIRequest(http.MethodGet, uri, params, nil)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

func (a *API) NewOrder(side, market string, price, amount string) (APIOrder, error) {
	var uri string
	if side == "buy" {
		uri = apiNewBuyOrder
	} else {
		uri = apiNewSellOrder
	}

	params := make(map[string]interface{})
	params["market"] = market
	params["type"] = "limit"
	params["price"] = price
	params["amount"] = amount

	var res APIOrder
	req, err := a.newAPIRequest(http.MethodPost, uri, nil, params)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

func (a *API) CancelOrder(market, orderID string) (APIOrder, error) {
	url := fmt.Sprintf(apiCancelOrder, orderID)

	var res APIOrder
	req, err := a.newAPIRequest(http.MethodDelete, url, nil, nil)
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

func (a *API) newAPIRequest(method, uri string, params *url.Values, bodyParams map[string]interface{}) (*http.Request, error) {
	timestamp := time.Now().Unix()
	expires := timestamp + 5 // Request expires in 5 seconds
	nonce := timestamp + rand.Int63()

	if method != http.MethodPost {
		if params == nil {
			params = &url.Values{}
		}
		params.Set("timestamp", strconv.FormatInt(expires, 10))
		params.Set("nonce", strconv.FormatInt(nonce, 10))
	}

	var body []byte
	var bodyReader io.Reader
	if bodyParams != nil {
		bodyParams["timestamp"] = expires
		bodyParams["nonce"] = nonce
		data, err := json.Marshal(bodyParams)
		if err != nil {
			return nil, fmt.Errorf("failed marshal body params, err: %s", err)
		}
		body = data
		bodyReader = bytes.NewReader(body)
	}

	req, err := http.NewRequest(method, uri, bodyReader)
	if err != nil {
		return nil, err
	}

	signData := req.Method + req.URL.Path

	// Set params
	if params != nil {
		req.URL.RawQuery = params.Encode()
		signData += "?" + req.URL.RawQuery
	}
	if body != nil {
		signData += string(body)
	}

	hmac := hmac.New(sha512.New, []byte(a.options.ApiSecret))
	hmac.Write([]byte(signData))
	signature := hex.EncodeToString(hmac.Sum(nil))
	encodedToken := base64.StdEncoding.EncodeToString([]byte(a.options.ApiKey + ":" + signature))

	req.Header.Set("Content-Type", "application/json;charset=UTF-8")
	req.Header.Set("Authorization", "Basic "+encodedToken)

	return req, nil
}

func (a *API) makeHttpRequest(req *http.Request, responseObject interface{}) error {
	body, err := a.sendHttpRequest(req)
	if err != nil {
		return err
	}

	errVal, dataType, _, _ := jsonparser.Get(body, "error")
	if dataType != jsonparser.NotExist {
		return fmt.Errorf("api responded with error message: %s\nresponse body: %s", string(errVal), string(body))
	}

	if responseObject != nil {
		err = json.Unmarshal(body, responseObject)
		if err != nil {
			return fmt.Errorf("failed to unmarshal json, body: %s, unmarshal err: %s", string(body), err)
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

	if res.StatusCode == 401 {
		return body, fmt.Errorf("Unauthorized request, msg: %s", string(body))
	}

	return body, nil
}
