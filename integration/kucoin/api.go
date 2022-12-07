package kucoin

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/buger/jsonparser"
	"github.com/google/go-querystring/query"
	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

const (
	apiBase          = "https://api.kucoin.com"
	apiSymbols       = apiBase + "/api/v1/symbols"
	apiOrders        = apiBase + "/api/v1/orders"
	apiAccounts      = apiBase + "/api/v1/accounts"
	apiOrderBook     = apiBase + "/api/v1/market/orderbook/level2_100"
	apiBulletPublic  = apiBase + "/api/v1/bullet-public"
	apiBulletPrivate = apiBase + "/api/v1/bullet-private"
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

type ApiMarket struct {
	Symbol          string `json:"symbol"`
	BaseCurrency    string `json:"baseCurrency"`
	QuoteCurrency   string `json:"quoteCurrency"`
	BaseMinSize     string `json:"baseMinSize"`
	QuoteMinSize    string `json:"quoteMinSize"`
	BaseMaxSize     string `json:"baseMaxSize"`
	QuoteMaxSize    string `json:"quoteMaxSize"`
	BaseIncrement   string `json:"baseIncrement"`
	QuoteIncrement  string `json:"quoteIncrement"`
	PriceIncrement  string `json:"priceIncrement"`
	EnableTrading   bool   `json:"enableTrading"`
	IsMarginEnabled bool   `json:"isMarginEnabled"`
}

func (a *API) GetMarkets() (res []ApiMarket, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiSymbols, nil)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res, false)

	return res, err
}

type ApiGetOrdersRequest struct {
	Status string `url:"status,omitempty"`
	Symbol string `url:"symbol,omitempty"`
}

type ApiGetOrdersResponse struct {
	Items []ApiOrder `json:"items"`
}

type ApiOrder struct {
	Id          string  `json:"id"`
	Symbol      string  `json:"symbol"`
	Type        string  `json:"type"`
	Side        string  `json:"side"`
	Price       float64 `json:"price,string"`
	Size        float64 `json:"size,string"`
	Funds       float64 `json:"funds,string"`
	DealFunds   float64 `json:"dealFunds,string"`
	DealSize    float64 `json:"dealSize,string"`
	Fee         float64 `json:"fee,string"`
	FeeCurrency string  `json:"feeCurrency"`
	IsActive    bool    `json:"isActive"`
	CreateAt    int64   `json:"createAt"`
}

func (a *API) GetOrders(params ApiGetOrdersRequest) (orders []ApiOrder, err error) {
	data, _ := query.Values(params)

	req, err := a.newHttpRequest(http.MethodGet, apiOrders+"?"+data.Encode(), nil)
	if err != nil {
		return nil, err
	}

	res := ApiGetOrdersResponse{}
	err = a.makeHttpRequest(req, &res, true)
	if err != nil {
		return nil, err
	}

	return res.Items, nil
}

func (a *API) GetOrder(orderId string) (order *ApiOrder, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiOrders+"/"+orderId, nil)
	if err != nil {
		return nil, err
	}

	err = a.makeHttpRequest(req, &order, true)
	if err != nil {
		return nil, err
	}

	return order, nil
}

type ApiBalance struct {
	Id        string  `json:"id"`
	Currency  string  `json:"currency"`
	Balance   float64 `json:"balance,string"`
	Available float64 `json:"available,string"`
	Holds     float64 `json:"holds,string"`
}

func (a *API) GetBalances() (balances []ApiBalance, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiAccounts, nil)
	if err != nil {
		return nil, err
	}

	err = a.makeHttpRequest(req, &balances, true)
	if err != nil {
		return nil, err
	}

	return balances, nil
}

type ApiPostOrderRequest struct {
	ClientOid string `json:"clientOid"`
	Side      string `json:"side"`
	Symbol    string `json:"symbol"`
	Type      string `json:"type"`
	Price     string `json:"price"`
	Size      string `json:"size"`
	PostOnly  bool   `json:"postOnly"`
}

type ApiPostOrderResponse struct {
	OrderId string `json:"orderId"`
}

func (a *API) PostOrder(orderRequest ApiPostOrderRequest) (orderId string, err error) {
	data, err := json.Marshal(orderRequest)
	if err != nil {
		return "", err
	}

	req, err := a.newHttpRequest(http.MethodPost, apiOrders, bytes.NewReader(data))
	if err != nil {
		return "", err
	}

	res := ApiPostOrderResponse{}
	err = a.makeHttpRequest(req, &res, true)

	return res.OrderId, err
}

func (a *API) CancelOrder(orderId string) error {
	req, err := a.newHttpRequest(http.MethodDelete, apiOrders+"/"+orderId, nil)
	if err != nil {
		return err
	}

	err = a.makeHttpRequest(req, nil, true)

	return err
}

type ApiOrderBookResponse struct {
	Bids []gateway.PriceArray `json:"bids"`
	Asks []gateway.PriceArray `json:"asks"`
}

func (a *API) GetOrderBook(symbol string) (orderBook *ApiOrderBookResponse, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiOrderBook+"?symbol="+symbol, nil)
	if err != nil {
		return nil, err
	}

	err = a.makeHttpRequest(req, &orderBook, false)

	return orderBook, err
}

type PostBulletPublicResponse struct {
	Token string `json:"token"`
}

type ApiInstanceServer struct {
	Encrypt      bool   `json:"encrypt"`
	Endpoint     string `json:"endpoint"`
	PingInterval int64  `json:"pingInterval"`
	PingTimeout  int64  `json:"pingTimeout"`
	Protocol     string `json:"protocol"`
}

type ApiBulletResponse struct {
	InstanceServers []ApiInstanceServer `json:"instanceServers"`
	Token           string              `json:"token"`
}

func (a *API) GetBullet(private bool) (res ApiBulletResponse, err error) {
	var url string
	if private {
		url = apiBulletPrivate
	} else {
		url = apiBulletPublic
	}

	req, err := a.newHttpRequest(http.MethodPost, url, nil)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res, private)
	return res, err
}

func (a *API) newHttpRequest(method string, url string, data io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, url, data)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json;charset=UTF-8")

	return req, nil
}

func (a *API) makeHttpRequest(req *http.Request, responseObject interface{}, private bool) error {
	body, err := a.sendHttpRequest(req, private)
	if err != nil {
		return err
	}

	code, _, _, _ := jsonparser.Get(body, "code")
	errMsg, _, _, _ := jsonparser.Get(body, "message")
	if string(code) != "200" && string(code) != "200000" {
		return utils.NewHttpError(
			Exchange.Name,
			utils.HttpRequestError,
			req,
			string(code)+" - "+string(errMsg),
			string(body),
		).AsError()
	}

	if responseObject != nil {
		data, _, _, _ := jsonparser.Get(body, "data")

		err = json.Unmarshal(data, responseObject)
		if err != nil {
			return utils.NewHttpError(
				Exchange.Name,
				utils.UnmarshalError,
				req,
				err.Error(),
				string(body),
			).AsError()
		}
	}

	return nil
}

func (a *API) sendHttpRequest(req *http.Request, private bool) ([]byte, error) {
	if private {
		timestamp := time.Now().UnixMilli()
		timestampStr := strconv.FormatInt(timestamp, 10)
		signer := NewKcSigner(a.options.ApiKey, a.options.ApiSecret, a.options.ApiPassphrase)
		payload := fmt.Sprint(timestampStr, req.Method, req.URL.Path)

		if req.URL.RawQuery != "" {
			payload += "?" + req.URL.RawQuery
		}

		if req.Body != nil {
			reqBody, _ := req.GetBody()
			body, _ := ioutil.ReadAll(reqBody)
			payload += string(body)
		}

		signature := signer.Sign([]byte(payload))

		h := hmac.New(sha256.New, []byte(a.options.ApiSecret))
		h.Write([]byte(a.options.ApiPassphrase))
		signedPassphrase := b64.StdEncoding.EncodeToString(h.Sum(nil))

		req.Header.Set("KC-API-KEY", a.options.ApiKey)
		req.Header.Set("KC-API-SIGN", string(signature))
		req.Header.Set("KC-API-TIMESTAMP", timestampStr)
		req.Header.Set("KC-API-PASSPHRASE", signedPassphrase)
		req.Header.Set("KC-API-KEY-VERSION", "2")
	}

	res, err := a.client.SendRequest(req)
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(res.Body)

	return body, err
}
