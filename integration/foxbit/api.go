package foxbit

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
	"net/url"
	"strconv"
	"time"

	"github.com/buger/jsonparser"
	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

const (
	apiBase        = "https://api.foxbit.com.br"
	apiMarkets     = apiBase + "/rest/v3/markets"
	apiAccounts    = apiBase + "/rest/v3/accounts"
	apiCancelOrder = apiBase + "/rest/v3/orders/cancel"
	apiTrades      = apiBase + "/rest/v3/trades"
	apiOrder       = apiBase + "/rest/v3/orders/%s"
	apiOrders      = apiBase + "/rest/v3/orders"
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

type APIPair struct {
	Name   string `json:"name"`
	Symbol string `json:"symbol"`
	Type   string `json:"type"`
}

type APIMarket struct {
	Symbol            string  `json:"symbol"`
	Base              APIPair `json:"base"`
	Quote             APIPair `json:"quote"`
	QuantityMin       float64 `json:"quantity_min,string"`
	QuantityIncrement float64 `json:"quantity_increment,string"`
	PriceMin          float64 `json:"price_min,string"`
	PriceIncrement    float64 `json:"price_increment,string"`
}

type APIPlaceOrderReq struct {
	MarketSymbol string `json:"market_symbol"`
	Price        string `json:"price"`
	Quantity     string `json:"quantity"`
	Side         string `json:"side"`
	Type         string `json:"type"`
}

func (a *API) Markets() (res []APIMarket, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiMarkets, nil)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

type APIPlaceOrderRes struct {
	Sn string `json:"sn"`
}

func (a *API) PlaceOrder(placeOrder APIPlaceOrderReq) (res APIPlaceOrderRes, err error) {
	data, err := json.Marshal(placeOrder)
	if err != nil {
		return res, err
	}

	req, err := a.newHttpRequest(http.MethodPost, apiOrders, bytes.NewReader(data))
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

type APICancelOrderReq struct {
	Sn   string `json:"sn"`
	Type string `json:"type"`
}

type APIOrder struct {
	ClientOrderID    string  `json:"client_order_id"`
	CreatedAt        string  `json:"created_at"`
	MarketSymbol     string  `json:"market_symbol"`
	Price            float64 `json:"price,string"`
	PriceAvg         float64 `json:"price_avg,string"`
	Quantity         float64 `json:"quantity,string"`
	QuantityExecuted float64 `json:"quantity_executed,string"`
	Side             string  `json:"side"`
	Sn               string  `json:"sn"`
	State            string  `json:"state"`
	TradesCount      int64   `json:"trades_count"`
	Type             string  `json:"type"`
}

func (a *API) CancelOrder(orderID string) (err error) {
	dataReq := APICancelOrderReq{
		Type: "SN",
		Sn:   orderID,
	}
	data, err := json.Marshal(dataReq)
	if err != nil {
		return err
	}

	req, err := a.newHttpRequest(http.MethodPut, apiCancelOrder, bytes.NewReader(data))
	if err != nil {
		return err
	}

	var res struct {
		Data []APIOrder `json:"data"`
	}
	err = a.makeHttpRequest(req, &res)
	if err != nil {
		return err
	}

	if len(res.Data) == 0 {
		return fmt.Errorf("expected res to contain at least 1 order id [%s], res: %+v", orderID, res)
	}

	order := res.Data[0]
	if orderID != order.Sn {
		return fmt.Errorf("returned canceled order id \"%s\", expected order id \"%s\"", order.Sn, orderID)
	}

	return nil
}

type APITrade struct {
	CreatedAt         string  `json:"created_at"`
	Fee               float64 `json:"fee,string"`
	FeeCurrencySymbol string  `json:"fee_currency_symbol"`
	MarketSymbol      string  `json:"market_symbol"`
	Price             float64 `json:"price,string"`
	Quantity          float64 `json:"quantity,string"`
	Side              string  `json:"side"`
	Sn                string  `json:"sn"`
}

func (a *API) Trades(query url.Values) (trades []APITrade, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiTrades, nil)
	if err != nil {
		return trades, err
	}

	req.URL.RawQuery = query.Encode()

	var res struct {
		Data []APITrade `json:"data"`
	}
	err = a.makeHttpRequest(req, &res)
	if err != nil {
		return trades, err
	}

	return res.Data, nil
}

func (a *API) Orders(query url.Values) (orders []APIOrder, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiOrders, nil)
	if err != nil {
		return orders, err
	}

	req.URL.RawQuery = query.Encode()

	var res struct {
		Data []APIOrder `json:"data"`
	}
	err = a.makeHttpRequest(req, &res)
	if err != nil {
		return orders, err
	}

	return res.Data, nil
}

func (a *API) GetOrder(orderID string) (order APIOrder, err error) {
	req, err := a.newHttpRequest(http.MethodGet, fmt.Sprintf(apiOrder, orderID), nil)
	if err != nil {
		return order, err
	}

	err = a.makeHttpRequest(req, &order)
	return order, err
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

func (a *API) newHttpRequest(method string, url string, data io.Reader) (*http.Request, error) {
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
		q := req.URL.Query()
		timestamp := time.Now().UnixMilli()
		timestampStr := strconv.FormatInt(timestamp, 10)

		data := timestampStr
		data += req.Method
		//data += req.Host
		data += req.URL.Path
		data += q.Encode()
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

		req.Header.Set("X-FB-ACCESS-KEY", a.options.ApiKey)
		req.Header.Set("X-FB-ACCESS-TIMESTAMP", timestampStr)
		req.Header.Set("X-FB-ACCESS-SIGNATURE", signature)
	}

	res, err := a.client.SendRequest(req)
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(res.Body)

	return body, err
}
