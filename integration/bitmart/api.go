package bitmart

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
	"strconv"
	"time"

	"github.com/buger/jsonparser"
	"github.com/google/go-querystring/query"
	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

const (
	apiBase         = "https://api-cloud.bitmart.com"
	apiSymbols      = apiBase + "/spot/v1/symbols/details"
	apiSubmitOrder  = apiBase + "/spot/v1/submit_order"
	apiCancelOrder  = apiBase + "/spot/v2/cancel_order"
	apiOrderHistory = apiBase + "/spot/v2/orders"
	apiOrderDetail  = apiBase + "/spot/v1/order_detail"
	apiBalances     = apiBase + "/account/v1/wallet"
)

type API struct {
	options gateway.Options
	client  *utils.HttpClient
}

type ApiResponse struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

func NewAPI(options gateway.Options) *API {
	client := utils.NewHttpClient()
	client.UseProxies(options.Proxies)

	return &API{
		options: options,
		client:  client,
	}
}

type ApiGetMarketsResponse struct {
	Symbols []ApiMarket `json:"symbols"`
}

type ApiMarket struct {
	Symbol            string  `json:"symbol"`
	SymbolId          int     `json:"symbol_id"`
	BaseCurrency      string  `json:"base_currency"`
	QuoteCurrency     string  `json:"quote_currency"`
	QuoteIncrement    float64 `json:"quote_increment,string"`
	BaseMinSize       float64 `json:"base_min_size,string"`
	BaseMaxSize       float64 `json:"base_max_size,string"`
	PriceMinPrecision int     `json:"price_min_precision"`
	PriceMaxPrecision int     `json:"price_max_precision"`
	MinBuyAmount      float64 `json:"min_buy_amount,string"`
	MinSellAmount     float64 `json:"min_sell_amount,string"`
	TradeStatus       string  `json:"trade_status"`
}

func (a *API) GetMarkets() (markets []ApiMarket, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiSymbols, nil)
	if err != nil {
		return nil, err
	}

	res := ApiGetMarketsResponse{}

	err = a.makeHttpRequest(req, &res)
	if err != nil {
		return nil, err
	}

	return res.Symbols, nil
}

type ApiPostOrderResponse struct {
	OrderId int64 `json:"order_id"`
}

func (a *API) PostOrder(postOrderRequest ApiPlaceOrder) (res ApiPostOrderResponse, err error) {
	data, err := json.Marshal(postOrderRequest)
	if err != nil {
		return res, err
	}

	req, err := a.newHttpRequest(http.MethodPost, apiSubmitOrder, bytes.NewReader(data))
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

type ApiCancelOrderRequest struct {
	OrderId string `json:"order_id"`
}

type ApiCancelOrderResponse struct {
	Result bool `json:"result"`
}

func (a *API) CancelOrder(cancelOrderRequest ApiCancelOrderRequest) error {
	data, err := json.Marshal(cancelOrderRequest)
	if err != nil {
		return err
	}

	req, err := a.newHttpRequest(http.MethodPost, apiCancelOrder, bytes.NewReader(data))
	if err != nil {
		return err
	}

	res := ApiCancelOrderResponse{}
	err = a.makeHttpRequest(req, &res)
	if err != nil {
		return err
	}

	if !res.Result {
		err = utils.NewHttpError(
			Exchange.Name,
			utils.HttpRequestError,
			req,
			"Cancel order failed.",
			"",
		).AsError()

		return err
	}

	return err
}

type ApiGetOrderHistroyRequest struct {
	Symbol string `url:"symbol"`
	Limit  int    `url:"N"`
	Status string `url:"status"`
}

type ApiGetOrderHistroyResponse struct {
	Orders []ApiOrder `url:"orders"`
}

type ApiPlaceOrder struct {
	Symbol string `json:"symbol"`
	Side   string `json:"side"`
	Type   string `json:"type"`
	Price  string `json:"price"`
	Size   string `json:"size"`
}

type ApiOrder struct {
	OrderId        int64   `json:"order_id"`
	Symbol         string  `json:"symbol"`
	Side           string  `json:"side"`
	Type           string  `json:"type"`
	Price          float64 `json:"price,string"`
	Size           float64 `json:"size,string"`
	UnfilledVolume float64 `json:"unfilled_volume,string"`
	Status         string  `json:"status"`
	// Statuses:
	// 1=Order failure
	// 2=Placing order
	// 3=Order failure, Freeze failure
	// 4=Order success, Pending for fulfilment
	// 5=Partially filled
	// 6=Fully filled
	// 7=Canceling
	// 8=Canceled
}

func (a *API) GetOrderHistory(request ApiGetOrderHistroyRequest) ([]ApiOrder, error) {
	data, _ := query.Values(request)

	req, err := a.newHttpRequest(http.MethodGet, apiOrderHistory+"?"+data.Encode(), nil)
	if err != nil {
		return nil, err
	}

	res := ApiGetOrderHistroyResponse{}
	err = a.makeHttpRequest(req, &res)
	if err != nil {
		return nil, err
	}

	return res.Orders, nil
}

type ApiGetBalancesResponse struct {
	Wallet []ApiBalance `json:"wallet"`
}

type ApiBalance struct {
	Currency  string  `json:"currency"`
	Name      string  `json:"name"`
	Available float64 `json:"available,string"`
	Frozen    float64 `json:"frozen,string"`
}

func (a *API) GetBalances() (orders []ApiBalance, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiBalances, nil)
	if err != nil {
		return nil, err
	}

	res := ApiGetBalancesResponse{}
	err = a.makeHttpRequest(req, &res)
	if err != nil {
		return nil, err
	}

	return res.Wallet, nil
}

func (a *API) GetOrder(orderId string) (order *ApiOrder, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiOrderDetail+"?order_id="+orderId, nil)
	if err != nil {
		return nil, err
	}

	err = a.makeHttpRequest(req, &order)
	if err != nil {
		return nil, err
	}

	return order, nil
}

func (a *API) makeHttpRequest(req *http.Request, responseObject interface{}) error {
	body, err := a.sendHttpRequest(req)
	if err != nil {
		return err
	}

	msg, _, _, _ := jsonparser.Get(body, "message")
	if string(msg) != "OK" {
		return utils.NewHttpError(
			Exchange.Name,
			utils.HttpRequestError,
			req,
			string(msg),
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
		timestamp := time.Now().UnixMilli()
		timestampStr := strconv.FormatInt(timestamp, 10)
		var content string

		if req.Body != nil {
			reqBody, _ := req.GetBody()
			body, err := ioutil.ReadAll(reqBody)
			if err != nil {
				return nil, err
			}

			content = string(body)
		} else {
			urlQuery := req.URL.Query()
			content = urlQuery.Encode()
		}

		data := fmt.Sprintf("%d#%s#%s", timestamp, a.options.ApiMemo, content)

		h := hmac.New(sha256.New, []byte(a.options.ApiSecret))
		h.Write([]byte(data))
		signature := hex.EncodeToString(h.Sum(nil))

		req.Header.Set("X-BM-KEY", a.options.ApiKey)
		req.Header.Set("X-BM-TIMESTAMP", timestampStr)
		req.Header.Set("X-BM-SIGN", signature)
	}

	res, err := a.client.SendRequest(req)
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(res.Body)

	return body, err
}
