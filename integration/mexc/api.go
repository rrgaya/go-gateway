package mexc

import (
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
	apiBase         = "https://api.mexc.com"
	apiExchangeInfo = apiBase + "/api/v3/exchangeInfo"
	apiAccount      = apiBase + "/api/v3/account"
	apiOrder        = apiBase + "/api/v3/order"
	apiMyTrades     = apiBase + "/api/v3/myTrades"
	apiDepth        = apiBase + "/api/v3/depth"
	apiTrades       = apiBase + "/api/v3/trades"
	apiOpenOrders   = apiBase + "/api/v3/openOrders"
)

type API struct {
	options    gateway.Options
	httpClient *utils.HttpClient
}

func NewAPI(options gateway.Options) *API {
	client := utils.NewHttpClient()
	client.UseProxies(options.Proxies)

	return &API{
		options:    options,
		httpClient: client,
	}
}

type APISymbol struct {
	BaseAsset                  string        `json:"baseAsset"`
	BaseAssetPrecision         int64         `json:"baseAssetPrecision"`
	BaseCommissionPrecision    int64         `json:"baseCommissionPrecision"`
	Filters                    []interface{} `json:"filters"`
	IcebergAllowed             bool          `json:"icebergAllowed"`
	IsMarginTradingAllowed     bool          `json:"isMarginTradingAllowed"`
	IsSpotTradingAllowed       bool          `json:"isSpotTradingAllowed"`
	OcoAllowed                 bool          `json:"ocoAllowed"`
	OrderTypes                 []string      `json:"orderTypes"`
	Permissions                []string      `json:"permissions"`
	QuoteAsset                 string        `json:"quoteAsset"`
	QuoteAssetPrecision        int64         `json:"quoteAssetPrecision"`
	QuoteCommissionPrecision   int64         `json:"quoteCommissionPrecision"`
	QuoteOrderQtyMarketAllowed bool          `json:"quoteOrderQtyMarketAllowed"`
	QuotePrecision             int64         `json:"quotePrecision"`
	Status                     string        `json:"status"`
	Symbol                     string        `json:"symbol"`
}

type APIExchangeInfo struct {
	ServerTime int64       `json:"serverTime"`
	Symbols    []APISymbol `json:"symbols"`
}

func (a *API) ExchangeInfo() (info APIExchangeInfo, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiExchangeInfo, nil)
	if err != nil {
		return info, err
	}

	err = a.makeHttpRequest(req, &info, false)
	return info, err
}

type APIBalance struct {
	Asset  string  `json:"asset"`
	Free   float64 `json:"free,string"`
	Locked float64 `json:"locked,string"`
}

type APIAccount struct {
	AccountType      string       `json:"accountType"`
	BuyerCommission  int64        `json:"buyerCommission"`
	CanDeposit       bool         `json:"canDeposit"`
	CanTrade         bool         `json:"canTrade"`
	CanWithdraw      bool         `json:"canWithdraw"`
	MakerCommission  int64        `json:"makerCommission"`
	Permissions      []string     `json:"permissions"`
	SellerCommission int64        `json:"sellerCommission"`
	TakerCommission  int64        `json:"takerCommission"`
	Balances         []APIBalance `json:"balances"`
}

func (a *API) Account() (info APIAccount, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiAccount, nil)
	if err != nil {
		return info, err
	}

	err = a.makeHttpRequest(req, &info, true)
	return info, err
}

type APIAccountTrade struct {
	Commission      float64 `json:"commission,string"`
	CommissionAsset string  `json:"commissionAsset"`
	ID              string  `json:"id"`
	IsBestMatch     bool    `json:"isBestMatch"`
	IsBuyer         bool    `json:"isBuyer"`
	IsMaker         bool    `json:"isMaker"`
	OrderID         string  `json:"orderId"`
	Price           float64 `json:"price,string"`
	Qty             float64 `json:"qty,string"`
	QuoteQty        float64 `json:"quoteQty,string"`
	Symbol          string  `json:"symbol"`
	Time            int64   `json:"time"`
}

func (a *API) AccountTrades(params *url.Values) (trades []APIAccountTrade, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiMyTrades, nil)
	if err != nil {
		return trades, err
	}

	req.URL.RawQuery = params.Encode()

	err = a.makeHttpRequest(req, &trades, true)
	return trades, err
}

type APINewOrderRes struct {
	OrderID string `json:"orderId"`
	Symbol  string `json:"symbol"`
}

func (a *API) NewOrder(params *url.Values) (res APINewOrderRes, err error) {
	req, err := a.newHttpRequest(http.MethodPost, apiOrder, nil)
	if err != nil {
		return res, err
	}

	req.URL.RawQuery = params.Encode()

	err = a.makeHttpRequest(req, &res, true)
	return res, err
}

func (a *API) CancelOrder(params *url.Values) (res APIOrder, err error) {
	req, err := a.newHttpRequest(http.MethodDelete, apiOrder, nil)
	if err != nil {
		return res, err
	}

	req.URL.RawQuery = params.Encode()

	err = a.makeHttpRequest(req, &res, true)
	return res, err
}

type APIOrder struct {
	ClientOrderID       string  `json:"clientOrderId"`
	CummulativeQuoteQty float64 `json:"cummulativeQuoteQty,string"`
	ExecutedQty         float64 `json:"executedQty,string"`
	IcebergQty          string  `json:"icebergQty"`
	IsWorking           bool    `json:"isWorking"`
	OrderID             string  `json:"orderId"`
	OrigQty             float64 `json:"origQty,string"`
	OrigQuoteOrderQty   float64 `json:"origQuoteOrderQty,string"`
	Price               float64 `json:"price,string"`
	Side                string  `json:"side"`
	Status              string  `json:"status"`
	StopPrice           float64 `json:"stopPrice,string"`
	Symbol              string  `json:"symbol"`
	Time                int64   `json:"time"`
	TimeInForce         string  `json:"timeInForce"`
	Type                string  `json:"type"`
	UpdateTime          int64   `json:"updateTime"`
}

func (a *API) OpenOrders(params *url.Values) (orders []APIOrder, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiOpenOrders, nil)
	if err != nil {
		return orders, err
	}

	if params != nil {
		req.URL.RawQuery = params.Encode()
	}

	err = a.makeHttpRequest(req, &orders, true)
	return orders, err
}

type APIDepth struct {
	Bids []gateway.PriceArray
	Asks []gateway.PriceArray
}

func (a *API) Depth(params *url.Values) (depth APIDepth, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiDepth, nil)
	if err != nil {
		return depth, err
	}

	req.URL.RawQuery = params.Encode()

	err = a.makeHttpRequest(req, &depth, false)
	return depth, err
}

type APITrade struct {
	IsBestMatch  bool    `json:"isBestMatch"`
	IsBuyerMaker bool    `json:"isBuyerMaker"`
	Price        float64 `json:"price,string"`
	Qty          float64 `json:"qty,string"`
	QuoteQty     float64 `json:"quoteQty,string"`
	Time         int64   `json:"time"`
}

func (a *API) Trades(params *url.Values) (trades []APITrade, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiTrades, nil)
	if err != nil {
		return trades, err
	}

	req.URL.RawQuery = params.Encode()

	err = a.makeHttpRequest(req, &trades, false)
	return trades, err
}

func (a *API) makeHttpRequest(req *http.Request, responseObject interface{}, signedRequest bool) error {
	body, err := a.sendHttpRequest(req, signedRequest)
	if err != nil {
		return err
	}

	errCode, dataType, _, _ := jsonparser.Get(body, "code")
	if dataType != jsonparser.NotExist {
		return fmt.Errorf("MEXC %s %s responded with error code: %s\nresponse body: %s", req.Method, req.URL.String(), string(errCode), string(body))
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

func (a *API) sendHttpRequest(req *http.Request, signed bool) ([]byte, error) {
	if signed {
		req.Header.Set("X-MEXC-APIKEY", a.options.ApiKey)

		q := req.URL.Query()
		q.Set("timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))

		data := q.Encode()

		h := hmac.New(sha256.New, []byte(a.options.ApiSecret))
		h.Write([]byte(data))
		signature := hex.EncodeToString(h.Sum(nil))

		q.Set("signature", signature)
		req.URL.RawQuery = q.Encode()
	}

	res, err := a.httpClient.SendRequest(req)
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(res.Body)

	return body, err
}
