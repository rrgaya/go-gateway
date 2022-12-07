package chiliz

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/buger/jsonparser"
	"github.com/google/go-querystring/query"
	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

// https://chiliz.zendesk.com/hc/en-us/articles/360011405400-Chiliz-net-Rest-API-
const (
	apiBase          = "https://api.chiliz.net/openapi"
	apiSymbols       = apiBase + "/v1/brokerInfo"
	apiAccount       = apiBase + "/v1/account"
	apiOrder         = apiBase + "/v1/order"
	apiOpenOrders    = apiBase + "/v1/openOrders"
	apiOrdersHistory = apiBase + "/v1/historyOrders"
)

type API struct {
	options    gateway.Options
	httpClient *utils.HttpClient
}

type SymbolsResponse struct {
	Symbols []SymbolResponse `json:"symbols"`
}

type SymbolResponse struct {
	Filters            []SymbolFilter
	ExchangeId         int     `json:"exchangeId,string"`
	Symbol             string  `json:"symbol"`
	SymbolName         string  `json:"symbolName"`
	Status             string  `json:"status"`
	BaseAsset          string  `json:"baseAsset"`
	BaseAssetName      string  `json:"baseAssetName"`
	BaseAssetPrecision float64 `json:"baseAssetPrecision,string"`
	QuoteAsset         string  `json:"quoteAsset"`
	QuoteAssetName     string  `json:"quoteAssetName"`
	QuotePrecision     float64 `json:"quotePrecision,string"`
	IcerberAllowed     bool    `json:"icerberAllowed"`
	IsAggregate        bool    `json:"isAggregate"`
	AllowMargin        bool    `json:"allowMargin"`
}

type SymbolFilter struct {
	MinPrice   float64 `json:"minPrice,string"`
	MaxPrice   float64 `json:"maxPrice,string"`
	MinQty     float64 `json:"minQty,string"`
	MaxQty     float64 `json:"maxQty,string"`
	TickSize   float64 `json:"tickSize,string"`
	FilterType string  `json:"filterType"`
}

type AccountResponse struct {
	CanTrade    bool   `json:"canTrade"`
	CanWithdraw bool   `json:"canWithdraw"`
	CanDeposit  bool   `json:"canDeposit"`
	UpdateTime  string `json:"updateTime"`
	Balances    []struct {
		Asset  string  `json:"asset"`
		Free   float64 `json:"free,string"`
		Locked float64 `json:"locked,string"`
	} `json:"balances"`
}

type BalanceRequest struct {
	Timestamp  int64  `url:"timestamp"`
	RecvWindow string `url:"recvWindow,omitempty"`
	Signature  string `url:"signature,omitempty"`
}

type CreateOrderRequest struct {
	Symbol           string  `url:"symbol"`
	AssetType        string  `url:"assetType,omitempty"`
	Side             string  `url:"side"`
	Type             string  `url:"type"`
	TimeInForce      string  `url:"timeInForce,omitempty"`
	Quantity         float64 `url:"quantity"`
	Price            float64 `url:"price,omitempty"`
	NewClientOrderID string  `url:"newClientOrderId,omitempty"`
	StopPrice        string  `url:"stopPrice,omitempty"`
	IcebergQty       float64 `url:"icebergQty,omitempty"`
	RecvWindow       int     `url:"recvWindow,omitempty"`
	Timestamp        int64   `url:"timestamp"`
	Signature        string  `url:"signature,omitempty"`
}

type CreateOrderResponse struct {
	OrderID       string `json:"orderId"`
	ClientOrderID string `json:"clientOrderId"`
}

// Either orderId or clientOrderId must be sent.
type CancelOrderRequest struct {
	OrderID       int64  `url:"orderId,omitempty"`
	ClientOrderID string `url:"clientOrderId,omitempty"`
	RecvWindow    string `url:"recvWindow,omitempty"`
	Timestamp     int64  `url:"timestamp"`
	Signature     string `url:"signature,omitempty"`
}

type CancelOrderResponse struct {
	Symbol        string `json:"symbol"`
	ClientOrderID string `json:"clientOrderId"`
	OrderID       string `json:"orderId"`
	Status        string `json:"status"`
}

// If orderId is set, it will get orders < that orderId. Otherwise most recent orders are returned.
type OpenOrdersRequest struct {
	Symbol     string `url:"symbol,omitempty"`
	OrderID    string `url:"orderId,omitempty"`
	Limit      int    `url:"limit,omitempty"`
	RecvWindow string `url:"recvWindow,omitempty"`
	Timestamp  int64  `url:"timestamp"`
	Signature  string `url:"signature,omitempty"`
}

type OrdersHistoryRequest struct {
	Symbol     string `url:"symbol,omitempty"`
	OrderID    string `url:"orderId,omitempty"`
	Limit      int    `url:"limit,omitempty"`
	StartTime  int64  `url:"startTime,omitempty"`
	EndTime    int64  `url:"endTime,omitempty"`
	RecvWindow string `url:"recvWindow,omitempty"`
	Timestamp  int64  `url:"timestamp"`
	Signature  string `url:"signature,omitempty"`
}

type Order struct {
	Symbol              string  `json:"symbol"`
	OrderID             string  `json:"orderId"`
	ClientOrderID       string  `json:"clientOrderId"`
	Price               float64 `json:"price,string"`
	OrigQty             float64 `json:"origQty,string"`
	ExecutedQty         float64 `json:"executedQty,string"`
	CummulativeQuoteQty float64 `json:"cummulativeQuoteQty,string"`
	AvgPrice            float64 `json:"avgPrice,string"`
	Status              string  `json:"status"`
	TimeInForce         string  `json:"timeInForce"`
	Type                string  `json:"type"`
	Side                string  `json:"side"`
	StopPrice           float64 `json:"stopPrice,string"`
	IcebergQty          float64 `json:"icebergQty,string"`
	Time                int64   `json:"time,string"`
	UpdateTime          int64   `json:"updateTime,string"`
	IsWorking           bool    `json:"isWorking"`
}

func NewAPI(options gateway.Options) *API {
	client := utils.NewHttpClient()
	client.UseProxies(options.Proxies)

	return &API{
		options:    options,
		httpClient: client,
	}
}

func (s *SymbolResponse) SelectFilter(filterType string) (*SymbolFilter, error) {
	for _, filter := range s.Filters {
		if filter.FilterType == filterType {
			return &filter, nil
		}
	}

	return nil, errors.New(fmt.Sprintf("Couldn't find filterType %v", filterType))
}

func (api *API) Symbols() (symbols []SymbolResponse, err error) {
	req, err := api.newHttpRequest(http.MethodGet, apiSymbols, nil)
	if err != nil {
		return symbols, err
	}

	symbolsResponse := SymbolsResponse{}

	err = api.makeHttpRequest(req, &symbolsResponse)
	return symbolsResponse.Symbols, err
}

// It's from here we get thse user balances.
func (api *API) FetchAccount() (AccountResponse, error) {
	var account AccountResponse
	balanceRequest := BalanceRequest{
		Timestamp: time.Now().UnixMilli(),
	}
	partialData, _ := query.Values(balanceRequest)

	balanceRequest.Signature = api.generateSignature(partialData.Encode())

	data, _ := query.Values(balanceRequest)
	encodedData := data.Encode()

	url := fmt.Sprintf("%s?%s", apiAccount, encodedData)

	req, err := api.newHttpRequest(http.MethodGet, url, nil)

	if err != nil {
		return account, err
	}

	err = api.makeHttpRequest(req, &account)
	return account, err
}

func (api *API) CreateOrder(createOrderReq CreateOrderRequest) (order CreateOrderResponse, err error) {
	createOrderReq.Timestamp = time.Now().UnixMilli()

	partialData, _ := query.Values(createOrderReq)

	createOrderReq.Signature = api.generateSignature(partialData.Encode())

	data, _ := query.Values(createOrderReq)
	encodedData := data.Encode()

	url := fmt.Sprintf("%s?%s", apiOrder, encodedData)

	req, err := api.newHttpRequest(http.MethodPost, url, nil)
	if err != nil {
		return order, err
	}

	err = api.makeHttpRequest(req, &order)
	return order, err
}

func (api *API) CancelOrder(cancelOrderReq CancelOrderRequest) (order CancelOrderResponse, err error) {
	cancelOrderReq.Timestamp = time.Now().UnixMilli()

	partialData, _ := query.Values(cancelOrderReq)

	cancelOrderReq.Signature = api.generateSignature(partialData.Encode())

	data, _ := query.Values(cancelOrderReq)
	encodedData := data.Encode()

	url := fmt.Sprintf("%s?%s", apiOrder, encodedData)

	req, err := api.newHttpRequest(http.MethodDelete, url, nil)
	if err != nil {
		return order, err
	}

	err = api.makeHttpRequest(req, &order)
	return order, err
}

func (api *API) FetchOpenOrders(openOrdersReq OpenOrdersRequest) (orders []Order, err error) {
	openOrdersReq.Timestamp = time.Now().UnixMilli()

	partialData, _ := query.Values(openOrdersReq)

	openOrdersReq.Signature = api.generateSignature(partialData.Encode())

	data, _ := query.Values(openOrdersReq)
	encodedData := data.Encode()

	url := fmt.Sprintf("%s?%s", apiOpenOrders, encodedData)

	req, err := api.newHttpRequest(http.MethodGet, url, nil)
	if err != nil {
		return orders, err
	}

	err = api.makeHttpRequest(req, &orders)
	return orders, err
}

func (api *API) FetchOrdersHistory(ordersHistoryReq OrdersHistoryRequest) (orders []Order, err error) {
	ordersHistoryReq.Timestamp = time.Now().UnixMilli()

	partialData, _ := query.Values(ordersHistoryReq)

	ordersHistoryReq.Signature = api.generateSignature(partialData.Encode())

	data, _ := query.Values(ordersHistoryReq)
	encodedData := data.Encode()

	url := fmt.Sprintf("%s?%s", apiOrdersHistory, encodedData)

	req, err := api.newHttpRequest(http.MethodGet, url, nil)
	if err != nil {
		return orders, err
	}

	err = api.makeHttpRequest(req, &orders)
	return orders, err
}

func (api *API) makeHttpRequest(req *http.Request, responseObject interface{}) error {
	body, err := api.sendHttpRequest(req)
	if err != nil {
		return err
	}

	errVal, dataType, _, _ := jsonparser.Get(body, "code")
	if dataType != jsonparser.NotExist {
		req.URL.RequestURI()
		return fmt.Errorf("api responded with error message: %s\nresponse body: %s", string(errVal), string(body))
	}

	if responseObject != nil {
		err = json.Unmarshal(body, responseObject)

		if err != nil {
			return fmt.Errorf("failed to unmarshal json, body: %s, unmarshal err: %s", body, err)
		}
	}

	return nil
}

func (api *API) newHttpRequest(method string, url string, data io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, url, data)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	return req, nil
}

func (api *API) sendHttpRequest(req *http.Request) ([]byte, error) {
	isAuthenticated := api.options.ApiKey != ""

	if isAuthenticated {
		req.Header.Set("X-BH-APIKEY", api.options.ApiKey)
	}

	res, err := api.httpClient.SendRequest(req)
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(res.Body)

	return body, err
}

func (api *API) generateSignature(body string) string {
	signer := hmac.New(sha256.New, []byte(api.options.ApiSecret))
	signer.Write([]byte(body))

	return hex.EncodeToString(signer.Sum(nil))
}
