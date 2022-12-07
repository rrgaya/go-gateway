package bitso

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
	"strings"
	"time"

	"github.com/buger/jsonparser"
	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

const (
	apiBase           = "https://api.bitso.com"
	apiOrderBook      = "/v3/order_book"
	apiAvailableBooks = "/v3/available_books"
	apiAccountBalance = "/v3/balance"
	apiOrders         = "/v3/orders"
	apiOpenOrders     = "/v3/open_orders"
)

type ApiResponse struct {
	Success bool            `json:"success"`
	Payload json.RawMessage `json:"payload"`
}

type ApiError struct {
	Message string `json:"message"`
	Code    string `json:"code"`
}

type ApiAuth struct {
	Nonce       int64
	HttpMethod  string
	RequestPath string
	Payload     json.RawMessage
}

// We can receive floats as strings like ".001", what breaks the json struct parse.
// For this reason I receive them with a "X" var, and parse it to the "non-X"
// var as float (e.g., XMaxValue -> parsefloat MaxValue)
type ApiAvailableBook struct {
	Symbol     string `json:"book"`
	XMinAmount string `json:"minimum_amount"`
	XMaxAmount string `json:"maximum_amount"`
	XMinPrice  string `json:"minimum_price"`
	XMaxPrice  string `json:"maximum_price"`
	XMinValue  string `json:"minimum_value"`
	XMaxValue  string `json:"maximum_value"`
	TickSize   string `json:"tick_size"`
	MinAmount  float64
	MaxAmount  float64
	MinPrice   float64
	MaxPrice   float64
	MinValue   float64
	MaxValue   float64
	Fees       ApiFee `json:"fees"`
}

type ApiFee struct {
	FlatRate struct {
		Maker float64 `json:"maker,string"`
		Taker float64 `json:"taker,string"`
	} `json:"flat_tate"`
}

type ApiBalance struct {
	Currency  string  `json:"currency"`
	Total     float64 `json:"total,string"`
	Locked    float64 `json:"locked,string"`
	Available float64 `json:"available,string"`
}

// An order must be specified in terms of major or minor, never both.
// Major = crypto
// Minor = FIAT
type ApiCreateOrderRequest struct {
	Symbol      string `json:"book"`
	Side        string `json:"side"`
	Type        string `json:"type"`
	Major       string `json:"major,omitempty"`
	Minor       string `json:"minor,omitempty"`
	Price       string `json:"price,omitempty"`
	Stop        string `json:"stop,omitempty"`
	TimeInForce string `json:"time_in_force,omitempty"`
	OriginID    string `json:"origin_id,omitempty"`
}

type ApiOrderBook struct {
	Asks []ApiOrderBookResult `json:"asks"`
	Bids []ApiOrderBookResult `json:"bids"`
}

type ApiOrderBookResult struct {
	Symbol string  `json:"book"`
	Price  float64 `json:"price,string"`
	Amount float64 `json:"amount,string"`
}

type ApiOrder struct {
	Symbol         string  `json:"book"`
	OriginalAmount float64 `json:"original_amount,string"`
	UnfilledAmount float64 `json:"unfilled_amount,string"`
	OriginalValue  float64 `json:"original_value,string"`
	CreatedAt      string  `json:"created_at"`
	UpdatedAt      string  `json:"updated_at"`
	Price          float64 `json:"price,string"`
	ID             string  `json:"oid"`
	Side           string  `json:"side"`
	Status         string  `json:"status"`
	Type           string  `json:"type"`
}

type API struct {
	options       gateway.Options
	httpClient    *utils.HttpClient
	lastNonceUsed int64
}

func NewAPI(options gateway.Options) *API {
	client := utils.NewHttpClient()
	client.UseProxies(options.Proxies)

	return &API{
		options:    options,
		httpClient: client,
	}
}

func (api *API) getNonce() int64 {
	nonce := time.Now().UnixMicro()
	if nonce <= api.lastNonceUsed {
		nonce = api.lastNonceUsed + 1
	}

	api.lastNonceUsed = nonce
	return api.lastNonceUsed
}

func (api *API) FetchOrderBook(symbol string) (orderBook ApiOrderBook, err error) {
	url := apiBase + apiOrderBook + "?book=" + symbol
	req, err := api.newHttpRequest(http.MethodGet, url, nil, nil)
	if err != nil {
		return orderBook, err
	}

	response := ApiResponse{}

	if err = api.makeHttpRequest(req, &response); err != nil {
		return orderBook, err
	}

	if err = json.Unmarshal(response.Payload, &orderBook); err != nil {
		return orderBook, fmt.Errorf("Failed in parse Bitso order book, err: %g", err)
	}

	return orderBook, nil
}

func (api *API) FetchSymbols() (symbols []ApiAvailableBook, err error) {
	req, err := api.newHttpRequest(http.MethodGet, apiBase+apiAvailableBooks, nil, nil)
	if err != nil {
		return symbols, err
	}

	response := ApiResponse{}

	if err = api.makeHttpRequest(req, &response); err != nil {
		return symbols, err
	}

	if err = json.Unmarshal(response.Payload, &symbols); err != nil {
		return symbols, fmt.Errorf("Failed in parse Bitso symbols, err: %g", err)
	}

	for _, symbol := range symbols {
		symbol.MaxAmount, _ = strconv.ParseFloat(symbol.XMaxAmount, 64)
		symbol.MinAmount, _ = strconv.ParseFloat(symbol.XMinAmount, 64)
		symbol.MaxPrice, _ = strconv.ParseFloat(symbol.XMaxPrice, 64)
		symbol.MinPrice, _ = strconv.ParseFloat(symbol.XMinPrice, 64)
		symbol.MaxValue, _ = strconv.ParseFloat(symbol.XMaxValue, 64)
		symbol.MinValue, _ = strconv.ParseFloat(symbol.XMinValue, 64)
	}

	return symbols, nil
}

func (api *API) FetchBalances() (balances []ApiBalance, err error) {
	auth := ApiAuth{
		Nonce:       api.getNonce(),
		HttpMethod:  http.MethodGet,
		RequestPath: apiAccountBalance,
	}

	req, err := api.newHttpRequest(http.MethodGet, apiBase+apiAccountBalance, nil, &auth)
	if err != nil {
		return balances, err
	}

	response := ApiResponse{}

	if err = api.makeHttpRequest(req, &response); err != nil {
		return balances, err
	}

	data, _, _, _ := jsonparser.Get(response.Payload, "balances")

	if err = json.Unmarshal(data, &balances); err != nil {
		return balances, fmt.Errorf("Failed in parse Bitso balances, err: %g", err)
	}

	return balances, nil
}

func (api *API) CreateOrder(createOrderReq ApiCreateOrderRequest) (string, error) {
	body, err := json.Marshal(createOrderReq)
	if err != nil {
		return "", err
	}

	auth := ApiAuth{
		Nonce:       api.getNonce(),
		HttpMethod:  http.MethodPost,
		RequestPath: apiOrders,
		Payload:     body,
	}

	req, err := api.newHttpRequest(http.MethodPost, apiBase+apiOrders, bytes.NewReader(body), &auth)
	if err != nil {
		return "", err
	}

	response := ApiResponse{}

	if err = api.makeHttpRequest(req, &response); err != nil {
		return "", err
	}

	oid, _, _, _ := jsonparser.Get(response.Payload, "oid")

	return string(oid), nil
}

func (api *API) CancelOrder(orderID string) error {
	requestPath := fmt.Sprintf("%s/%s", apiOrders, orderID)

	auth := ApiAuth{
		Nonce:       api.getNonce(),
		HttpMethod:  http.MethodDelete,
		RequestPath: requestPath,
	}

	url := apiBase + requestPath

	req, err := api.newHttpRequest(http.MethodDelete, url, nil, &auth)
	if err != nil {
		return err
	}

	if err = api.makeHttpRequest(req, nil); err != nil {
		return err
	}

	return nil
}

func (api *API) FetchOpenOrders(symbol string) (orders []ApiOrder, err error) {
	filterOrdersBySymbol := func(orders []ApiOrder) []ApiOrder {
		n := 0
		for _, order := range orders {
			if order.Symbol == symbol {
				orders[n] = order
				n++
			}
		}

		return orders[:n]
	}

	auth := ApiAuth{
		Nonce:       api.getNonce(),
		HttpMethod:  http.MethodGet,
		RequestPath: apiOpenOrders,
	}

	// TODO: Bitso allow us to filter with the query parameter "book", but
	// there is a bug with this filter that they're solving first. When they
	// launch the fix, we can use the commented url bellow and stop using
	// filterOrdersBySymbol func.
	//url := fmt.Sprintf("%s%s?book=%s", apiBase, apiOpenOrders, symbol)

	req, err := api.newHttpRequest(http.MethodGet, apiBase+apiOpenOrders, nil, &auth)

	if err != nil {
		return orders, err
	}

	response := ApiResponse{}

	if err = api.makeHttpRequest(req, &response); err != nil {
		return orders, err
	}

	if err = json.Unmarshal(response.Payload, &orders); err != nil {
		return orders, fmt.Errorf("Failed in parse Bitso open orders, err: %g", err)
	}

	return filterOrdersBySymbol(orders), nil
}

func (api *API) LookupOrders(ids []string) (orders []ApiOrder, err error) {
	path := fmt.Sprintf("%s?oids=%s", apiOrders, strings.Join(ids, ","))
	url := fmt.Sprintf("%s%s", apiBase, path)

	auth := ApiAuth{
		Nonce:       api.getNonce(),
		HttpMethod:  http.MethodGet,
		RequestPath: path,
	}

	req, err := api.newHttpRequest(http.MethodGet, url, nil, &auth)
	if err != nil {
		return orders, err
	}

	response := ApiResponse{}

	if err = api.makeHttpRequest(req, &response); err != nil {
		return orders, err
	}

	if err = json.Unmarshal(response.Payload, &orders); err != nil {
		return orders, fmt.Errorf("Failed in parse Bitso orders lookup, err: %g", err)
	}

	return orders, nil
}

func (api *API) makeHttpRequest(req *http.Request, responseObject interface{}) error {
	body, err := api.sendHttpRequest(req)
	if err != nil {
		return err
	}

	errVal, dataType, _, _ := jsonparser.Get(body, "error")
	if dataType != jsonparser.NotExist {
		return fmt.Errorf("Bitso %s %s responded with error message: %s\nresponse body: %s", req.Method, req.URL.String(), string(errVal), string(body))
	}

	if responseObject != nil {
		err = json.Unmarshal(body, responseObject)

		if err != nil {
			return fmt.Errorf("failed to unmarshal json, body: %s, unmarshal err: %s", body, err)
		}
	}

	return nil
}

func (api *API) newHttpRequest(method string, url string, data io.Reader, auth *ApiAuth) (*http.Request, error) {
	req, err := http.NewRequest(method, url, data)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")

	if auth != nil {
		message := fmt.Sprint(auth.Nonce) + auth.HttpMethod + auth.RequestPath + string(auth.Payload)

		signer := hmac.New(sha256.New, []byte(api.options.ApiSecret))
		signer.Write([]byte(message))

		signature := hex.EncodeToString(signer.Sum(nil))

		authHeader := fmt.Sprintf("Bitso %s:%v:%s", api.options.ApiKey, auth.Nonce, signature)

		req.Header.Set("Authorization", authHeader)
	}

	return req, nil
}

func (api *API) sendHttpRequest(req *http.Request) ([]byte, error) {
	res, err := api.httpClient.SendRequest(req)

	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(res.Body)

	return body, err
}
