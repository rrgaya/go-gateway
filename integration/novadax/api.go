package novadax

import (
	"bytes"
	"crypto/hmac"
	"crypto/md5"
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

	"github.com/google/go-querystring/query"
	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

const (
	apiBase         = "https://api.novadax.com"
	apiSymbols      = apiBase + "/v1/common/symbols"
	apiCreateOrder  = apiBase + "/v1/orders/create"
	apiCancelOrder  = apiBase + "/v1/orders/cancel"
	apiOrderHistory = apiBase + "/v1/orders/list"
	apiBalance      = apiBase + "/v1/account/getBalance"
	apiOrderBook    = apiBase + "/v1/market/depth"
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

type apiResponse struct {
	Code    string          `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

type ApiSymbolData struct {
	Symbol            string  `json:"symbol"`
	BaseCurrency      string  `json:"baseCurrency"`
	QuoteCurrency     string  `json:"quoteCurrency"`
	PricePrecision    int     `json:"pricePrecision"`
	AmountPrecision   int     `json:"amountPrecision"`
	ValuePrecision    float64 `json:"valuePrecision"`
	MinOrderAmount    float64 `json:"minOrderAmount,string"`
	MinimumOrderValue float64 `json:"minimumOrderValue,string"`
}

func (a *API) GetSymbols() ([]ApiSymbolData, error) {
	req, err := a.newHttpRequest(http.MethodGet, apiSymbols, nil)
	if err != nil {
		return nil, err
	}

	symbols := make([]ApiSymbolData, 0)
	err = a.makeHttpRequest(req, &symbols)
	if err != nil {
		return nil, err
	}

	return symbols, err
}

type ApiOrderRequest struct {
	Symbol string `json:"symbol"`
	Type   string `json:"type"`
	Side   string `json:"side"`
	Amount string `json:"amount"`
	Price  string `json:"price"`
}

type ApiOrder struct {
	Id           string  `json:"id"`
	Symbol       string  `json:"symbol"`
	Type         string  `json:"type:"`
	Side         string  `json:"side"`
	Price        float64 `json:"price,string"`
	AveragePrice float64 `json:"averagePrice,string"`
	Amount       float64 `json:"amount,string"`
	FilledAmount float64 `json:"filledAmount,string"`
	Value        float64 `json:"value,string"`
	FilledValue  float64 `json:"filledValue,string"`
	Status       string  `json:"status"`
}

func (a *API) PostOrder(bodyReq ApiOrderRequest) (*ApiOrder, error) {
	data, _ := json.Marshal(bodyReq)

	req, err := a.newHttpRequest(http.MethodPost, apiCreateOrder, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	a.SetAuthHeaders(req, data)

	apiOrder := &ApiOrder{}
	err = a.makeHttpRequest(req, apiOrder)
	if err != nil {
		return nil, err
	}

	return apiOrder, err
}

type apiCancelOrderRequest struct {
	Id string `json:"id"`
}

type apiCancelOrderData struct {
	Result bool `json:"result"`
}

func (a *API) CancelOrder(id string) error {
	reqParams := apiCancelOrderRequest{Id: id}
	data, _ := json.Marshal(reqParams)

	req, err := a.newHttpRequest(http.MethodPost, apiCancelOrder, bytes.NewReader(data))
	if err != nil {
		return err
	}

	a.SetAuthHeaders(req, data)

	resp := &apiCancelOrderData{}
	err = a.makeHttpRequest(req, resp)
	if err != nil {
		return err
	}

	return nil
}

type ApiGetOrderHistoryRequest struct {
	Status string `url:"status,omitempty"`
}

func (a *API) GetOrderHistory(paramsReq ApiGetOrderHistoryRequest) ([]ApiOrder, error) {
	data, _ := query.Values(paramsReq)
	url := fmt.Sprintf("%s?%s", apiOrderHistory, data.Encode())
	req, err := a.newHttpRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	a.SetAuthHeaders(req, nil)

	orders := make([]ApiOrder, 0)

	err = a.makeHttpRequest(req, &orders)
	if err != nil {
		return nil, err
	}

	return orders, err
}

type ApiOrderBookRequest struct {
	Limit  int    `url:"limit"`
	Symbol string `url:"symbol"`
}

type apiOrderBookData struct {
	Bids []gateway.PriceArray `json:"bids"`
	Asks []gateway.PriceArray `json:"asks"`
}

func (a *API) GetOrderBook(paramsReq ApiOrderBookRequest) (*apiOrderBookData, error) {
	data, _ := query.Values(paramsReq)
	url := fmt.Sprintf("%s?%s", apiOrderBook, data.Encode())
	req, err := a.newHttpRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	book := apiOrderBookData{}

	err = a.makeHttpRequest(req, &book)
	if err != nil {
		return nil, err
	}

	return &book, nil
}

type apiAccountBalance struct {
	Currency  string  `json:"currency"`
	Balance   float64 `json:"balance,string"`
	Hold      float64 `json:"hold,string"`
	Available float64 `json:"available,string"`
}

func (a *API) GetBalances() ([]apiAccountBalance, error) {
	req, err := a.newHttpRequest(http.MethodGet, apiBalance, nil)
	if err != nil {
		return nil, err
	}

	a.SetAuthHeaders(req, nil)

	balances := make([]apiAccountBalance, 0)

	err = a.makeHttpRequest(req, &balances)
	if err != nil {
		return nil, err
	}

	return balances, nil
}

func (a *API) makeHttpRequest(req *http.Request, responseObject interface{}) error {
	body, err := a.sendHttpRequest(req)
	if err != nil {
		return err
	}

	response := apiResponse{}
	err = json.Unmarshal(body, &response)
	if err != nil {
		return utils.NewHttpError(
			Exchange.Name,
			utils.UnmarshalError,
			req,
			err.Error(),
			string(body),
		).AsError()
	}

	if response.Code != "A10000" {
		return utils.NewHttpError(
			Exchange.Name,
			utils.HttpRequestError,
			req,
			response.Message,
			string(body),
		).AsError()
	}

	if responseObject != nil {
		err = json.Unmarshal(response.Data, responseObject)
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

	if req.Method == "POST" {
		req.Header.Set("Content-Type", "application/json")
	}

	return req, nil
}

func (a *API) sendHttpRequest(req *http.Request) ([]byte, error) {
	res, err := a.httpClient.SendRequest(req)
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(res.Body)

	return body, err
}

func (a *API) SetAuthHeaders(req *http.Request, body []byte) {
	timestamp := time.Now().UnixMilli()
	signature := ""
	url, _ := url.Parse(req.URL.String())

	switch req.Method {
	case "POST":
		md5 := a.getMD5(body)
		msg := fmt.Sprintf("%s\n%s\n%s\n%v", req.Method, url.Path, md5, timestamp)
		signature = a.signMessage(msg)
	case "GET":
		orderedQuery := url.Query().Encode()
		msg := fmt.Sprintf("%s\n%s\n%s\n%v", req.Method, url.Path, orderedQuery, timestamp)
		signature = a.signMessage(msg)
	default:
		apiErr := utils.NewHttpError(
			Exchange.Name,
			utils.HttpRequestError,
			req,
			"No authentication strategy implemented.",
			"",
		)
		panic(apiErr.AsError())
	}

	req.Header.Set("X-Nova-Access-Key", a.options.ApiKey)
	req.Header.Set("X-Nova-Signature", signature)
	req.Header.Set("X-Nova-Timestamp", strconv.FormatInt(timestamp, 10))
}

func (a *API) signMessage(message string) string {
	signer := hmac.New(sha256.New, []byte(a.options.ApiSecret))
	signer.Write([]byte(message))

	return hex.EncodeToString(signer.Sum(nil))
}

func (a *API) getMD5(message []byte) string {
	hasher := md5.New()
	hasher.Write(message)

	return hex.EncodeToString(hasher.Sum(nil))
}
