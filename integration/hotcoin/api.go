package hotcoin

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/buger/jsonparser"
	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

const (
	apiBase         = "https://api.hotcoinfin.com"
	apiMatchResults = "/v1/order/matchresults"
	apiOrderDetail  = "/v1/order/detailById"
	apiOrders       = "/v1/order/entrust"
	apiCreateOrder  = "/v1/order/place"
	apiCancelOrder  = "/v1/order/cancel"
	apiSymbols      = "/v1/common/symbols"
	apiBalance      = "/v1/balance"
)

type APIBalance struct {
	Wallet []APIWallet `json:"wallet"`
}

type APIWallet struct {
	CoinName  string  `json:"coinName"`
	Uid       int64   `json:"uid"`
	CoinID    int64   `json:"coinId"`
	Total     float64 `json:"total"`
	Frozen    float64 `json:"frozen"`
	Symbol    string  `json:"symbol"`
	ShortName string  `json:"shortName"`
}

type APIResponse struct {
	Code int             `json:"code"`
	Msg  string          `json:"string"`
	Time int             `json:"time"`
	Data json.RawMessage `json:"data"`
}

type APISymbol struct {
	BaseCurrency    string  `json:"baseCurrency"`
	QuoteCurrency   string  `json:"quoteCurrency"`
	PricePrecision  uint    `json:"pricePrecision"`
	AmountPrecision uint    `json:"amountPrecision"`
	SymbolPartition string  `json:"symbolPartition"`
	Symbol          string  `json:"symbol"`
	State           string  `json:"state"`
	MinOrderCount   float64 `json:"minOrderCount"`
	MaxOrderCount   float64 `json:"maxOrderCount"`
	MinOrderPrice   float64 `json:"minOrderPrice"`
	MaxOrderPrice   float64 `json:"maxOrderPrice"`
}

type APITransaction struct {
	CreatedAt    int64   `json:"createdAt"`
	FilledAmount float64 `json:"filledAmount,string"`
	FilledFees   float64 `json:"filledFees,string"`
	ID           int64   `json:"id"`
	MatchID      int64   `json:"matchId"`
	OrderID      int64   `json:"orderId"`
	Price        float64 `json:"price,string"`
	Type         string  `json:"type"`
	Role         string  `json:"role"`
}

type APIOrderDetails struct {
	CurrentOrders []APIOrder `json:"entrutsCur"`
	HistoryOrders []APIOrder `json:"entrutsHis"`
}

type APIOrder struct {
	ID            int64   `json:"id"`
	Time          string  `json:"time"`
	Types         string  `json:"types"`
	Source        string  `json:"source"`
	Price         float64 `json:"price"`
	Count         float64 `json:"count"`
	LeftCount     float64 `json:"leftcount"`
	Last          float64 `json:"last"`
	SuccessAmount float64 `json:"successamount"`
	Fees          float64 `json:"fees"`
	StatusCode    int     `json:"statusCode"`
	Status        string  `json:"status"`
	Type          int     `json:"type"`
	BuySymbol     string  `json:"buysymbol"`
	SellSymbol    string  `json:"sellsymbol"`
}

type CreateOrderRes struct {
	ID int64 `json:"ID"`
}

type API struct {
	baseURL    string
	authHeader string
	options    gateway.Options
	client     *utils.HttpClient
}

type APIError struct {
	Code        int64  `json:"code"`
	Description string `json:"description"`
	Message     string `json:"message"`
}

func (api *API) Symbols() (res []APISymbol, err error) {
	req, err := api.newHttpRequest(http.MethodGet, apiSymbols, nil, false)
	if err != nil {
		return res, err
	}

	err = api.makeHttpRequest(req, &res)
	return res, err
}

func (api *API) Balance() (res APIBalance, err error) {
	req, err := api.newHttpRequest(http.MethodGet, apiBalance, nil, true)
	if err != nil {
		return res, err
	}

	err = api.makeHttpRequest(req, &res)
	return res, err
}

func (api *API) CreateOrder(params map[string]string) (res CreateOrderRes, err error) {
	req, err := api.newHttpRequest(http.MethodPost, apiCreateOrder, params, true)
	if err != nil {
		return res, err
	}

	err = api.makeHttpRequest(req, &res)
	return res, err
}

func (api *API) CancelOrder(orderID string) error {
	params := make(map[string]string)
	params["id"] = orderID
	req, err := api.newHttpRequest(http.MethodPost, apiCancelOrder, params, true)
	if err != nil {
		return err
	}

	return api.makeHttpRequest(req, nil)
}

func (api *API) MatchResults(params map[string]string) (res []APITransaction, err error) {
	req, err := api.newHttpRequest(http.MethodGet, apiMatchResults, params, true)
	if err != nil {
		return res, err
	}

	err = api.makeHttpRequest(req, &res)
	return res, err
}

func (api *API) GetOrders(params map[string]string) (res APIOrderDetails, err error) {
	req, err := api.newHttpRequest(http.MethodGet, apiOrders, params, true)
	if err != nil {
		return res, err
	}

	err = api.makeHttpRequest(req, &res)
	return res, err
}

func (api *API) GetOrder(orderID string) (res APIOrder, err error) {
	params := make(map[string]string)
	params["id"] = orderID
	req, err := api.newHttpRequest(http.MethodGet, apiOrderDetail, params, true)
	if err != nil {
		return res, err
	}

	err = api.makeHttpRequest(req, &res)
	return res, err
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
		q := req.URL.Query()
		for k, v := range params {
			q.Set(k, v)
		}
		req.URL.RawQuery = q.Encode()
	}

	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}

	if signed {
		q := req.URL.Query()

		q.Set("AccessKeyId", a.options.ApiKey)
		q.Set("SignatureMethod", "HmacSHA256")
		q.Set("SignatureVersion", "2")
		q.Set("Timestamp", time.Now().UTC().Format("2006-01-02T15:04:05"))
		if method == http.MethodPost && params != nil {
			for k, v := range params {
				q.Set(k, v)
			}
		}

		data := req.Method + "\n"
		data += req.Host + "\n"
		data += req.URL.Path + "\n"
		data += q.Encode()

		h := hmac.New(sha256.New, []byte(a.options.ApiSecret))
		h.Write([]byte(data))
		signature := base64.StdEncoding.EncodeToString(h.Sum(nil))

		q.Set("Signature", signature)
		req.URL.RawQuery = q.Encode()
	}

	return req, nil
}

func (api *API) makeHttpRequest(req *http.Request, responseObject interface{}) error {
	body, err := api.sendHttpRequest(req)
	if err != nil {
		return err
	}

	errCode, _ := jsonparser.GetInt(body, "code")
	if errCode != 200 {
		return fmt.Errorf("api responded with error code: %d\nresponse body: %s", errCode, string(body))
	}

	if responseObject != nil {
		apiResponse := APIResponse{}

		err = json.Unmarshal(body, &apiResponse)
		if err != nil {
			return fmt.Errorf("failed to unmarshal json, body: %s, unmarshal err: %s", string(body), err)
		}

		err = json.Unmarshal(apiResponse.Data, responseObject)
		if err != nil {
			return fmt.Errorf("failed to unmarshal json, body: %s, unmarshal err: %s", string(apiResponse.Data), err)
		}
	}

	return nil
}

func (api *API) sendHttpRequest(req *http.Request) ([]byte, error) {
	res, err := api.client.SendRequest(req)
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
