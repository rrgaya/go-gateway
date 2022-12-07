package latoken

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

const (
	apiBase         = "https://api.latoken.com"
	apiHost         = "api.latoken.com"
	apiPlaceOrder   = "/v2/auth/order/place"
	apiCancelOrder  = "/v2/auth/order/cancel"
	apiActiveOrders = "/v2/auth/order/pair/%s/%s/active"
	apiUser         = "/v2/auth/user"
	apiAuthAccount  = "/v2/auth/account"
	apiCurrencies   = "/v2/currency"
	apiPairs        = "/v2/pair"
)

type ApiClient struct {
	options    gateway.Options
	httpClient *utils.HttpClient
}

type BodyParam struct {
	Key   string
	Value interface{}
}

type ApiListRes struct {
	First    bool `json:"first"`
	HasNext  bool `json:"hasNext"`
	PageSize bool `json:"first"`
	Page     struct {
		TotalPages    int64 `json:"totalPages"`
		TotalElements int64 `json:"totalElements"`
	}
	Content json.RawMessage `json:"content"`
}

type ApiOrder struct {
	ID            string  `json:"id"`
	BaseCurrency  string  `json:"baseCurrency"`
	QuoteCurrency string  `json:"quoteCurrency"`
	ChangeType    string  `json:"changeType"`
	ClientOrderID string  `json:"clientOrderId"`
	Condition     string  `json:"condition"`
	Cost          float64 `json:"cost,string"`
	DeltaFilled   float64 `json:"deltaFilled,string"`
	Filled        float64 `json:"filled,string"`
	Price         float64 `json:"price,string"`
	Quantity      float64 `json:"quantity,string"`
	Side          string  `json:"side"`
	Status        string  `json:"status"`
	Timestamp     int64   `json:"timestamp"`
	Type          string  `json:"type"`
	User          string  `json:"user"`
}

type ApiUser struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Role   string `json:"role"`
	Email  string `json:"email"`
}

type ResultResponse struct {
	Result bool            `json:"result"`
	Data   json.RawMessage `json:"data"`
}

func NewApiClient(options gateway.Options) *ApiClient {
	client := utils.NewHttpClient()
	client.UseProxies(options.Proxies)

	return &ApiClient{
		options:    options,
		httpClient: client,
	}
}

type APICurrency struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Status string `json:"status"`
	Tag    string `json:"tag"`
	Tier   int64  `json:"tier"`
	Type   string `json:"type"`
}

type APIPair struct {
	ID               string  `json:"id"`
	BaseCurrency     string  `json:"baseCurrency"`
	QuoteCurrency    string  `json:"quoteCurrency"`
	MinOrderQuantity float64 `json:"minOrderQuantity,string"`
	PriceTick        float64 `json:"priceTick,string"`
	QuantityTick     float64 `json:"quantityTick,string"`
	Status           string  `json:"status"`
}

func (a *ApiClient) GetCurrencies() (res []APICurrency, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiCurrencies, nil)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

func (a *ApiClient) GetPairs() (res []APIPair, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiPairs, nil)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

func (a *ApiClient) GetUser() (ApiUser, error) {
	var res ApiUser

	req, err := a.newHttpRequest(http.MethodGet, apiUser, nil)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

type AccountBalance struct {
	ID        string  `json:"id"`
	Status    string  `json:"status"`
	Type      string  `json:"type"`
	Currency  string  `json:"currency"`
	Timestamp int64   `json:"timestamp"`
	Available float64 `json:"available,string"`
	Blocked   float64 `json:"blocked,string"`
}

func (a *ApiClient) GetBalances() ([]AccountBalance, error) {
	var res []AccountBalance

	req, err := a.newHttpRequest(http.MethodGet, apiAuthAccount, nil)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

func (a *ApiClient) ActiveOrders(base, quote string) ([]ApiOrder, error) {
	var orders []ApiOrder

	url := fmt.Sprintf(apiActiveOrders, base, quote)
	req, err := a.newHttpRequest(http.MethodGet, url, nil)
	if err != nil {
		return orders, err
	}

	err = a.makeHttpRequest(req, &orders)
	if err != nil {
		return orders, err
	}

	return orders, nil
}

func (a *ApiClient) CreateOrder(base, quote, side, _type, condition, price, quantity string) (string, error) {
	bodyParams := []BodyParam{
		BodyParam{"baseCurrency", base},
		BodyParam{"quoteCurrency", quote},
		BodyParam{"side", side},
		BodyParam{"condition", condition},
		BodyParam{"type", _type},
		BodyParam{"price", price},
		BodyParam{"quantity", quantity},
	}

	req, err := a.newHttpRequest(http.MethodPost, apiPlaceOrder, bodyParams)
	if err != nil {
		return "", err
	}

	var res struct {
		Message string `json:"message"`
		Status  string `json:"status"`
		ID      string `json:"id"`
	}

	reqAt := time.Now()

	err = a.makeHttpRequest(req, &res)
	if err != nil {
		return "", err
	}

	if res.Status != "SUCCESS" {
		return "", fmt.Errorf("Failed request, status '%s', 'error msg '%s'", res.Status, res.Message)
	}

	resAt := time.Now()

	log.Printf("[latoken] Posted %s %s (%v @ %v) pair %s_%s order %s. Exchange response time: %fms", side, _type, quantity, price, base, quote, res.ID, float64(resAt.UnixNano()-reqAt.UnixNano())/float64(time.Millisecond))

	return res.ID, nil
}

func (a *ApiClient) CancelOrder(orderID string) error {
	bodyParams := []BodyParam{
		BodyParam{"id", orderID},
	}

	req, err := a.newHttpRequest(http.MethodPost, apiCancelOrder, bodyParams)
	if err != nil {
		return err
	}

	var res struct {
		Message string `json:"message"`
		Status  string `json:"status"`
		ID      string `json:"id"`
	}

	err = a.makeHttpRequest(req, &res)
	if err != nil {
		return err
	}

	if res.Status != "SUCCESS" {
		return fmt.Errorf("Failed request, status '%s', 'error msg '%s'", res.Status, res.Message)
	}

	return nil
}

func (a *ApiClient) makeHttpRequest(req *http.Request, responseObject interface{}) error {
	res, err := a.httpClient.SendRequest(req)
	if err != nil {
		return err
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}

	err = json.Unmarshal(body, responseObject)
	if err != nil {
		return fmt.Errorf("failed to unmarshal, data: %s, error: %s", string(body), err)
	}

	return nil
}

func (a *ApiClient) newHttpRequest(method string, path string, bodyParams []BodyParam) (*http.Request, error) {
	if string(path[0]) != "/" {
		path = "/" + path
	}

	// Sort params by key, or else, the servers
	// generated signature wont match ours
	sort.Slice(bodyParams, func(i, j int) bool {
		return bodyParams[i].Key < bodyParams[j].Key
	})

	var data io.Reader
	if len(bodyParams) > 0 {
		obj := make(map[string]interface{})
		for _, b := range bodyParams {
			obj[b.Key] = b.Value
		}

		body, err := json.Marshal(obj)
		if err != nil {
			return nil, err
		}

		data = bytes.NewReader(body)
	}

	url := apiBase + path
	req, err := http.NewRequest(method, url, data)
	if err != nil {
		return nil, err
	}

	req.Host = apiHost
	req.Header.Set("Content-Type", "application/json;charset=UTF-8")

	if a.options.UserAgent != "" {
		req.Header.Add("User-Agent", a.options.UserAgent)
	}
	if a.options.Cookie != "" {
		req.Header.Add("Cookie", a.options.Cookie)
	}

	if a.options.ApiKey != "" {
		// Sign request
		signData := req.Method + req.URL.Path + req.URL.RawQuery
		if len(bodyParams) > 0 {
			values := make([]string, 0)
			for _, b := range bodyParams {
				values = append(values, fmt.Sprintf("%s=%v", b.Key, b.Value))
			}

			signData += strings.Join(values, "&")
		}
		h := hmac.New(sha256.New, []byte(a.options.ApiSecret))
		h.Write([]byte(signData))
		signature := hex.EncodeToString(h.Sum(nil))

		req.Header.Set("X-LA-APIKEY", a.options.ApiKey)
		req.Header.Set("X-LA-SIGNATURE", signature)
	}

	return req, nil
}
