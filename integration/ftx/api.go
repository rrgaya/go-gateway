package ftx

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
	"strings"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

const (
	apiBase           = "https://ftx.com/api"
	apiWallet         = "/wallet"
	apiWalletBalances = "/wallet/balances"
	apiMarkets        = "/markets"
	apiOrders         = "/orders"
	apiModifyOrder    = apiOrders + "/:orderID/modify"
)

type API struct {
	options    gateway.Options
	httpClient *utils.HttpClient
}

type ApiResponse struct {
	Success bool            `json:"success"`
	Result  json.RawMessage `json:"result"`
}

type ApiAuth struct {
	Timestamp  int64
	HttpMethod string
	Path       string
	Body       json.RawMessage
}

type ApiBalance struct {
	Coin  string  `json:"coin"`
	Free  float64 `json:"free"`
	Total float64 `json:"total"`
}

// Names separed by "/" are spot markets, and separed by
// "-" are future markets (e.g. "BTC/USD" for spot, "BTC-PERP"
// for futures)
type ApiMarket struct {
	Name           string  `json:"name"`
	BaseCurrency   string  `json:"baseCurrency"`
	QuoteCurrency  string  `json:"quoteCurrency"`
	Type           string  `json:"type"`
	Underlying     string  `json:"underlying"` // Future markets only
	Enabled        bool    `json:"enabled"`
	PriceIncrement float64 `json:"priceIncrement"`
	SizeIncrement  float64 `json:"sizeIncrement"`
	MinProvideSize float64 `json:"minProvideSize"`
}

type ApiOrder struct {
	ID           int64   `json:"id"`
	Market       string  `json:"market"`
	Type         string  `json:"type"`
	Side         string  `json:"side"`
	Price        float64 `json:"price"`
	Size         float64 `json:"size"`
	FilledSize   float64 `json:"filledSize"`
	RemaingSize  float64 `json:"remaingSize"`
	AvgFillPrice float64 `json:"avgFillPrice"`
	Status       string  `json:"status"`
	CreatedAt    string  `json:"createdAt"`
}

type ModifyOrderReq struct {
	Price   float64 `json:"price,omitempty"`
	Size    float64 `json:"size,omitempty"`
	OrderID string
}

func NewAPI(options gateway.Options) *API {
	client := utils.NewHttpClient()
	client.UseProxies(options.Proxies)

	return &API{
		options:    options,
		httpClient: client,
	}
}

func (a *API) FetchMarkets() (markets []ApiMarket, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiBase+apiMarkets, nil, nil)
	if err != nil {
		return nil, err
	}

	resp, err := a.makeHttpRequest(req)
	if err != nil {
		return markets, err
	}

	err = json.Unmarshal(resp.Result, &markets)
	if err != nil {
		err = fmt.Errorf("Failed to unmarshal FTX API markets. Err: %s", "")
		return markets, err
	}

	return markets, nil
}

func (a *API) FetchBalances() (balances []ApiBalance, err error) {
	httpMethod := http.MethodGet

	auth := ApiAuth{
		Timestamp:  time.Now().UnixMilli(),
		HttpMethod: httpMethod,
		Path:       apiWalletBalances,
	}

	req, err := a.newHttpRequest(httpMethod, apiBase+apiWalletBalances, nil, &auth)
	if err != nil {
		return balances, err
	}

	resp, err := a.makeHttpRequest(req)

	err = json.Unmarshal(resp.Result, &balances)
	if err != nil {
		err = fmt.Errorf("Failed to unmarshal FTX API balances. Err: %s", "")
		return balances, err
	}

	return balances, nil
}

func (a *API) FetchOpenOrders(symbol string) (orders []ApiOrder, err error) {
	httpMethod := http.MethodGet
	path := fmt.Sprintf("%s?market=%s", apiOrders, symbol)
	auth := ApiAuth{
		Timestamp:  time.Now().UnixMilli(),
		HttpMethod: httpMethod,
		Path:       path,
	}

	req, err := a.newHttpRequest(httpMethod, apiBase+path, nil, &auth)
	if err != nil {
		return orders, err
	}

	resp, err := a.makeHttpRequest(req)

	err = json.Unmarshal(resp.Result, &orders)
	if err != nil {
		err = fmt.Errorf("Failed to unmarshal FTX API open orders. Err: %s", "")
		return orders, err
	}

	return orders, nil
}

func (a *API) CreateOrder(order ApiOrder) (respOrder ApiOrder, err error) {
	httpMethod := http.MethodPost
	body, _ := json.Marshal(order)
	auth := ApiAuth{
		Timestamp:  time.Now().UnixMilli(),
		HttpMethod: httpMethod,
		Path:       apiOrders,
		Body:       body,
	}

	req, err := a.newHttpRequest(httpMethod, apiOrders, bytes.NewReader(body), &auth)
	if err != nil {
		return respOrder, err
	}

	resp, err := a.makeHttpRequest(req)

	err = json.Unmarshal(resp.Result, &order)
	if err != nil {
		err = fmt.Errorf("Failed to unmarshal FTX API created order. Err: %s", "")
		return respOrder, err
	}

	return respOrder, nil
}

func (a *API) ModifyOrder(args ModifyOrderReq) (order ApiOrder, err error) {
	httpMethod := http.MethodPost
	body, _ := json.Marshal(args)
	path := strings.Replace(apiModifyOrder, ":orderID", args.OrderID, 1)
	auth := ApiAuth{
		Timestamp:  time.Now().UnixMilli(),
		HttpMethod: httpMethod,
		Path:       path,
		Body:       body,
	}

	req, err := a.newHttpRequest(httpMethod, apiBase+path, bytes.NewReader(body), &auth)
	if err != nil {
		return order, err
	}

	resp, err := a.makeHttpRequest(req)

	err = json.Unmarshal(resp.Result, &order)
	if err != nil {
		err = fmt.Errorf("Failed to unmarshal FTX API mopdified order. Err: %s", "")
		return order, err
	}

	return order, nil
}

func (a *API) CancelOrder(orderID int64) error {
	httpMethod := http.MethodDelete
	path := fmt.Sprintf("/orders/%d", orderID)
	auth := ApiAuth{
		Timestamp:  time.Now().UnixMilli(),
		HttpMethod: httpMethod,
		Path:       path,
	}

	req, err := a.newHttpRequest(httpMethod, apiOrders, nil, &auth)
	if err != nil {
		return err
	}

	_, err = a.makeHttpRequest(req)

	return nil
}

func (a *API) makeHttpRequest(req *http.Request) (resp ApiResponse, err error) {
	body, err := a.sendHttpRequest(req)
	if err != nil {
		return resp, err
	}

	err = json.Unmarshal(body, &resp)

	if err != nil {
		errMsg := fmt.Errorf(
			"Failed to unmarshal FTX API response from %s. Body: %s.\n Err: %s",
			describeBaseRequest(req),
			body,
			err,
		)

		return resp, errMsg
	}

	if resp.Success == false {
		return resp, fmt.Errorf("FTX API returned an error from %s. Body: %s", describeBaseRequest(req), body)
	}

	return resp, nil
}

func (a *API) newHttpRequest(method string, url string, data io.Reader, auth *ApiAuth) (*http.Request, error) {
	req, err := http.NewRequest(method, url, data)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")

	if auth != nil {
		signaturePayload := fmt.Sprintf("%d%s%s%s", auth.Timestamp, auth.HttpMethod, auth.Path, string(auth.Body))
		req.Header.Set("FTX-APIKEY", a.options.ApiKey)
		req.Header.Set("FTX-SIGNATURE", a.sign(signaturePayload))
		req.Header.Set("FTX-TIMESTAMP", fmt.Sprintf("%d", auth.Timestamp))
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

func (a *API) sign(payload string) string {
	mac := hmac.New(sha256.New, []byte(a.options.ApiSecret))
	mac.Write([]byte(payload))

	return hex.EncodeToString(mac.Sum(nil))
}

func describeBaseRequest(req *http.Request) string {
	return fmt.Sprintf("%s %s", req.Method, req.URL.String())
}
