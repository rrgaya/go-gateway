package huobi

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

const (
	apiBase           = "https://api.huobi.pro"
	apiTimestamp      = apiBase + "/v1/common/timestamp"
	apiSymbols        = apiBase + "/v1/common/symbols"
	apiAccounts       = apiBase + "/v1/account/accounts"
	apiAccountBalance = apiBase + "/v1/account/accounts/%d/balance"
	apiPlaceOrder     = apiBase + "/v1/order/orders/place"
	apiCancelOrder    = apiBase + "/v1/order/orders/%s/submitcancel"
	apiOpenOrders     = apiBase + "/v1/order/openOrders"
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

func (a *API) Timestamp() (ts int64, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiTimestamp, nil)
	if err != nil {
		return ts, err
	}

	err = a.makeHttpRequest(req, &ts)
	return ts, err
}

type APISymbol struct {
	BaseCurrency       string  `json:"base-currency"`
	QuoteCurrency      string  `json:"quote-currency"`
	PricePrecision     int     `json:"price-precision"`
	AmountPrecision    int     `json:"amount-precision"`
	SymbolPartition    string  `json:"symbol-partition"`
	Innovation         string  `json:"innovation"`
	State              string  `json:"state"`
	ValuePrecision     int     `json:"value-precision"`
	MinimumOrderAmount float64 `json:"min-order-amt"`
	MaximumOrderAmount float64 `json:"max-order-amt"`
	MinimumOrderValue  float64 `json:"min-order-value"`
}

func (a *API) Symbols() (symbols []APISymbol, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiSymbols, nil)
	if err != nil {
		return symbols, err
	}

	err = a.makeHttpRequest(req, &symbols)
	return symbols, err
}

type APIAccount struct {
	ID     int64  `json:"id"`
	Type   string `json:"type"`
	State  string `json:"state"`
	UserID int64  `json:"user-id"`
}

func (a *API) Accounts() (res []APIAccount, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiAccounts, nil)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

type APIAccountBalance struct {
	ID    int64                     `json:"id"`
	Type  string                    `json:"type"`
	State string                    `json:"state"`
	List  []APIAccountBalanceDetail `json:"list"`
}

type APIAccountBalanceDetail struct {
	Currency string  `json:"currency"`
	Type     string  `json:"type"`
	Balance  float64 `json:"balance,string"`
}

func (a *API) AccountBalance(accountID int64) (res APIAccountBalance, err error) {
	url := fmt.Sprintf(apiAccountBalance, accountID)
	req, err := a.newHttpRequest(http.MethodGet, url, nil)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

type APIPlaceOrder struct {
	AccountID int64  `json:"account-id,string"`
	Amount    string `json:"amount"`
	Price     string `json:"price"`
	Source    string `json:"source,omitempty"`
	Symbol    string `json:"symbol"`
	Type      string `json:"type"`
}

func (a *API) PlaceOrder(params APIPlaceOrder) (orderID string, err error) {
	reqData, err := json.Marshal(params)
	if err != nil {
		return "", err
	}

	req, err := a.newHttpRequest(http.MethodPost, apiPlaceOrder, bytes.NewReader(reqData))
	if err != nil {
		return "", err
	}

	err = a.makeHttpRequest(req, &orderID)
	return orderID, err
}

func (a *API) CancelOrder(orderID string) (err error) {
	url := fmt.Sprintf(apiCancelOrder, orderID)
	req, err := a.newHttpRequest(http.MethodPost, url, nil)
	if err != nil {
		return err
	}

	var canceledID string
	err = a.makeHttpRequest(req, &canceledID)
	if err != nil {
		return err
	}
	if orderID != canceledID {
		return fmt.Errorf("returned canceled order id \"%s\", expected order id \"%d\"", canceledID, orderID)
	}
	return nil
}

type APIOrder struct {
	ID               int64   `json:"id"`
	Symbol           string  `json:"symbol"`
	AccountID        int64   `json:"account-id"`
	Amount           float64 `json:"amount,string"`
	Price            float64 `json:"price,string"`
	CreatedAt        int64   `json:"created-at"`
	Type             string  `json:"type"`
	FieldAmount      float64 `json:"field-amount,string"`
	FieldCashAmount  float64 `json:"field-cash-amount,string"`
	FilledAmount     float64 `json:"filled-amount,string"`
	FilledCashAmount float64 `json:"filled-cash-amount,string"`
	FilledFees       float64 `json:"filled-fees,string"`
	FinishedAt       int64   `json:"finished-at"`
	UserID           int64   `json:"user-id"`
	Source           string  `json:"source"`
	State            string  `json:"state"`
	CanceledAt       int64   `json:"canceled-at"`
	Exchange         string  `json:"exchange"`
	Batch            string  `json:"batch"`
}

func (a *API) OpenOrders(accountID int64, symbol string) (orders []APIOrder, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiOpenOrders, nil)
	if err != nil {
		return orders, err
	}

	q := req.URL.Query()
	q.Set("account-id", strconv.FormatInt(accountID, 10))
	q.Set("symbol", symbol)
	req.URL.RawQuery = q.Encode()

	err = a.makeHttpRequest(req, &orders)
	return orders, err
}

type APIResponse struct {
	Status       string `json:"status"`
	ErrorCode    string `json:"err-code"`
	ErrorMessage string `json:"err-msg"`
	Data         json.RawMessage
}

func (a *API) makeHttpRequest(req *http.Request, responseObject interface{}) error {
	body, err := a.sendHttpRequest(req)
	if err != nil {
		return err
	}

	var res APIResponse
	err = json.Unmarshal(body, &res)
	if err != nil {
		return fmt.Errorf("makeHttpRequest failed to unmarshal body, err: %s", err)
	}

	if res.ErrorCode != "" {
		return fmt.Errorf("api returned error code %s, body: %s", res.ErrorCode, string(body))
	}

	if responseObject != nil {
		err = json.Unmarshal(res.Data, responseObject)
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

		q.Set("AccessKeyId", a.options.ApiKey)
		q.Set("SignatureMethod", "HmacSHA256")
		q.Set("SignatureVersion", "2")
		q.Set("Timestamp", time.Now().UTC().Format("2006-01-02T15:04:05"))

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

	res, err := a.httpClient.SendRequest(req)
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(res.Body)

	return body, err
}
