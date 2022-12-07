package lbank

import (
	"crypto"
	"crypto/md5"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	mathrand "math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

const (
	apiBase          = "https://api.lbkex.com"
	apiTradingPairs  = apiBase + "/v2/accuracy.do"
	apiAssetInfo     = apiBase + "/v2/user_info.do"
	apiSubKey        = apiBase + "/v2/subscribe/get_key.do"
	apiPendingOrders = apiBase + "/v2/orders_info_no_deal.do"
	apiCreateOrder   = apiBase + "/v2/create_order.do"
	apiCancelOrder   = apiBase + "/v2/cancel_order.do"
)

type API struct {
	options    gateway.Options
	httpClient *utils.HttpClient
	privateKey *rsa.PrivateKey
}

func NewAPI(options gateway.Options) *API {
	client := utils.NewHttpClient()
	client.UseProxies(options.Proxies)

	return &API{
		options:    options,
		httpClient: client,
	}
}

func (a *API) LoadPrivateKey(apiSecret string) error {
	privateKey, err := ParsePKCS1PrivateKey(FormatPrivateKey(apiSecret))
	if err != nil {
		return err
	}

	a.privateKey = privateKey
	return nil
}

type APIResponse struct {
	ErrorCode int             `json:"error_code"`
	Data      json.RawMessage `json:"data"`
}

type APITradingPair struct {
	Symbol           string  `json:"symbol"`
	MinTranQua       float64 `json:"minTranQua,string"`
	PriceAccuracy    int64   `json:"priceAccuracy,string"`
	QuantityAccuracy int64   `json:"quantityAccuracy,string"`
}

func (a *API) TradingPairs() ([]APITradingPair, error) {
	req, err := a.newHttpRequest(http.MethodGet, apiTradingPairs)
	if err != nil {
		return []APITradingPair{}, err
	}

	var res []APITradingPair
	err = a.makeHttpRequest(req, &res, false)
	return res, err
}

type APIAssetInfos struct {
	Freeze map[string]json.Number `json:"freeze"`
	Free   map[string]json.Number `json:"free"`
	Asset  map[string]json.Number `json:"asset"` // Total
}

func (a *API) AssetInformation() (APIAssetInfos, error) {
	req, err := a.newHttpRequest(http.MethodPost, apiAssetInfo)
	if err != nil {
		return APIAssetInfos{}, err
	}

	var res APIAssetInfos
	err = a.makeHttpRequest(req, &res, true)
	return res, err
}

func (a *API) SubKey() (string, error) {
	req, err := a.newHttpRequest(http.MethodPost, apiSubKey)
	if err != nil {
		return "", err
	}

	var res string
	err = a.makeHttpRequest(req, &res, true)
	return res, err
}

type APIPendingOrders struct {
	PageLength  int64      `json:"page_length"`
	CurrentPage int64      `json:"current_page"`
	Total       int64      `json:"total"`
	Orders      []APIOrder `json:"orders,omitempty"`
}

type APIOrder struct {
	Symbol     string  `json:"symbol"`
	OrderID    string  `json:"order_id"`
	Status     int     `json:"status"`
	Type       string  `json:"type"`
	Price      float64 `json:"price"`
	Amount     float64 `json:"amount"`
	DealAmount float64 `json:"deal_amount"`
	AvgPrice   float64 `json:"avg_price"`
	Time       int64   `json:"time"`
}

func statusToOrderStateGateway(sts int) gateway.OrderState {
	switch sts {
	case -1:
		return gateway.OrderCancelled
	case 0:
		return gateway.OrderOpen
	case 1:
		return gateway.OrderPartiallyFilled
	case 2:
		return gateway.OrderFullyFilled
	case 3: // Partially filled and cancelled
		return gateway.OrderPartiallyFilled
	case 4: // Cancellation being processed
		return gateway.OrderCancelled
	}
	return ""
}

func typeToSideGateway(_type string) gateway.Side {
	if _type[0:3] == "buy" {
		return gateway.Bid
	} else if _type[0:4] == "sell" {
		return gateway.Ask
	}

	return ""
}

func (a *API) PendingOrders(params url.Values) (APIPendingOrders, error) {
	req, err := a.newHttpRequest(http.MethodPost, apiPendingOrders)
	if err != nil {
		return APIPendingOrders{}, err
	}

	// Set
	req.URL.RawQuery = params.Encode()

	var res APIPendingOrders
	err = a.makeHttpRequest(req, &res, true)
	return res, err
}

func (a *API) CreateOrder(params url.Values) (res APIOrder, err error) {
	req, err := a.newHttpRequest(http.MethodPost, apiCreateOrder)
	if err != nil {
		return res, err
	}

	// Set
	req.URL.RawQuery = params.Encode()

	err = a.makeHttpRequest(req, &res, true)
	return res, err
}

func (a *API) CancelOrder(params url.Values) (res APIOrder, err error) {
	req, err := a.newHttpRequest(http.MethodPost, apiCancelOrder)
	if err != nil {
		return res, err
	}

	// Set
	req.URL.RawQuery = params.Encode()

	err = a.makeHttpRequest(req, &res, true)
	return res, err
}

func (a *API) makeHttpRequest(req *http.Request, responseObject interface{}, signed bool) error {
	if signed {
		if a.privateKey == nil {
			return fmt.Errorf("missing privateKey --apiSecret")
		}

		timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)

		tmpStr, _ := uuid.NewRandom()
		echostr := tmpStr.String()
		echostr = strings.Replace(echostr, "-", strconv.Itoa(mathrand.Int()%10), -1)

		signParams := req.URL.Query()
		signParams.Set("api_key", a.options.ApiKey)
		signParams.Set("signature_method", "RSA")
		signParams.Set("timestamp", timestamp)
		signParams.Set("echostr", echostr)
		encodedParams := signParams.Encode()

		md5Hash := md5.Sum([]byte(encodedParams))
		md5Str := strings.ToUpper(hex.EncodeToString(md5Hash[:]))

		paramsSha256 := sha256.New()
		paramsSha256.Write([]byte(md5Str))
		sha256Hash := paramsSha256.Sum(nil)

		sigMsg, err := rsa.SignPKCS1v15(rand.Reader, a.privateKey, crypto.SHA256, sha256Hash)
		if err != nil {
			return fmt.Errorf("failed to calculate rsa signature from sha256Hash [%s] err [%s]", sha256Hash, err)
		}

		signature := base64.StdEncoding.EncodeToString(sigMsg)

		q := req.URL.Query()
		q.Set("api_key", a.options.ApiKey)
		q.Set("sign", signature)
		req.URL.RawQuery = q.Encode()

		req.Header.Set("signature_method", "RSA")
		req.Header.Set("timestamp", timestamp)
		req.Header.Set("echostr", echostr)
	}

	res, err := a.httpClient.SendRequest(req)
	if err != nil {
		return err
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}

	var apiRes APIResponse
	err = json.Unmarshal(body, &apiRes)
	if err != nil {
		return fmt.Errorf("failed to unmarshal body [%s] into APIResponse, err: %s", string(body), err)
	}

	if apiRes.ErrorCode != 0 {
		return fmt.Errorf("request to api [%s] failed, returned error code [%d], body [%s]", req.URL, apiRes.ErrorCode, string(body))
	}

	if responseObject != nil {
		err = json.Unmarshal(apiRes.Data, responseObject)
		if err != nil {
			return fmt.Errorf("failed to unmarshal body [%s] into responseObject, err: %s", string(body), err)
		}
	}

	return nil
}

func (a *API) newHttpRequest(method string, url string) (*http.Request, error) {
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	return req, nil
}
