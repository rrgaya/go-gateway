package digifinex

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
	"strings"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

const (
	apiBase         = "https://openapi.digifinex.com"
	apiMarkets      = apiBase + "/v3/markets"
	apiSpotAssets   = apiBase + "/v3/spot/assets"
	apiNewOrder     = apiBase + "/v3/%s/order/new"
	apiCancelOrder  = apiBase + "/v3/%s/order/cancel"
	apiCurrentOrder = apiBase + "/v3/%s/order/current"
)

type APIClient struct {
	options    gateway.Options
	httpClient *utils.HttpClient
}

func NewAPIClient(options gateway.Options) APIClient {
	client := utils.NewHttpClient()
	client.UseProxies(options.Proxies)

	return APIClient{
		options:    options,
		httpClient: client,
	}
}

type APIResponse struct {
	Code int64 `json:"code"`
	Date int64 `json:"date"`
}

type APIMarket struct {
	Market          string  `json:"market"`
	MinAmount       float64 `json:"min_amount"`
	MinVolume       float64 `json:"min_volume"`
	PricePrecision  int64   `json:"price_precision"`
	VolumePrecision int64   `json:"volume_precision"`
}

type APIMarketRes struct {
	Data []APIMarket
}

func (a *APIClient) GetMarkets() ([]APIMarket, error) {
	var res APIMarketRes
	req, err := a.newHttpRequest(http.MethodGet, apiMarkets, nil, false)
	if err != nil {
		return []APIMarket{}, err
	}

	err = a.makeHttpRequest(req, &res)

	return res.Data, err
}

type APIAsset struct {
	Currency string  `json:"currency"`
	Free     float64 `json:"free"`
	Total    float64 `json:"total"`
}

type APIAssetRes struct {
	List []APIAsset
}

func (a *APIClient) SpotAssets() ([]APIAsset, error) {
	req, err := a.newHttpRequest(http.MethodGet, apiSpotAssets, nil, true)
	if err != nil {
		return []APIAsset{}, err
	}

	var res APIAssetRes
	err = a.makeHttpRequest(req, &res)
	return res.List, err
}

type NewOrderRes struct {
	OrderID string `json:"order_id"`
}

func (a *APIClient) NewOrder(market string, params *url.Values) (string, error) {
	url := fmt.Sprintf(apiNewOrder, market)
	req, err := a.newHttpRequest(http.MethodPost, url, params, true)
	if err != nil {
		return "", err
	}

	var res NewOrderRes
	err = a.makeHttpRequest(req, &res)
	return res.OrderID, err
}

type CancelOrderRes struct {
	Success []string `json:"success"`
	Error   []string `json:"error"`
}

func (a *APIClient) CancelOrder(market string, orderID string) error {
	params := url.Values{}
	params.Set("order_id", orderID)

	cancelOrderURL := fmt.Sprintf(apiCancelOrder, market)
	req, err := a.newHttpRequest(http.MethodPost, cancelOrderURL, &params, true)
	if err != nil {
		return err
	}

	var res CancelOrderRes
	err = a.makeHttpRequest(req, &res)
	if err != nil {
		return fmt.Errorf("failed http request, err: %s", err)
	}

	for _, id := range res.Success {
		if id == orderID {
			return nil
		}
	}

	return fmt.Errorf("expected orderID [%s] to be on sucessfull cancel list, instead list was: success [%s] failed [%s]", orderID, res.Success, res.Error)
}

type APIOrder struct {
	Amount         float64 `json:"amount"`
	AvgPrice       float64 `json:"avg_price"`
	CashAmount     float64 `json:"cash_amount"`
	CreatedDate    int64   `json:"created_date"`
	ExecutedAmount float64 `json:"executed_amount"`
	FinishedDate   int64   `json:"finished_date"`
	Kind           string  `json:"kind"`
	OrderID        string  `json:"order_id"`
	Price          float64 `json:"price"`
	Status         int64   `json:"status"`
	Symbol         string  `json:"symbol"`
	Type           string  `json:"type"`
}

type APIOrdersRes struct {
	Data []APIOrder `json:"data"`
}

func (a *APIClient) CurrentOrders(market, symbol string) ([]APIOrder, error) {
	params := url.Values{}
	if symbol != "" {
		//params.Set("symbol", symbol)
	}

	url := fmt.Sprintf(apiCurrentOrder, market)
	req, err := a.newHttpRequest(http.MethodGet, url, &params, true)
	if err != nil {
		return []APIOrder{}, err
	}

	var res APIOrdersRes
	err = a.makeHttpRequest(req, &res)
	return res.Data, err
}

func (a *APIClient) makeHttpRequest(req *http.Request, responseObject interface{}) error {
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
		return fmt.Errorf("failed to unmarshal body into APIResponse, err: %s", err)
	}

	if apiRes.Code != 0 {
		return fmt.Errorf("request to api [%s] failed, returned response code [%d]", req.URL, apiRes.Code)
	}

	if responseObject != nil {
		err = json.Unmarshal(body, responseObject)
		if err != nil {
			return fmt.Errorf("failed to unmarshal body [%s] into responseObject, err: %s", string(body), err)
		}
	}

	return nil
}

func (a *APIClient) newHttpRequest(method string, url string, params *url.Values, private bool) (*http.Request, error) {
	var encodedParams string
	var data io.Reader
	if params != nil {
		encodedParams = params.Encode()
		data = strings.NewReader(encodedParams)
	}

	req, err := http.NewRequest(method, url, data)
	if err != nil {
		return nil, err
	}

	if private {
		// Sign request
		signData := ""
		if params != nil {
			signData += encodedParams
		}
		h := hmac.New(sha256.New, []byte(a.options.ApiSecret))
		h.Write([]byte(signData))
		signature := hex.EncodeToString(h.Sum(nil))

		req.Header.Set("ACCESS-KEY", a.options.ApiKey)
		req.Header.Add("ACCESS-TIMESTAMP", strconv.Itoa(int(time.Now().Unix())))
		req.Header.Set("ACCESS-SIGN", signature)
	}

	return req, nil
}
