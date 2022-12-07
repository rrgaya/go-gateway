package bigone

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/golang-jwt/jwt"
	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

const (
	apiBase        = "https://big.one/api/v3"
	apiAssetPair   = apiBase + "/asset_pairs"
	apiAccounts    = apiBase + "/viewer/accounts"
	apiOrders      = apiBase + "/viewer/orders"
	apiCancelOrder = apiBase + "/viewer/orders/%s/cancel"
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
	Code    int64           `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

type APIAsset struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Symbol string `json:"symbol"`
}

type APIAssetPair struct {
	BaseScale     int64    `json:"base_scale"`
	ID            string   `json:"id"`
	MinQuoteValue float64  `json:"min_quote_value,string"`
	Name          string   `json:"name"`
	BaseAsset     APIAsset `json:"base_asset"`
	QuoteAsset    APIAsset `json:"quote_asset"`
	QuoteScale    int64    `json:"quote_scale"`
}

type APIOrder struct {
	Amount            float64 `json:"amount,string"`
	AssetPairName     string  `json:"asset_pair_name"`
	AvgDealPrice      float64 `json:"avg_deal_price,string"`
	ClientOrderID     string  `json:"client_order_id"`
	CreatedAt         string  `json:"created_at"`
	FilledAmount      float64 `json:"filled_amount,string"`
	ID                int64   `json:"id"`
	ImmediateOrCancel bool    `json:"immediate_or_cancel"`
	Operator          string  `json:"operator"`
	PostOnly          bool    `json:"post_only"`
	Price             float64 `json:"price,string"`
	Side              string  `json:"side"`
	State             string  `json:"state"`
	StopPrice         string  `json:"stop_price"`
	Type              string  `json:"type"`
	UpdatedAt         string  `json:"updated_at"`
}

func (a *APIClient) AssetPairs() ([]APIAssetPair, error) {
	req, err := a.newHttpRequest(http.MethodGet, apiAssetPair, nil, false)
	if err != nil {
		return []APIAssetPair{}, err
	}

	var res []APIAssetPair
	err = a.makeHttpRequest(req, &res)
	return res, err
}

type APIAccountBalance struct {
	AssetSymbol   string  `json:"asset_symbol"`
	Balance       float64 `json:"balance,string"`
	LockedBalance float64 `json:"locked_balance,string"`
}

func (a *APIClient) SpotAccounts() ([]APIAccountBalance, error) {
	req, err := a.newHttpRequest(http.MethodGet, apiAccounts, nil, true)
	if err != nil {
		return []APIAccountBalance{}, err
	}

	var res []APIAccountBalance
	err = a.makeHttpRequest(req, &res)
	return res, err
}

type APICreateOrder struct {
	Amount        string `json:"amount"`
	AssetPairName string `json:"asset_pair_name"`
	Operator      string `json:"operator"`
	Price         string `json:"price"`
	Side          string `json:"side"`
	Type          string `json:"type"`
	PostOnly      bool   `json:"post_only"`
}

func (a *APIClient) CreateOrder(params APICreateOrder) (res APIOrder, err error) {
	data, err := json.Marshal(params)
	if err != nil {
		return res, err
	}

	req, err := a.newHttpRequest(http.MethodPost, apiOrders, bytes.NewReader(data), true)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

type CancelOrderRes struct {
	Success []string `json:"success"`
	Error   []string `json:"error"`
}

func (a *APIClient) CancelOrder(orderID string) (res APIOrder, err error) {
	cancelOrderURL := fmt.Sprintf(apiCancelOrder, orderID)
	req, err := a.newHttpRequest(http.MethodPost, cancelOrderURL, nil, true)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

type APIListOrders struct {
	AssetPairName string `json:"asset_pair_name,omitempty"`
	Side          string `json:"side,omitempty"`
	State         string `json:"state,omitempty"`
	Limit         string `json:"limit,omitempty"`
}

func (a *APIClient) ListOrders(params url.Values) ([]APIOrder, error) {
	req, err := a.newHttpRequest(http.MethodGet, apiOrders, nil, true)
	if err != nil {
		return []APIOrder{}, err
	}

	if len(params) > 0 {
		req.URL.RawQuery = params.Encode()
	}

	var res []APIOrder
	err = a.makeHttpRequest(req, &res)
	return res, err
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
		return fmt.Errorf("request to api [%s] failed, returned response code [%d], msg [%s]", req.URL, apiRes.Code, string(body))
	}

	if responseObject != nil {
		err = json.Unmarshal(apiRes.Data, responseObject)
		if err != nil {
			return fmt.Errorf("failed to unmarshal body into responseObject, err: %s", err)
		}
	}

	return nil
}

func (a *APIClient) newHttpRequest(method string, url string, data io.Reader, private bool) (*http.Request, error) {
	req, err := http.NewRequest(method, url, data)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")

	if private {
		var signKey = []byte(a.options.ApiSecret)
		token := jwt.New(jwt.SigningMethodHS256)
		claims := token.Claims.(jwt.MapClaims)

		claims["type"] = "OpenAPIV2"
		claims["sub"] = a.options.ApiKey
		claims["nonce"] = strconv.FormatInt(time.Now().UnixNano(), 10)

		tokenStr, err := token.SignedString(signKey)
		if err != nil {
			return req, fmt.Errorf("jwt token gen err: %s", err)
		}

		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", tokenStr))
	}

	return req, nil
}
