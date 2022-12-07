package binance

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

const (
	apiBase         = "https://api.binance.com"
	apiExchangeInfo = apiBase + "/api/v3/exchangeInfo"
	apiDepth        = apiBase + "/api/v3/depth"
	fapiBase        = "https://fapi.binance.com"
	fapiDepth       = fapiBase + "/api/v1/depth"
)

type API struct {
	options gateway.Options
	client  *utils.HttpClient
}

func NewAPI(options gateway.Options) *API {
	client := utils.NewHttpClient()
	client.UseProxies(options.Proxies)

	return &API{
		options: options,
		client:  client,
	}
}

type APIResponse struct {
	Code    int64  `json:"code"`
	Message string `json:"message"`
}

type APIExchangeInfo struct {
	ServerTime int64       `json:"serverTime"`
	Symbols    []APISymbol `json:"symbols"`
}

type APISymbol struct {
	Symbol              string      `json:"symbol"`
	Status              string      `json:"status"`
	BaseAsset           string      `json:"baseAsset"`
	BaseAssetPrecision  int64       `json:"baseAssetPrecision"`
	QuoteAsset          string      `json:"quoteAsset"`
	QuoteAssetPrecision int64       `json:"quoteAssetPrecision"`
	QuotePrecision      int64       `json:"quotePrecision"`
	Filters             []APIFilter `json:"filters"`
}

type APIFilter struct {
	FilterType  string  `json:"filterType"`
	MaxPrice    float64 `json:"maxPrice,string"`
	MaxQty      float64 `json:"maxQty,string"`
	MinPrice    float64 `json:"minPrice,string"`
	MinQty      float64 `json:"minQty,string"`
	StepSize    float64 `json:"stepSize,string"`
	TickSize    float64 `json:"tickSize,string"`
	MinNotional float64 `json:"minNotional,string"`
}

func (a *API) ExchangeInfo() (APIExchangeInfo, error) {
	req, err := a.newAPIRequest(http.MethodGet, apiExchangeInfo, nil)
	if err != nil {
		return APIExchangeInfo{}, err
	}

	var res APIExchangeInfo
	err = a.makeHttpRequest(req, &res)
	return res, err
}

type APIPrice struct {
	Price  float64 `json:",string"`
	Amount float64 `json:",string"`
}

// Custom Unmarshal function to handle response data format
func (o *APIPrice) UnmarshalJSON(b []byte) error {
	var s [2]string
	err := json.Unmarshal(b, &s)
	if err != nil {
		return err
	}
	o.Price, err = strconv.ParseFloat(s[0], 64)
	if err != nil {
		return err
	}
	o.Amount, err = strconv.ParseFloat(s[1], 64)
	if err != nil {
		return err
	}
	return nil
}

type APIDepth struct {
	LastUpdateID int64      `json:"lastUpdateId"`
	Bids         []APIPrice `json:"bids"`
	Asks         []APIPrice `json:"asks"`
}

func (a *API) Depth(params url.Values) (APIDepth, error) {
	req, err := a.newAPIRequest(http.MethodGet, apiDepth, nil)
	if err != nil {
		return APIDepth{}, err
	}

	if len(params) > 0 {
		req.URL.RawQuery = params.Encode()
	}

	var res APIDepth
	err = a.makeHttpRequest(req, &res)
	return res, err
}

func (a *API) FDepth(params url.Values) (APIDepth, error) {
	req, err := a.newAPIRequest(http.MethodGet, fapiDepth, nil)
	if err != nil {
		return APIDepth{}, err
	}

	if len(params) > 0 {
		req.URL.RawQuery = params.Encode()
	}

	var res APIDepth
	err = a.makeHttpRequest(req, &res)
	return res, err
}

func (a *API) newAPIRequest(method, url string, data io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, url, data)
	if err != nil {
		return nil, err
	}

	return req, nil
}

func (a *API) makeHttpRequest(req *http.Request, responseObject interface{}) error {
	body, err := a.sendHttpRequest(req)
	if err != nil {
		return err
	}

	var res APIResponse
	err = json.Unmarshal(body, &res)
	if err != nil {
		return fmt.Errorf("json unmarshal err: %s", err)
	}

	if res.Code != 0 {
		return fmt.Errorf("api responded with error code: %d\nresponse body: %s", res.Code, string(body))
	}

	if responseObject != nil {
		err = json.Unmarshal(body, responseObject)
		if err != nil {
			return fmt.Errorf("failed to unmarshal json, res.Data, body: %s, unmarshal err: %s", string(body), err)
		}
	}

	return nil
}

func (a *API) sendHttpRequest(req *http.Request) ([]byte, error) {
	res, err := a.client.SendRequest(req)
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	return body, nil
}
