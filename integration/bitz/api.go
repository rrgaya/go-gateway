package bitz

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
)

const (
	apiBase      = "https://apiv2.bitz.com"
	apiHost      = "apiv2.bitz.com"
	apiSymbols   = "Market/symbolList"
	apiUserAgent = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36" // Request user-agent by the api's documentation
)

type API struct {
	apiKey     string
	apiSecret  string
	httpClient http.Client
}

func NewApi(apiKey, apiSecret string) *API {
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	return &API{
		apiKey:    apiKey,
		apiSecret: apiSecret,
		httpClient: http.Client{
			Transport: transport,
		},
	}
}

type ApiV2Res struct {
	Status int64           `json:"status"`
	Msg    string          `json:"msg"`
	Data   json.RawMessage `json:"data"`
}

type Symbol struct {
	ID          string  `json:"id"`
	Name        string  `json:"name"`
	CoinFrom    string  `json:"coinFrom"`
	CoinTo      string  `json:"coinTo"`
	MinTrade    float64 `json:"minTrade,string"`
	MaxTrade    float64 `json:"maxTrade,string"`
	NumberFloat int     `json:"numberFloat,string"`
	PriceFloat  int     `json:"priceFloat,string"`
}

func (a *API) GetSymbols() ([]Symbol, error) {
	req, err := a.newHttpRequest(http.MethodGet, apiSymbols, nil)
	if err != nil {
		return []Symbol{}, err
	}

	var data map[string]Symbol
	err = a.makeHttpRequest(req, &data)

	symbols := make([]Symbol, 0, len(data))
	for _, symbol := range data {
		symbols = append(symbols, symbol)
	}

	return symbols, err
}

func (a *API) newHttpRequest(method string, path string, data io.Reader) (*http.Request, error) {
	if string(path[0]) != "/" {
		path = "/" + path
	}

	url := apiBase + path
	req, err := http.NewRequest(method, url, data)
	if err != nil {
		return nil, err
	}

	req.Host = apiHost
	req.Header.Set("User-Agent", apiUserAgent)
	req.Header.Set("Content-Type", "application/json")

	return req, nil
}

func (a *API) makeHttpRequest(req *http.Request, responseObject interface{}) error {
	httpRes, body, err := a.sendHttpRequest(req)
	if err != nil {
		return err
	}

	if httpRes.StatusCode == 401 {
		return fmt.Errorf("unauthorized request, msg: %s", string(body))
	}

	var res ApiV2Res
	err = json.Unmarshal(body, &res)
	if err != nil {
		return fmt.Errorf("failed to unmarshal body data: %s, unmarshal err: %s", string(body), err)
	}

	if responseObject != nil {
		err = json.Unmarshal(res.Data, responseObject)
		if err != nil {
			return fmt.Errorf("failed to unmarshal res.Data: %s, unmarshal err: %s", res.Data, err)
		}
	}

	return nil
}

func (a *API) sendHttpRequest(req *http.Request) (*http.Response, []byte, error) {
	res, err := a.httpClient.Do(req)
	if err != nil {
		return res, nil, err
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return res, nil, err
	}

	return res, body, nil
}
