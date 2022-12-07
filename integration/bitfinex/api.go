package bitfinex

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

const (
	apiBase    = "https://api-pub.bitfinex.com"
	apiTickers = "/v2/tickers"
)

type API struct {
	options    gateway.Options
	httpClient *utils.HttpClient
}

// Doc: https://docs.bitfinex.com/v2/reference#rest-public-tickers
type Ticker struct {
	Symbol             string
	Bid                float64
	BidSize            float64
	Ask                float64
	AskSize            float64
	DailyChange        float64
	DailyChangePercent float64
	LastPrice          float64
	Volume             float64
	DailyHigh          float64
	DailyLow           float64
}

func (t *Ticker) UnmarshalJSON(b []byte) error {
	var data [11]interface{}
	if err := json.Unmarshal(b, &data); err != nil {
		return err
	}

	t.Symbol = data[0].(string)
	t.Bid = parseStringToFloat(data[1])
	t.BidSize = parseStringToFloat(data[2])
	t.Ask = parseStringToFloat(data[3])
	t.AskSize = parseStringToFloat(data[4])
	t.DailyChange = parseStringToFloat(data[5])
	t.DailyChangePercent = parseStringToFloat(data[6])
	t.LastPrice = parseStringToFloat(data[7])
	t.Volume = parseStringToFloat(data[8])
	t.DailyHigh = parseStringToFloat(data[9])
	t.DailyLow = parseStringToFloat(data[10])

	return nil
}

func parseStringToFloat(data interface{}) float64 {
	switch v := data.(type) {
	case string:
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0
		}
		return f
	case int64:
		return float64(v)
	case float64:
		return v
	}

	return 0
}

func NewAPI(options gateway.Options) *API {
	client := utils.NewHttpClient()
	client.UseProxies(options.Proxies)

	return &API{
		options:    options,
		httpClient: client,
	}
}

func (a *API) Tickers() ([]Ticker, error) {
	req, err := a.newHttpRequest(http.MethodGet, apiTickers, nil)
	if err != nil {
		return []Ticker{}, err
	}

	q := req.URL.Query()
	q.Set("symbols", "ALL")
	req.URL.RawQuery = q.Encode()

	var tickers []Ticker
	err = a.makeHttpRequest(req, &tickers)
	return tickers, err
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

	req.Header.Set("Content-Type", "application/json")

	return req, nil
}

func (a *API) makeHttpRequest(req *http.Request, responseObject interface{}) error {
	body, err := a.sendHttpRequest(req)
	if err != nil {
		return err
	}

	if responseObject != nil {
		err = json.Unmarshal(body, responseObject)
		if err != nil {
			return fmt.Errorf("failed to unmarshal json, body: %s, unmarshal err: %s", body, err)
		}
	}

	return nil
}

func (a *API) sendHttpRequest(req *http.Request) ([]byte, error) {
	res, err := a.httpClient.SendRequest(req)
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	return body, nil
}
