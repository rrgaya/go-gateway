package mercadobitcoin

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

const (
	apiBase             = "https://www.mercadobitcoin.net/api"
	apiOrderbook        = "/%s/orderbook"
	apiTrades           = "/%s/trades"
	apiTradesFromTime   = "/%s/trades/%d"
	apiTradesFromToTime = "/%s/trades/%d/%d"
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

type APIBookRow [2]json.Number

type APIOrderBook struct {
	Asks []APIBookRow `json:"asks"`
	Bids []APIBookRow `json:"bids"`
}

func (a *API) OrderBook(asset string) (APIOrderBook, error) {
	var res APIOrderBook

	url := fmt.Sprintf(apiBase+apiOrderbook, asset)
	req, err := a.newAPIRequest(http.MethodGet, url)
	if err != nil {
		return res, err
	}

	req.Header.Set("Accept", "application/vnd.api.v2+json;")

	err = a.makeHttpRequest(req, &res)
	return res, err
}

type APITrade struct {
	TID    int64       `json:"tid"`
	Date   int64       `json:"date"`
	Type   string      `json:"type"`
	Price  json.Number `json:"price"`
	Amount json.Number `json:"amount"`
}

func (a *API) Trades(asset string, sinceTID int64, fromTime time.Time) ([]APITrade, error) {
	var res []APITrade

	var url string
	if fromTime.IsZero() {
		url = fmt.Sprintf(apiBase+apiTrades, asset)
	} else {
		url = fmt.Sprintf(apiBase+apiTradesFromTime, asset, fromTime.Unix())
	}

	req, err := a.newAPIRequest(http.MethodGet, url)
	if err != nil {
		return res, err
	}

	if sinceTID != 0 {
		q := req.URL.Query()
		q.Set("since", strconv.FormatInt(sinceTID, 10))
		req.URL.RawQuery = q.Encode()
	}

	req.Header.Set("Accept", "application/vnd.api.v2+json;")

	err = a.makeHttpRequest(req, &res)
	return res, err
}

func (a *API) newAPIRequest(method, url string) (*http.Request, error) {
	req, err := http.NewRequest(method, url, nil)
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

	if responseObject != nil {
		err = json.Unmarshal(body, responseObject)
		if err != nil {
			return fmt.Errorf("failed to unmarshal json, body: %s, unmarshal err: %s", string(body), err)
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
