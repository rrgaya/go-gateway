package whitebit

import (
	"encoding/json"
	"net/http"
	"net/url"
)

const (
	WhiteBitHost        = "https://whitebit.com"
	WhiteBiTickerRoute  = "/api/v1/public/tickers"
	WhiteBitPairXSNUSDT = "XSN_USDT"
	WhiteBitPairBTCUSDT = "BTC_USDT"
	WhiteBitPairETHUSDT = "ETH_USDT"
)

type WhitebitClient struct {
	ApiHost string
	Client  *http.Client
}

func NewWhitebitClient(apiHost string) *WhitebitClient {
	wc := &WhitebitClient{}
	wc.ApiHost = apiHost
	wc.Client = &http.Client{}
	return wc
}

func (w *WhitebitClient) GetTicker() (TickerResponse, error) {
	u, err := url.Parse(w.ApiHost + WhiteBiTickerRoute)
	if err != nil {
		return TickerResponse{}, err
	}
	var t TickerResponse
	err = w.getJson(u.String(), &t)
	return t, nil
}

func (w *WhitebitClient) getJson(url string, target interface{}) error {
	r, err := w.Client.Get(url)
	if err != nil {
		return err
	}
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(target)
}
