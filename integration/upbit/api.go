package upbit

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"regexp"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
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

type Market struct {
	BaseCurrencyCode          string `json:"baseCurrencyCode"`
	BaseCurrencyDecimalPlace  int64  `json:"baseCurrencyDecimalPlace"`
	Code                      string `json:"code"`
	EnglishName               string `json:"englishName"`
	Exchange                  string `json:"exchange"`
	IsTradingSuspended        bool   `json:"isTradingSuspended"`
	KoreanName                string `json:"koreanName"`
	ListingDate               string `json:"listingDate"`
	LocalName                 string `json:"localName"`
	MarketState               string `json:"marketState"`
	MarketStateForIOS         string `json:"marketStateForIOS"`
	Pair                      string `json:"pair"`
	QuoteCurrencyCode         string `json:"quoteCurrencyCode"`
	QuoteCurrencyDecimalPlace int64  `json:"quoteCurrencyDecimalPlace"`
	Timestamp                 int64  `json:"timestamp"`
	TradeStatus               string `json:"tradeStatus"`
}

func (a *API) Markets() ([]Market, error) {
	var markets []Market

	req, err := http.NewRequest(http.MethodGet, "https://s3.ap-northeast-2.amazonaws.com/crix-production/crix_master", nil)
	if err != nil {
		return markets, err
	}

	res, err := a.httpClient.SendRequest(req)
	if err != nil {
		return markets, err
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return markets, err
	}

	err = json.Unmarshal(body, &markets)
	if err != nil {
		return markets, fmt.Errorf("failed to unmarshal markets res, err: %s", err)
	}

	// We need to remove all markets which code does not contian "UPBIT"
	// Since this endpoint returns all markets managed by "CRIX", which also
	// includes POLONIEX markets. They seem to aggregate market data from multiple
	// exchanges, not only UPBIT, not sure what they are exactly.
	upbitMarkets := make([]Market, 0)
	upbitRE := regexp.MustCompile(`\.UPBIT\.`)
	for _, market := range markets {
		if upbitRE.MatchString(market.Code) {
			upbitMarkets = append(upbitMarkets, market)
		}
	}

	return upbitMarkets, nil
}

func (a *API) ActiveMarkets() ([]Market, error) {
	markets, err := a.Markets()
	if err != nil {
		return markets, err
	}

	activeMarkets := make([]Market, 0)
	for _, market := range markets {
		if market.MarketState == "ACTIVE" {
			activeMarkets = append(activeMarkets, market)
		}
	}

	return activeMarkets, nil
}
