package currencylayer

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/buger/jsonparser"
	"github.com/herenow/atomic-gtw/utils"
)

const (
	apiBase = "https://apilayer.net"
	apiLive = "/api/live"
)

type API struct {
	apiKey     string
	httpClient *utils.HttpClient
}

type QuoteValue string

func (qv *QuoteValue) UnmarshalJSON(b []byte) error {
	*qv = QuoteValue(b)
	return nil
}

type LiveResponse struct {
	Success   bool                  `json:"success"`
	Timestamp int64                 `json:"timestamp"`
	Source    string                `json:"source"` // Base currency
	Quotes    map[string]QuoteValue `json:"quotes"`
}

type Quote struct {
	Symbol    string
	Base      string
	Quote     string
	Rate      float64
	Precision int
}

func NewAPI(apiKey string) *API {
	client := utils.NewHttpClient()

	return &API{
		apiKey:     apiKey,
		httpClient: client,
	}
}

func (a *API) GetQuotes(source string) ([]Quote, error) {
	req, err := a.newHttpRequest(http.MethodGet, apiLive, nil)
	if err != nil {
		return []Quote{}, err
	}

	q := req.URL.Query()
	q.Add("source", source)
	req.URL.RawQuery = q.Encode()

	var res LiveResponse
	err = a.makeHttpRequest(req, &res)
	if err != nil {
		return []Quote{}, err
	}

	quotes := make([]Quote, 0, len(res.Quotes))
	for symbol, quoteValue := range res.Quotes {
		if len(symbol) < 6 {
			log.Printf("CurrencyLayer unable to extract currency base/quote from symbol %s, expected 6 letter symbol", symbol)
			continue
		}

		precision := 0
		parts := strings.Split(string(quoteValue), ".")
		if len(parts) >= 2 {
			precision = len(parts[1]) // parts[1] is the fraction part of this float
		}

		rate, err := strconv.ParseFloat(string(quoteValue), 64)
		if err != nil {
			log.Printf("CurrencyLayer unable to parse exchange rate %s (%s) into float", quoteValue, symbol)
			continue
		}

		quotes = append(quotes, Quote{
			Symbol:    symbol,
			Base:      symbol[0:3],
			Quote:     symbol[3:6],
			Rate:      rate,
			Precision: precision,
		})
	}

	return quotes, err
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

	q := req.URL.Query()
	q.Add("access_key", a.apiKey)
	req.URL.RawQuery = q.Encode()

	req.Header.Set("Content-Type", "application/json")

	return req, nil
}

func (a *API) makeHttpRequest(req *http.Request, responseObject interface{}) error {
	body, err := a.sendHttpRequest(req)
	if err != nil {
		return err
	}

	errMessage, _ := jsonparser.GetString(body, "error", "info")
	if errMessage != "" {
		return fmt.Errorf("api responded with error message: %s\nresponse body: %s", errMessage, string(body))
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
