package gemini

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/buger/jsonparser"
	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
	"github.com/shopspring/decimal"
)

const (
	baseApi          = "https://api.gemini.com/v1"
	symbolsApi       = baseApi + "/symbols"
	symbolDetailsApi = symbolsApi + "/details/:symbol"
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

func (a *API) GetSymbols() ([]string, error) {
	req, err := a.newHttpRequest(http.MethodGet, symbolsApi, nil)
	if err != nil {
		return nil, err
	}

	symbols := make([]string, 0)

	err = a.makeHttpRequest(req, &symbols)
	if err != nil {
		return nil, err
	}

	return symbols, nil
}

type ApiSymbolDetail struct {
	Symbol         string          `json:"symbol"`
	BaseCurrency   string          `json:"base_currency"`
	QuoteCurrency  string          `json:"quote_currency"`
	TickSize       decimal.Decimal `json:"tick_size"`
	QuoteIncrement decimal.Decimal `json:"quote_increment"`
	MinOrderSize   float64         `json:"min_order_size,string"`
	Status         string          `json:"status"`
}

func (a *API) GetSymbolDetails(symbol string) (*ApiSymbolDetail, error) {
	url := strings.Replace(symbolDetailsApi, ":symbol", symbol, 1)
	req, err := a.newHttpRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	symbolDetail := &ApiSymbolDetail{}

	err = a.makeHttpRequest(req, &symbolDetail)
	if err != nil {
		return nil, err
	}

	return symbolDetail, nil
}

func (a *API) newHttpRequest(method string, url string, data io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, url, data)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json;charset=UTF-8")

	return req, nil
}

func (a *API) makeHttpRequest(req *http.Request, responseObject interface{}) error {
	body, err := a.sendHttpRequest(req)
	if err != nil {
		return err
	}

	errVal, dataType, _, _ := jsonparser.Get(body, "error")
	if dataType != jsonparser.NotExist {
		return utils.NewHttpError(
			Exchange.Name,
			utils.HttpRequestError,
			req,
			string(errVal),
			"",
		).AsError()
	}

	if responseObject != nil {
		err = json.Unmarshal(body, responseObject)
		if err != nil {
			return utils.NewHttpError(
				Exchange.Name,
				utils.UnmarshalError,
				req,
				err.Error(),
				string(body),
			).AsError()
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

	return body, err
}
