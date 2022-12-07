package pancakeswap

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
)

const (
	apiBase = "https://bsc.streamingfast.io/subgraphs/name/pancakeswap/exchange-v2"
)

type API struct {
	httpClient http.Client
}

func NewAPI() *API {
	return &API{
		httpClient: http.Client{},
	}
}

func (a *API) RunGraphQuery(queryStmt string, res interface{}) error {
	query := struct {
		Query string `json:"query"`
	}{queryStmt}

	data, err := json.Marshal(query)
	if err != nil {
		return fmt.Errorf("failed to marshal query, err: %s", err)
	}

	req, err := a.newHttpRequest(http.MethodPost, apiBase, bytes.NewReader(data))
	if err != nil {
		return err
	}

	return a.makeHttpRequest(req, &res)
}

func (a *API) newHttpRequest(method string, url string, data io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, url, data)
	if err != nil {
		return nil, err
	}

	req.Header.Set("origin", "https://pancakeswap.finance")
	req.Header.Set("referer", "https://pancakeswap.finance/")
	req.Header.Set("authority", "bsc.streamingfast.io")
	req.Header.Set("content-type", "content-type")
	req.Header.Set("user-agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36")

	return req, nil
}

type GraphRes struct {
	Data   json.RawMessage
	Errors []json.RawMessage
}

func (a *API) makeHttpRequest(req *http.Request, responseObject interface{}) error {
	body, err := a.sendHttpRequest(req)
	if err != nil {
		return err
	}

	var graphRes GraphRes
	err = json.Unmarshal(body, &graphRes)
	if err != nil {
		return fmt.Errorf("failed to unmarshal json, body: %s, unmarshal err: %s", body, err)
	}

	if len(graphRes.Errors) > 0 {
		return fmt.Errorf("api responded with error message: response body: %s", string(body))
	}

	if responseObject != nil {
		err = json.Unmarshal(graphRes.Data, responseObject)
		if err != nil {
			return fmt.Errorf("failed to unmarshal json, body: %s, unmarshal err: %s", body, err)
		}
	}

	return nil
}

func (a *API) sendHttpRequest(req *http.Request) ([]byte, error) {
	res, err := a.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	return body, nil
}

func buildQueryForMarkets(markets []string) string {
	var where string
	if len(markets) > 0 {
		var clauses string
		for i, market := range markets {
			clauses += "\"" + market + "\""
			if i < len(markets)-1 {
				clauses += ","
			}
		}

		where = "where: { pair_in: [" + clauses + "] }"
	}

	return `
query {
	pairHourDatas(first: 1 ` + where + ` orderBy: hourStartUnix orderDirection: desc) {
		id
		hourStartUnix
		pair {
			id
			name
			token0 { id name symbol decimals totalLiquidity }
			token1 { id name symbol decimals totalLiquidity }
			reserve0
			reserve1
			token0Price
			token1Price
			block
			timestamp
		}
	}
}
`
}

type TokenGraph struct {
	Decimals       int64   `json:"decimals,string"`
	ID             string  `json:"id"`
	Name           string  `json:"name"`
	Symbol         string  `json:"symbol"`
	TotalLiquidity float64 `json:"totalLiquidity,string"`
}

type PairGraph struct {
	Block       string     `json:"block"`
	ID          string     `json:"id"`
	Name        string     `json:"name"`
	Reserve0    float64    `json:"reserve0,string"`
	Reserve1    float64    `json:"reserve1,string"`
	Timestamp   string     `json:"timestamp"`
	Token0Price float64    `json:"token0Price,string"`
	Token1Price float64    `json:"token1Price,string"`
	Token0      TokenGraph `json:"token0"`
	Token1      TokenGraph `json:"token1"`
}

type PairHourGraph struct {
	ID            string `json:"id"`
	HourStartUnix int64  `json:"hourStartUnix"`
	Pair          PairGraph
}
