package mercadobitcoin

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

const (
	tapiBase        = "https://www.mercadobitcoin.net/tapi/v3/"
	tapiBaseStaging = "https://wibix.apps.mercadolitecoin.com.br/tapi/v3/"
)

type TAPI struct {
	options       gateway.Options
	client        *utils.HttpClient
	lastNonceUsed int64
}

func NewTAPI(options gateway.Options) *TAPI {
	client := utils.NewHttpClient()
	client.UseProxies(options.Proxies)

	return &TAPI{
		options: options,
		client:  client,
	}
}

type TAPIResponse struct {
	Data                json.RawMessage `json:"response_data"`
	StatusCode          int64           `json:"status_code"`
	ErrorMessage        string          `json:"error_message"`
	ServerUnixTimestamp int64           `json:"server_unix_timestamp,string"`
}

type TAPIAccountInfo struct {
	Balance map[string]TAPIBalance `json:"balance"`
}

type TAPIBalance struct {
	Total            float64 `json:"total,string"`
	Available        float64 `json:"available,string"`
	AmountOpenOrders int64   `json:"amount_open_orders"`
}

func (a *TAPI) GetAccountInfo() (TAPIAccountInfo, error) {
	var res TAPIAccountInfo

	req, err := a.newTAPIRequest("get_account_info", nil)
	if err != nil {
		return res, err
	}

	err = a.makeHttpRequest(req, &res)
	return res, err
}

type TAPIOperation struct {
	ExecutedTimestamp string `json:"executed_timestamp"`
	FeeRate           string `json:"fee_rate"`
	OperationID       int64  `json:"operation_id"`
	Price             string `json:"price"`
	Quantity          string `json:"quantity"`
}

type TAPIOrder struct {
	OrderID          json.Number     `json:"order_id,Number"`
	OrderType        int64           `json:"order_type"`
	Quantity         float64         `json:"quantity,string"`
	Status           int64           `json:"status"`
	UpdatedTimestamp string          `json:"updated_timestamp"`
	CoinPair         string          `json:"coin_pair"`
	CreatedTimestamp int64           `json:"created_timestamp,string"`
	ExecutedPriceAvg float64         `json:"executed_price_avg,string"`
	ExecutedQuantity float64         `json:"executed_quantity,string"`
	Fee              float64         `json:"fee,string"`
	HasFills         bool            `json:"has_fills"`
	LimitPrice       float64         `json:"limit_price,string"`
	Operations       []TAPIOperation `json:"operations"`
}

type TAPIListOrders struct {
	Orders []TAPIOrder `json:"orders"`
}

func (a *TAPI) ListOrders(symbol string, extras map[string]string) ([]TAPIOrder, error) {
	params := url.Values{}
	params.Set("coin_pair", symbol)

	if extras != nil {
		for key, value := range extras {
			params.Set(key, value)
		}
	}

	var res TAPIListOrders

	req, err := a.newTAPIRequest("list_orders", params)
	if err != nil {
		return []TAPIOrder{}, err
	}

	err = a.makeHttpRequest(req, &res)

	return res.Orders, err
}

type TAPIPlaceOrder struct {
	Order TAPIOrder `json:"order"`
}

func (a *TAPI) PlaceOrder(symbol, side, quantity, price string, postOnly bool) (TAPIOrder, error) {
	var method string
	if side == "buy" {
		if postOnly {
			method = "place_postonly_buy_order"
		} else {
			method = "place_buy_order"
		}

	} else if side == "sell" {
		if postOnly {
			method = "place_postonly_sell_order"
		} else {
			method = "place_sell_order"
		}
	} else {
		return TAPIOrder{}, fmt.Errorf("side must be 'buy' or 'sell' cant be %s", side)
	}

	params := url.Values{}
	params.Set("coin_pair", symbol)
	params.Set("quantity", quantity)
	params.Set("limit_price", price)

	req, err := a.newTAPIRequest(method, params)
	if err != nil {
		return TAPIOrder{}, err
	}

	var res TAPIPlaceOrder
	err = a.makeHttpRequest(req, &res)
	return res.Order, err
}

type TAPICancelOrder struct {
	Order TAPIOrder `json:"order"`
}

func (a *TAPI) CancelOrder(symbol string, orderID string) error {
	params := url.Values{}
	params.Set("coin_pair", symbol)
	params.Set("order_id", orderID)

	req, err := a.newTAPIRequest("cancel_order", params)
	if err != nil {
		return err
	}

	var res TAPICancelOrder
	err = a.makeHttpRequest(req, &res)
	if err != nil {
		return err
	}

	if res.Order.Status != cancelledOrderStatus {
		return fmt.Errorf("Expected order status to be cancelled, instead status %d and order: %+v", res.Order.Status, res.Order)
	}

	return nil
}

type TAPIOrderbookOrder struct {
	IsOwner    bool        `json:"is_owner"`
	LimitPrice float64     `json:"limit_price,string"`
	OrderID    json.Number `json:"order_id,Number"`
	Quantity   float64     `json:"quantity,string"`
}

type TAPIOrderbook struct {
	Bids []TAPIOrderbookOrder `json:"bids"`
	Asks []TAPIOrderbookOrder `json:"asks"`
}

type TAPIListOrderbook struct {
	Orderbook TAPIOrderbook `json:"orderbook"`
}

func (a *TAPI) ListOrderbook(symbol string, full bool) (TAPIOrderbook, error) {
	params := url.Values{}
	params.Set("coin_pair", symbol)

	if full {
		params.Set("full", "true")
	} else {
		params.Set("full", "false")
	}

	var res TAPIListOrderbook

	req, err := a.newTAPIRequest("list_orderbook", params)
	if err != nil {
		return res.Orderbook, err
	}

	err = a.makeHttpRequest(req, &res)
	return res.Orderbook, err
}

func (a *TAPI) newTAPIRequest(method string, params url.Values) (*http.Request, error) {
	var uri string
	if a.options.Staging {
		uri = tapiBaseStaging
	} else {
		uri = tapiBase
	}

	if params == nil {
		params = url.Values{}
	}

	nonce := time.Now().UnixNano() / int64(time.Millisecond) / 100
	if nonce <= a.lastNonceUsed {
		nonce = a.lastNonceUsed + 1
	}

	a.lastNonceUsed = nonce

	params.Set("tapi_method", method)
	params.Set("tapi_nonce", strconv.FormatInt(nonce, 10))

	req, err := http.NewRequest(http.MethodPost, uri, bytes.NewBufferString(params.Encode()))
	if err != nil {
		return nil, err
	}

	// Sign request
	secret := a.options.ApiSecret
	payload := fmt.Sprintf("/tapi/v3/?%s", params.Encode())
	hmac := hmac.New(sha512.New, []byte(secret))
	hmac.Write([]byte(payload))
	signature := hex.EncodeToString(hmac.Sum(nil))

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("TAPI-ID", a.options.ApiKey)
	req.Header.Set("TAPI-MAC", signature)

	return req, nil
}

func (a *TAPI) makeHttpRequest(req *http.Request, responseObject interface{}) error {
	body, err := a.sendHttpRequest(req)
	if err != nil {
		return err
	}

	var res TAPIResponse
	err = json.Unmarshal(body, &res)

	if res.ErrorMessage != "" {
		return fmt.Errorf("api responded with error message: %s\nresponse body: %s", string(res.ErrorMessage), string(body))
	}

	if responseObject != nil {
		err = json.Unmarshal(res.Data, responseObject)
		if err != nil {
			return fmt.Errorf("failed to unmarshal json, body: %s, unmarshal err: %s", string(res.Data), err)
		}
	}

	return nil
}

func (a *TAPI) sendHttpRequest(req *http.Request) ([]byte, error) {
	res, err := a.client.SendRequest(req)

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	if res.StatusCode != 200 {
		return body, fmt.Errorf("expected 200 http status code got [%d], res body [%s]", res.StatusCode, string(body))
	}

	if res.StatusCode == 401 {
		return body, fmt.Errorf("Unauthorized request, msg: %s", string(body))
	}

	return body, nil
}
