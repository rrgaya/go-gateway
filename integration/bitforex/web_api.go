package bitforex

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

const (
	webapiBase           = "https://www.bitforex.com"
	webapiGetUserWsToken = "/server/cointrade.act?cmd=getUserWSToken"
	webapiGetOrders      = "/server/cointrade.act?cmd=getCompleteOrdersForEndTime&busitype=%s&tradetype=&orderstate=%s&page=1&size=1000"
	webapiGetUserInfo    = "/server/user_account.act?cmd=getUserBaseInfo"
)

type WebApiResponse struct {
	Data    json.RawMessage `json:"data"`
	Count   int64           `json:"count"`
	Success bool            `json:"success"`
	Time    int64           `json:"time"`
	Code    string          `json:"code"`    // Error code
	Message string          `json:"message"` // Error message
}

type WebApi struct {
	options    gateway.Options
	httpClient *utils.HttpClient
}

func NewWebApi(options gateway.Options) *WebApi {
	client := utils.NewHttpClient()
	client.UseProxies(options.Proxies)

	return &WebApi{
		options:    options,
		httpClient: client,
	}
}

type UserInfo struct {
	Email  string `json:"email"`
	Phone  string `json:"phone"`
	Type   int64  `json:"type"`
	UserId int64  `json:"userId"`
}

func (a *WebApi) GetUserInfo() (UserInfo, error) {
	req, err := a.newHttpRequest(http.MethodPost, webapiGetUserInfo, nil)
	if err != nil {
		return UserInfo{}, err
	}

	var userInfo UserInfo
	err = a.makeHttpRequest(req, &userInfo)
	return userInfo, err
}

type WebOrder struct {
	Symbol          string  `json:"busitype"`
	OrderId         string  `json:"orderid"`
	OrderState      int     `json:"orderState"`
	OrderPrice      float64 `json:"orderPrice,string"`
	OrderAmount     float64 `json:"orderAmount"`
	PriceAvg        float64 `json:"priceAvg,string"`
	FilledAmount    float64 `json:"dealAmount"`
	RemainingAmount float64 `json:"leftCoin"`
	Type            string  `json:"type"`
	CreateTime      int64   `json:"createTime"`
	EndTime         int64   `json:"endTime"`
	TradeFee        float64 `json:"tradeFee"`
	TradeType       int64   `json:"tradeType"`
}

var openOrderState = 0
var cancelledOrderState = 4
var fullyFilledOrderState = 2

var webapiGetUncompleteOrders = "5"
var webapiGetCompleteOrders = "6"

func (a *WebApi) GetOrders(symbol, state string) ([]WebOrder, error) {
	path := fmt.Sprintf(webapiGetOrders, symbol, state)
	req, err := a.newHttpRequest(http.MethodPost, path, nil)
	if err != nil {
		return []WebOrder{}, err
	}

	var orders []WebOrder
	err = a.makeHttpRequest(req, &orders)
	return orders, err
}

func (a *WebApi) GetWsToken() (string, error) {
	req, err := a.newHttpRequest(http.MethodGet, webapiGetUserWsToken, nil)
	if err != nil {
		return "", err
	}

	var token string
	err = a.makeHttpRequest(req, &token)
	return token, err
}

func (a *WebApi) makeHttpRequest(req *http.Request, responseObject interface{}) error {
	var res WebApiResponse

	err := a.sendHttpRequest(req, &res)
	if err != nil {
		return err
	}

	if res.Success != true {
		return errors.New(fmt.Sprintf("Bitforex web http request to %s failed. Code: %s. Msg: %s.", req.URL.String(), res.Code, res.Message))
	}

	err = json.Unmarshal(res.Data, responseObject)
	if err != nil {
		return err
	}

	return nil
}

func (a *WebApi) newHttpRequest(method string, path string, data io.Reader) (*http.Request, error) {
	if string(path[0]) != "/" {
		path = "/" + path
	}

	url := webapiBase + path
	req, err := http.NewRequest(method, url, data)
	if err != nil {
		return nil, err
	}

	req.Header.Set("cookie", a.options.Cookie)
	req.Header.Set("user-agent", a.options.UserAgent)
	req.Header.Set("origin", "https://www.bitforex.com")

	return req, nil
}

func (a *WebApi) sendHttpRequest(req *http.Request, responseObject interface{}) error {
	res, err := a.httpClient.SendRequest(req)
	if err != nil {
		return err
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}

	err = json.Unmarshal(body, responseObject)
	if err != nil {
		return err
	}

	return nil
}
