package hotbit

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
)

const (
	apiBase                = "https://www.hotbit.mobi"
	apiHost                = "www.hotbit.mobi"
	apiMarketsEndpoint     = "/public/markets"
	apiCreateOrderEndpoint = "/v1/order/create"
	apiCancelOrderEndpoint = "/v1/order/cancel"
	apiSessionInfoEndpoint = "/v1/info?platform=web"
)

type ApiClient struct {
	options    gateway.Options
	httpClient *http.Client
}

func NewApiClient(options gateway.Options) ApiClient {
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	return ApiClient{
		options: options,
		httpClient: &http.Client{
			Transport: transport,
		},
	}
}

type PublicApiResponse struct {
	Code    int64           `json:"code"`
	Msg     string          `json:"msg"`
	Content json.RawMessage `json:"content"`
}

type UserSign struct {
	From string `json:"from"`
	Sign string `json:"sign"`
	Time int64  `json:"time"`
	UID  int64  `json:"uid"`
}

type ApiSessionInfo struct {
	LoggedIn    bool          `json:"logined"`
	Pkey        string        `json:"pkey"`
	Maintenance bool          `json:"maintenance"`
	Portfolios  []interface{} `json:"portfolios"`
	ApiSum      int64         `json:"apiSum"`
	LoginSum    int64         `json:"loginSum"`
	User        struct {
		Uid            int64     `json:"uid"`
		LoginName      string    `json:"loginName"`
		Email          string    `json:"email"`
		LastLoginTime  time.Time `json:"lastLoginTime"`
		LastLoginIp    string    `json:"lastLoginIp"`
		InviterId      int64     `json:"inviterId"`
		InviteCount    int64     `json:"inviteCount"`
		Telephone      string    `json:"telephone"`
		IsBindGoogle   bool      `json:"isBindGoogle"`
		IsBindTelphone bool      `json:"isBindTelphone"`
		Extra          struct {
			Show_act bool `json:"show_act"`
		} `json:"extra"`
	} `json:"user"`
	UserSign UserSign `json:"user_sign"`
}

// Make an http connection to keep connection open
func (a *ApiClient) SessionInfo() (apiSessionInfo ApiSessionInfo, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiSessionInfoEndpoint, nil)
	if err != nil {
		return apiSessionInfo, err
	}

	req.Header.Set("Referer", "https://www.hotbit.mobi/") // They check the request referer, we need this

	err = a.makeHttpPublicRequest(req, &apiSessionInfo)

	return apiSessionInfo, err
}

func (a *ApiClient) GetMarkets() ([]Market, error) {
	var markets []Market

	req, err := a.newHttpRequest(http.MethodGet, apiMarketsEndpoint, nil)
	if err != nil {
		return markets, err
	}

	err = a.makeHttpPublicRequest(req, &markets)

	return markets, err
}

func (a *ApiClient) CreateOrder(price string, quantity string, market string, side string, _type string) (int64, error) {
	var res struct {
		Id int64 `json:"id"`
	}

	data := url.Values{}
	data.Set("price", price)
	data.Set("quantity", quantity)
	data.Set("market", market)
	data.Set("side", side)
	data.Set("type", _type)

	req, err := a.newHttpRequest(http.MethodPost, apiCreateOrderEndpoint, strings.NewReader(data.Encode()))
	if err != nil {
		return 0, err
	}

	req.Header.Set("Referer", "https://www.hotbit.mobi/exchange") // They check the request referer, we need this
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	err = a.makeHttpPublicRequest(req, &res)

	return res.Id, err
}

func (a *ApiClient) CancelOrder(orderId int64, market string) error {
	data := url.Values{}
	data.Set("order_id", strconv.FormatInt(orderId, 10))
	data.Set("market", market)

	req, err := a.newHttpRequest(http.MethodPost, apiCancelOrderEndpoint, strings.NewReader(data.Encode()))
	if err != nil {
		return err
	}

	req.Header.Set("Referer", "https://www.hotbit.mobi/exchange") // They check the request referer, we need this
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	var res interface{}

	err = a.makeHttpPublicRequest(req, &res)

	return err
}

func (a *ApiClient) makeHttpPublicRequest(req *http.Request, responseObject interface{}) error {
	var res PublicApiResponse

	err := a.sendHttpRequest(req, &res)
	if err != nil {
		return err
	}

	if res.Code != 1100 {
		return errors.New(fmt.Sprintf("Hotbit http request to %s failed. Code: %d. Msg: %s.", req.URL.String(), res.Code, res.Msg))
	}

	err = json.Unmarshal(res.Content, responseObject)
	if err != nil {
		return err
	}

	return nil
}

func (a *ApiClient) newHttpRequest(method string, path string, data io.Reader) (*http.Request, error) {
	if string(path[0]) != "/" {
		path = "/" + path
	}

	url := apiBase + path

	req, err := http.NewRequest(method, url, data)
	if err != nil {
		return nil, err
	}

	req.Host = apiHost
	req.Header.Set("User-Agent", a.options.UserAgent)
	req.Header.Set("Cookie", a.options.Cookie)
	req.Header.Set("Content-Type", "application/json;charset=UTF-8")

	return req, nil
}

func (a *ApiClient) sendHttpRequest(req *http.Request, responseObject interface{}) error {
	res, err := a.httpClient.Do(req)
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
