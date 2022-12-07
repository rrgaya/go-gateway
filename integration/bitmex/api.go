package bitmex

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/buger/jsonparser"
	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

const (
	apiBase      = "https://www.bitmex.com"
	apiHost      = "www.bitmex.com"
	apiPositions = "/api/v1/position"
	apiOrder     = "/api/v1/order"
	apiMargin    = "/api/v1/user/margin?currency=XBt"
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

type Margin struct {
	Amount          int64  `json:"amount"`
	AvailableMargin int64  `json:"availableMargin"`
	Currency        string `json:"currency"`
	MarginBalance   int64  `json:"marginBalance"`
	RiskLimit       int64  `json:"riskLimit"`
	RiskValue       int64  `json:"riskValue"`
}

func (a *API) Margin() (margin Margin, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiMargin, nil)
	if err != nil {
		return margin, err
	}

	err = a.makeHttpRequest(req, &margin)
	return margin, err
}

type Position struct {
	Account              int64     `json:"account"`
	Underlying           string    `json:"underlying"`
	Symbol               string    `json:"symbol"`
	AvgCostPrice         float64   `json:"avgCostPrice"`
	AvgEntryPrice        float64   `json:"avgEntryPrice"`
	BankruptPrice        float64   `json:"bankruptPrice"`
	BreakEvenPrice       float64   `json:"breakEvenPrice"`
	Commission           float64   `json:"commission"`
	CrossMargin          bool      `json:"crossMargin"`
	Currency             string    `json:"currency"`
	CurrentComm          float64   `json:"currentComm"`
	CurrentCost          float64   `json:"currentCost"`
	CurrentQty           float64   `json:"currentQty"`
	CurrentTimestamp     time.Time `json:"currentTimestamp"`
	DeleveragePercentile float64   `json:"deleveragePercentile"`
	ForeignNotional      float64   `json:"foreignNotional"`
	HomeNotional         float64   `json:"homeNotional"`
	IsOpen               bool      `json:"isOpen"`
	Leverage             float64   `json:"leverage"`
	LiquidationPrice     float64   `json:"liquidationPrice"`
	MarkPrice            float64   `json:"markPrice"`
	MarkValue            float64   `json:"markValue"`
	QuoteCurrency        string    `json:"quoteCurrency"`
	RealisedCost         float64   `json:"realisedCost"`
	RealisedGrossPnl     float64   `json:"realisedGrossPnl"`
	RealisedPnl          float64   `json:"realisedPnl"`
	RealisedTax          float64   `json:"realisedTax"`
	RebalancedPnl        float64   `json:"rebalancedPnl"`
	RiskLimit            float64   `json:"riskLimit"`
	RiskValue            float64   `json:"riskValue"`
	Timestamp            time.Time `json:"timestamp"`
}

func (a *API) Positions() ([]Position, error) {
	var positions []Position

	req, err := a.newHttpRequest(http.MethodGet, apiPositions, nil)
	if err != nil {
		return positions, err
	}

	err = a.makeHttpRequest(req, &positions)
	return positions, err
}

type Order struct {
	Account               int64   `json:"account"`
	AvgPx                 float64 `json:"avgPx"`
	ClOrdID               string  `json:"clOrdID"`
	ClOrdLinkID           string  `json:"clOrdLinkID"`
	ContingencyType       string  `json:"contingencyType"`
	CumQty                float64 `json:"cumQty"`
	Currency              string  `json:"currency"`
	DisplayQty            float64 `json:"displayQty"`
	ExDestination         string  `json:"exDestination"`
	ExecInst              string  `json:"execInst"`
	LeavesQty             float64 `json:"leavesQty"`
	MultiLegReportingType string  `json:"multiLegReportingType"`
	OrdRejReason          string  `json:"ordRejReason"`
	OrdStatus             string  `json:"ordStatus"`
	OrdType               string  `json:"ordType"`
	OrderID               string  `json:"orderID"`
	OrderQty              float64 `json:"orderQty"`
	PegOffsetValue        float64 `json:"pegOffsetValue"`
	PegPriceType          string  `json:"pegPriceType"`
	Price                 float64 `json:"price"`
	SettlCurrency         string  `json:"settlCurrency"`
	Side                  string  `json:"side"`
	SimpleCumQty          float64 `json:"simpleCumQty"`
	SimpleLeavesQty       float64 `json:"simpleLeavesQty"`
	SimpleOrderQty        float64 `json:"simpleOrderQty"`
	StopPx                float64 `json:"stopPx"`
	Symbol                string  `json:"symbol"`
	Text                  string  `json:"text"`
	TimeInForce           string  `json:"timeInForce"`
	Timestamp             string  `json:"timestamp"`
	TransactTime          string  `json:"transactTime"`
	Triggered             string  `json:"triggered"`
	WorkingIndicator      bool    `json:"workingIndicator"`
}

func statusToCommon(status string) gateway.OrderState {
	switch status {
	case "New", "PendingCancel", "PendingNew":
		return gateway.OrderOpen
	case "PartiallyFilled":
		return gateway.OrderPartiallyFilled
	case "Filled":
		return gateway.OrderFullyFilled
	case "Canceled":
		return gateway.OrderCancelled
	case "DoneForDay", "Rejected", "Expired":
		return gateway.OrderClosed
	}

	return gateway.OrderUnknown
}

func typeToCommon(_type string) gateway.OrderType {
	switch _type {
	case "Limit":
		return gateway.LimitOrder
	case "Market":
		return gateway.MarketOrder
	}

	return ""
}

func sideToCommon(side string) gateway.Side {
	switch side {
	case "Sell":
		return gateway.Ask
	case "Buy":
		return gateway.Bid
	}

	return ""
}

func (o Order) ToCommon() gateway.Order {
	order := gateway.Order{
		ID:            o.OrderID,
		ClientOrderID: o.ClOrdID,
		State:         statusToCommon(o.OrdStatus),
		Type:          typeToCommon(o.OrdType),
		Side:          sideToCommon(o.Side),
		Price:         o.Price,
		Amount:        o.OrderQty,
		AvgPrice:      o.AvgPx,
		FilledAmount:  o.OrderQty - o.LeavesQty,
	}

	return order
}

func (a *API) GetOrders(query url.Values) (orders []Order, err error) {
	req, err := a.newHttpRequest(http.MethodGet, apiOrder, nil)
	if err != nil {
		return orders, err
	}

	req.URL.RawQuery = query.Encode()
	err = a.makeHttpRequest(req, &orders)
	return orders, err
}

func (a *API) PlaceOrder(symbol, side, ordType, execInst string, price, orderQty float64) (order Order, err error) {
	orderData := struct {
		Symbol   string  `json:"symbol"`
		Side     string  `json:"side"`
		OrdType  string  `json:"ordType"`
		ExecInst string  `json:"execInst"`
		Price    float64 `json:"price"`
		OrderQty float64 `json:"orderQty"`
	}{symbol, side, ordType, execInst, price, orderQty}
	data, err := json.Marshal(orderData)
	if err != nil {
		return order, err
	}

	req, err := a.newHttpRequest(http.MethodPost, apiOrder, bytes.NewReader(data))
	if err != nil {
		return order, err
	}

	err = a.makeHttpRequest(req, &order)
	return order, err
}

type AmmendOrderReq struct {
	OrderID     string  `json:"orderID,omitempty"`
	OrigClOrdID string  `json:"origClOrdID,omitempty"`
	ClOrdID     string  `json:"clOrdID,omitempty"` // Update clOrdID
	Price       float64 `json:"price,omitempty"`
	OrderQty    float64 `json:"orderQty,omitempty"`
	LeavesQty   float64 `json:"leavesQty,omitempty"`
}

func (a *API) AmmendOrder(req AmmendOrderReq) (order Order, err error) {
	data, err := json.Marshal(req)
	if err != nil {
		return order, err
	}

	httpReq, err := a.newHttpRequest(http.MethodPut, apiOrder, bytes.NewReader(data))
	if err != nil {
		return order, err
	}

	err = a.makeHttpRequest(httpReq, &order)
	return order, err
}

func (a *API) CancelOrders(orderIds, clOrdIds []string) (orders []Order, err error) {
	orderData := struct {
		OrderID []string `json:"orderID"`
		ClOrdID []string `json:"clOrdID"`
	}{orderIds, clOrdIds}
	data, err := json.Marshal(orderData)
	if err != nil {
		return orders, err
	}

	req, err := a.newHttpRequest(http.MethodDelete, apiOrder, bytes.NewReader(data))
	if err != nil {
		return orders, err
	}

	err = a.makeHttpRequest(req, &orders)
	return orders, err
}

func (a *API) makeHttpRequest(req *http.Request, responseObject interface{}) error {
	body, err := a.sendHttpRequest(req)
	if err != nil {
		return err
	}

	errVal, dataType, _, _ := jsonparser.Get(body, "error")
	if dataType != jsonparser.NotExist {
		return fmt.Errorf("api responded with error message: %s\nresponse body: %s", string(errVal), string(body))
	}

	if responseObject != nil {
		err = json.Unmarshal(body, responseObject)
		if err != nil {
			return fmt.Errorf("failed to unmarshal json, body: %s, unmarshal err: %s", body, err)
		}
	}

	return nil
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

	req.Host = apiHost
	req.Header.Set("Content-Type", "application/json;charset=UTF-8")

	return req, nil
}

func (a *API) sendHttpRequest(req *http.Request) ([]byte, error) {
	isAuthenticated := a.options.ApiKey != ""

	if isAuthenticated {
		expires := strconv.FormatInt(time.Now().Unix()+3, 10) // Request expires in 3 seconds

		req.Header.Set("api-expires", expires)
		req.Header.Set("api-key", a.options.ApiKey)

		// Sign request
		data := req.Method + req.URL.RequestURI() + expires
		if req.Body != nil {
			reqBody, _ := req.GetBody()
			body, err := ioutil.ReadAll(reqBody)
			if err != nil {
				return nil, fmt.Errorf("failed to read request body to generate request signature, err %s", err)
			}
			data += string(body)
		}
		h := hmac.New(sha256.New, []byte(a.options.ApiSecret))
		h.Write([]byte(data))
		signature := hex.EncodeToString(h.Sum(nil))
		req.Header.Set("api-signature", signature)
	}

	res, err := a.httpClient.SendRequest(req)
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(res.Body)

	return body, err
}
