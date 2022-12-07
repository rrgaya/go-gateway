package bittrex

import (
	"bytes"
	"compress/flate"
	"crypto/hmac"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
	"github.com/shopspring/decimal"
	"github.com/thebotguys/signalr"
	"github.com/toorop/go-bittrex"
)

type AccountGateway struct {
	gateway.BaseAccountGateway
	api     *bittrex.Bittrex
	tickCh  chan gateway.Tick
	options gateway.Options
}

func NewAccountGateway(options gateway.Options, api *bittrex.Bittrex, tickCh chan gateway.Tick) *AccountGateway {
	return &AccountGateway{
		options: options,
		api:     api,
		tickCh:  tickCh,
	}
}

type WsRes struct {
	Success   bool   `json:"Success"`
	ErrorCode string `json:"ErrorCode"`
}

func _sendWsCmd(res interface{}, client *signalr.Client, hub string, method string, args ...interface{}) (err error) {
	data, err := client.CallHub(hub, method, args...)
	if err != nil {
		return fmt.Errorf("call hub err: %s", err)
	}

	err = json.Unmarshal(data, &res)
	if err != nil {
		return fmt.Errorf("json unmarshal err: %s, data: %s", err, string(data))
	}

	return nil
}

func sendWsCmd(client *signalr.Client, hub string, method string, args ...interface{}) (res WsRes, err error) {
	err = _sendWsCmd(&res, client, hub, method, args...)
	return res, err
}

func sendWsCmds(client *signalr.Client, hub string, method string, args ...interface{}) (res []WsRes, err error) {
	err = _sendWsCmd(&res, client, hub, method, args...)
	return res, err
}

func wsHeartbeatChecker(heartbeatRcvd chan bool, timeout time.Duration) {
	for {
		select {
		case <-heartbeatRcvd:
		case <-time.After(timeout):
			panic(fmt.Errorf("Bittrex timedout after %s waiting for ws heartbeat", timeout))
		}
	}
}

func wsDecodeMsg(arg json.RawMessage) (unzippedData json.RawMessage, err error) {
	var data string
	err = json.Unmarshal(arg, &data)
	if err != nil {
		return unzippedData, fmt.Errorf("failed unmarshal json arg [%s], err: %s", string(arg), err)
	}

	decodedData, err := base64.StdEncoding.DecodeString(string(data))
	if err != nil {
		return unzippedData, err
	}

	msgReader := bytes.NewReader(decodedData)
	reader := flate.NewReader(msgReader)
	unzippedData, err = io.ReadAll(reader)
	if err != nil {
		return unzippedData, err
	}

	return json.RawMessage(unzippedData), nil
}

func (g *AccountGateway) Connect() error {
	client := signalr.NewWebsocketClient()

	heartbeatRcvd := make(chan bool, 1)
	client.OnClientMethod = func(hub, method string, arguments []json.RawMessage) {
		switch method {
		case "execution":
			for _, arg := range arguments {
				data, err := wsDecodeMsg(arg)
				if err != nil {
					log.Printf("Bittrex account gtw failed decode exec msg [%s], err: %s", string(arg), err)
					continue
				}
				g.processExecution(data)
			}
		case "heartbeat":
			heartbeatRcvd <- true
		}
	}

	err := client.Connect("https", "socket-v3.bittrex.com", []string{"c3"})
	if err != nil {
		return fmt.Errorf("signalr connect err: %s", err)
	}

	log.Printf("Bittrex connected to signalr stream ws...")

	// Sub to order updates
	timestamp := time.Now().UnixMilli()
	timestampStr := strconv.FormatInt(timestamp, 10)
	randomContent := uuid.New().String()
	hmac := hmac.New(sha512.New, []byte(g.options.ApiSecret))
	hmac.Write([]byte(timestampStr + randomContent))
	signature := hex.EncodeToString(hmac.Sum(nil))
	res, err := sendWsCmd(client, "c3", "Authenticate", g.options.ApiKey, timestamp, randomContent, signature)
	if err != nil {
		return err
	}

	if !res.Success {
		return fmt.Errorf("ws auth failed, res: %+v", res)
	}

	log.Printf("Bittrex auth success")

	resList, err := sendWsCmds(client, "c3", "Subscribe", []string{"heartbeat", "execution", "order"})
	if err != nil {
		return err
	}

	if len(resList) < 2 {
		return fmt.Errorf("expected 2 res, instead: %+v", resList)
	}

	if !resList[0].Success {
		return fmt.Errorf("ws heartbeat sub failed, res: %+v", resList)
	}

	if !resList[1].Success {
		return fmt.Errorf("ws execution sub failed, res: %+v", resList)
	}

	log.Printf("Bittrex execution and heartbeat sub success")

	go wsHeartbeatChecker(heartbeatRcvd, 10*time.Second)

	return nil
}

type WsExecutionMsg struct {
	AccountID string        `json:"accountId"`
	Sequence  int64         `json:"sequence"`
	Deltas    []WsExecution `json:"deltas"`
}

type WsExecution struct {
	ID           string  `json:"id"`
	OrderID      string  `json:"orderId"`
	MarketSymbol string  `json:"marketSymbol"`
	Quantity     float64 `json:"quantity,string"`
	Commission   float64 `json:"commission,string"`
	Rate         float64 `json:"rate,string"`
	IsTaker      bool    `json:"isTaker"`
	ExecutedAt   string  `json:"executedAt"`
}

func (g *AccountGateway) processExecution(data json.RawMessage) {
	var msg WsExecutionMsg
	err := json.Unmarshal(data, &msg)
	if err != nil {
		log.Printf("Bittrex processExecution unmarshal data err: %s, data: %s", err, string(data))
		return
	}

	events := make([]gateway.Event, 0)
	for _, exec := range msg.Deltas {
		fillEvent := gateway.Fill{
			ID:       exec.ID,
			OrderID:  exec.OrderID,
			Symbol:   exec.MarketSymbol,
			Amount:   exec.Quantity,
			Price:    exec.Rate,
			Fee:      exec.Commission,
			FeeAsset: gateway.FeeInMoney,
		}

		events = append(events, gateway.Event{
			Type: gateway.FillEvent,
			Data: fillEvent,
		})
	}

	if len(events) > 0 {
		g.tickCh <- gateway.Tick{
			EventLog: events,
		}
	}
}

func (g *AccountGateway) Balances() ([]gateway.Balance, error) {
	res, err := g.api.GetBalances()
	if err != nil {
		return []gateway.Balance{}, err
	}

	balances := make([]gateway.Balance, len(res))
	for index, balance := range res {
		avail, _ := balance.Available.Float64()
		total, _ := balance.Total.Float64()

		balances[index] = gateway.Balance{
			Asset:     strings.ToUpper(balance.CurrencySymbol),
			Available: avail,
			Total:     total,
		}
	}

	return balances, nil
}

func (g *AccountGateway) OpenOrders(market gateway.Market) ([]gateway.Order, error) {
	res, err := g.api.GetOpenOrders(market.Symbol)
	if err != nil {
		return []gateway.Order{}, err
	}

	orders := make([]gateway.Order, len(res))
	for index, resOrder := range res {
		order := mapBittrexOrderV3ToGtw(resOrder)
		order.Market = market
		orders[index] = order

	}

	return orders, nil
}

func mapBittrexOrderV3ToGtw(order bittrex.OrderV3) gateway.Order {
	price, _ := order.Limit.Float64()
	quantity, _ := order.Quantity.Float64()
	fillQuantity, _ := order.FillQuantity.Float64()
	proceeds, _ := order.Proceeds.Float64()
	commission, _ := order.Commission.Float64()

	var filledMoneyValue, avgPrice float64
	filledMoneyValue = proceeds + commission
	avgPrice = filledMoneyValue / fillQuantity

	return gateway.Order{
		ID:               order.ID,
		State:            mapOrderStatusToState(order),
		Amount:           quantity,
		Price:            price,
		FilledAmount:     fillQuantity,
		FilledMoneyValue: filledMoneyValue,
		AvgPrice:         avgPrice,
	}
}

func mapOrderStatusToState(order bittrex.OrderV3) gateway.OrderState {
	switch order.Status {
	case "OPEN":
		if order.FillQuantity.IsPositive() {
			return gateway.OrderPartiallyFilled
		} else {
			return gateway.OrderOpen
		}
	case "CLOSED":
		if order.FillQuantity.GreaterThanOrEqual(order.Quantity) {
			return gateway.OrderFullyFilled
		} else {
			return gateway.OrderCancelled
		}
	default:
		return gateway.OrderUnknown
	}
}

var insuficientBalanceMatch = regexp.MustCompile(`balance`)

func (g *AccountGateway) SendOrder(order gateway.Order) (orderId string, err error) {
	params := bittrex.CreateOrderParams{
		MarketSymbol: order.Market.Symbol,
	}

	if order.Side == gateway.Bid {
		params.Direction = bittrex.BUY
	} else {
		params.Direction = bittrex.SELL
	}

	if order.Type == gateway.MarketOrder {
		params.Type = bittrex.MARKET
	} else {
		params.Type = bittrex.LIMIT

		px := utils.FloatToStringWithTick(order.Price, order.Market.PriceTick)
		price, err := strconv.ParseFloat(px, 64)
		if err != nil {
			return "", fmt.Errorf("failed parse order.Price [%s] to float", px)
		}
		params.Limit = price
	}

	params.TimeInForce = bittrex.GOOD_TIL_CANCELLED
	if order.PostOnly {
		params.TimeInForce = bittrex.POST_ONLY_GOOD_TIL_CANCELLED
	}

	qty, err := decimal.NewFromString(utils.FloatToStringWithTick(order.Amount, order.Market.AmountTick))
	if err != nil {
		return "", fmt.Errorf("qty decimal conv err: %s", err)
	}
	params.Quantity = qty

	res, err := g.api.CreateOrder(params)
	if err != nil {
		switch {
		case insuficientBalanceMatch.MatchString(err.Error()):
			return "", gateway.InsufficientBalanceErr
		default:
			return "", err
		}
	}

	return res.ID, nil
}

func (g *AccountGateway) CancelOrder(order gateway.Order) error {
	_, err := g.api.CancelOrder(order.ID)
	return err
}
