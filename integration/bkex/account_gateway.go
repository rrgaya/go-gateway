package bkex

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

type AccountGateway struct {
	gateway.BaseAccountGateway
	api     *API
	tickCh  chan gateway.Tick
	options gateway.Options
}

func NewAccountGateway(options gateway.Options, api *API, tickCh chan gateway.Tick) *AccountGateway {
	return &AccountGateway{
		options: options,
		api:     api,
		tickCh:  tickCh,
	}
}

func (g *AccountGateway) Connect() error {
	ws := utils.NewWsClient()

	err := ws.Connect("wss://api.bkex.com/socket.io/?EIO=3&transport=websocket")
	if err != nil {
		return fmt.Errorf("ws connect err: %s", err)
	}

	ch := make(chan []byte, 100)
	quitHandler := make(chan bool)
	finishedAuth := make(chan error)
	ws.SubscribeMessages(ch)

	apiKey := g.options.ApiKey
	ts := time.Now().UnixMilli()
	data := fmt.Sprintf("timestamp=%s", strconv.FormatInt(ts, 10))
	h := hmac.New(sha256.New, []byte(g.options.ApiSecret))
	h.Write([]byte(data))
	signature := hex.EncodeToString(h.Sum(nil))
	payload := fmt.Sprintf("42/account,[\"userLogin\",{\"signature\":\"%s\",\"accessKey\":\"%s\",\"timestamp\":%d}]", signature, apiKey, ts)

	go func() {
		for {
			select {
			case data := <-ch:
				if string(data) == "40" {
					ws.WriteMessage([]byte("40/account"))
				} else if string(data) == "40/account" {
					ws.WriteMessage([]byte(payload))
				} else if startsWith(string(data), "42/account,[\"userLogin\"") {
					if !strings.Contains(string(data), "success") {
						finishedAuth <- fmt.Errorf("auth failed, res: %s", string(data))
						return // quit
					}

					ws.WriteMessage([]byte("42/account,[\"subUserOrderDeal\"]"))
				} else if startsWith(string(data), "42/account,[\"subUserOrderDeal\"") {
					var err error
					if !strings.Contains(string(data), "success") {
						err = fmt.Errorf("sub user orders failed, res: %s", string(data))
					}

					finishedAuth <- err
					return // quit last step
				}
			case <-quitHandler:
				return
			}
		}
	}()

	select {
	case err := <-finishedAuth:
		if err != nil {
			return err
		}
	case <-time.After(5 * time.Second):
		quitHandler <- true
		return fmt.Errorf("timed out while authing to websocket...")
	}

	quitPinger := make(chan bool)

	go g.messageHandler(ch)
	go websocketPinger(ws, 20*time.Second, quitPinger)

	ws.OnDisconnect(quitPinger)

	return nil
}

type WsFill struct {
	DealVolume  float64 `json:"dealVolume,string"`
	Fee         float64 `json:"fee,string"`
	FeeCurrency string  `json:"feeCurrency"`
	ID          string  `json:"id"`
	OrderID     string  `json:"orderId"`
	OrderSide   string  `json:"orderSide"`
	Price       float64 `json:"price,string"`
	Symbol      string  `json:"symbol"`
	TradeTime   int64   `json:"tradeTime"`
}

const subOrderOrderDealStr = "42/account,[\"subUserOrderDeal\","

func (g *AccountGateway) messageHandler(ch chan []byte) {
	for fullData := range ch {
		if startsWith(string(fullData), subOrderOrderDealStr) {
			data := stripWsMsgToData(fullData, subOrderOrderDealStr)
			if err := g.processFill(data); err != nil {
				log.Printf("%s process fill err: %s", Exchange, err)
			}
		}
	}
}

func (g *AccountGateway) processFill(data []byte) error {
	var fill WsFill
	err := json.Unmarshal(data, &fill)
	if err != nil {
		return fmt.Errorf("unmarshal err: %s", err)
	}

	events := make([]gateway.Event, 0, 1)
	events = append(events, gateway.NewFillEvent(gateway.Fill{
		ID:        fill.ID,
		Symbol:    fill.Symbol,
		Timestamp: time.UnixMilli(fill.TradeTime),
		OrderID:   fill.OrderID,
		Price:     fill.Price,
		Amount:    fill.DealVolume,
		Fee:       fill.Fee,
		FeeAsset:  fill.FeeCurrency,
	}))

	g.tickCh <- gateway.TickWithEvents(events...)

	return nil
}

func (g *AccountGateway) Balances() ([]gateway.Balance, error) {
	res, err := g.api.Balance()
	if err != nil {
		return []gateway.Balance{}, err
	}

	bals, ok := res["WALLET"]
	if !ok {
		return []gateway.Balance{}, fmt.Errorf("Unable to locate \"WALLET\" key")
	}

	balances := make([]gateway.Balance, len(bals))
	for index, balance := range bals {
		balances[index] = gateway.Balance{
			Asset:     strings.ToUpper(balance.Currency),
			Available: balance.Available,
			Total:     balance.Total,
		}
	}

	return balances, nil
}

func (g *AccountGateway) OpenOrders(market gateway.Market) ([]gateway.Order, error) {
	params := make(map[string]string)
	params["symbol"] = market.Symbol

	res, err := g.api.OpenOrders(params)
	if err != nil {
		return []gateway.Order{}, err
	}

	orders := make([]gateway.Order, len(res))
	for index, resOrder := range res {
		orders[index] = gateway.Order{
			Market:       market,
			ID:           resOrder.ID,
			State:        mapOrderStatusToState(resOrder.Status),
			Amount:       resOrder.TotalVolume,
			Price:        resOrder.Price,
			FilledAmount: resOrder.DealVolume,
		}
	}

	return orders, nil
}

func mapOrderStatusToState(status int) gateway.OrderState {
	switch status {
	case 0:
		return gateway.OrderOpen
	case 1:
		return gateway.OrderFullyFilled
	case 2:
		return gateway.OrderCancelled
	case 3:
		return gateway.OrderPartiallyFilled
	default:
		return gateway.OrderUnknown
	}
}

var insuficientBalanceMatch = regexp.MustCompile(`balance`)

func (g *AccountGateway) SendOrder(order gateway.Order) (orderId string, err error) {
	params := make(map[string]string)

	params["symbol"] = order.Market.Symbol

	if order.Side == gateway.Bid {
		params["direction"] = "BID"
	} else {
		params["direction"] = "ASK"
	}

	if order.Type == gateway.MarketOrder {
		params["type"] = "MARKET"
	} else {
		params["type"] = "LIMIT"
	}

	params["volume"] = utils.FloatToStringWithTick(order.Amount, order.Market.AmountTick)
	params["price"] = utils.FloatToStringWithTick(order.Price, order.Market.PriceTick)

	res, err := g.api.CreateOrder(params)
	if err != nil {
		switch {
		case insuficientBalanceMatch.MatchString(err.Error()):
			return "", gateway.InsufficientBalanceErr
		default:
			return "", err
		}
	}

	return res, nil
}

func (g *AccountGateway) CancelOrder(order gateway.Order) error {
	res, err := g.api.CancelOrder(order.ID)
	if err != nil {
		return err
	}

	if res != order.ID {
		return fmt.Errorf("expected to have cancelled order id [%s] instead cancelled order id [%s]", order.ID, res)
	}

	return nil
}
