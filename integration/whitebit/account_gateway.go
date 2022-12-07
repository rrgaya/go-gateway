package whitebit

import (
	"encoding/json"
	"fmt"
	"log"
	"regexp"
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

func (g *AccountGateway) Connect(wsTradingURL string) error {
	ws := utils.NewWsClient()
	err := ws.Connect(wsTradingURL)
	if err != nil {
		return fmt.Errorf("stream ws conn err: %s", err)
	}

	log.Printf("Connected to stream ws [%s]...", wsTradingURL)

	// Sub to order updates
	authResID := int64(1000)
	spotSubResID := int64(1001)
	authMsg := fmt.Sprintf(
		"{\"method\": \"login\", \"params\": { \"type\": \"BASIC\", \"api_key\": \"%s\", \"secret_key\": \"%s\"}, \"id\": %d}",
		g.options.ApiKey,
		g.options.ApiSecret,
		authResID,
	)
	if err := ws.WriteMessage([]byte(authMsg)); err != nil {
		return fmt.Errorf("failed write auth msg to ws: %s", err)
	}

	wsCh := make(chan []byte, 100)
	ws.SubscribeMessages(wsCh)
	authResCh := make(chan error)
	spotSubResCh := make(chan error)
	quitSubsHandler := make(chan bool)
	go func() {
		for {
			select {
			case data := <-wsCh:
				var wsMsg WsTradingMessage
				err := json.Unmarshal(data, &wsMsg)
				if err != nil {
					log.Printf("Ws [%s] auth handler failed to unmarshal data\n%s\nErr:\n%s", wsTradingURL, string(data), err)
					continue
				}

				switch wsMsg.ID {
				case authResID:
					if !wsMsg.Result {
						authResCh <- fmt.Errorf("ws auth failed, res msg: %s", string(data))
					} else {
						authResCh <- nil
					}
				case spotSubResID:
					if !wsMsg.Result {
						spotSubResCh <- fmt.Errorf("ws spot sub failed, res msg: %s", string(data))
					} else {
						spotSubResCh <- nil
					}
				}
			case <-quitSubsHandler:
				return
			}
		}
	}()
	defer func() {
		ws.RemoveSubscriber(wsCh)
		quitSubsHandler <- true
	}()

	select {
	case err := <-authResCh:
		if err != nil {
			return err
		}
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timed out waiting for ws auth res...")
	}

	// Order updates processor
	// Need to start it up before sending the sub message
	// since a snapshot will be received when the sub is validated
	ch := make(chan []byte, 100)
	ws.SubscribeMessages(ch)
	go g.messageHandler(ch)

	// Sub to order updates
	spotSubMsg := fmt.Sprintf(
		"{\"method\": \"spot_subscribe\", \"id\": %d}",
		spotSubResID,
	)
	if err := ws.WriteMessage([]byte(spotSubMsg)); err != nil {
		return fmt.Errorf("failed write spot sub msg to ws: %s", err)
	}

	select {
	case err := <-spotSubResCh:
		if err != nil {
			return err
		}
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timed out waiting for ws spot sub res...")
	}

	return nil
}

type WsTradingMessage struct {
	Result bool            `json:"result"`
	Error  json.RawMessage `json:"error"`
	ID     int64           `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

type WsOrder struct {
	ClientOrderID      string  `json:"client_order_id"`
	CreatedAt          string  `json:"created_at"`
	ID                 int64   `json:"id"`
	PostOnly           bool    `json:"post_only"`
	Price              float64 `json:"price,string"`
	Quantity           float64 `json:"quantity,string"`
	QuantityCumulative float64 `json:"quantity_cumulative,string"`
	ReportType         string  `json:"report_type"`
	Side               string  `json:"side"`
	Status             string  `json:"status"`
	Symbol             string  `json:"symbol"`
	TimeInForce        string  `json:"time_in_force"`
	Type               string  `json:"type"`
	UpdatedAt          string  `json:"updated_at"`
}

func (g *AccountGateway) messageHandler(ch chan []byte) {
	for data := range ch {
		var wsMsg WsTradingMessage
		err := json.Unmarshal(data, &wsMsg)
		if err != nil {
			log.Printf("Ws msg handler failed to unmarshal data\n%s\nErr:\n%s", string(data), err)
			continue
		}

		if string(wsMsg.Error) != "" {
			log.Printf("Ws msg handler err msg: %s", string(data))
			continue
		}

		if wsMsg.Method == "spot_order" {
			var orderUpdate WsOrder
			err := json.Unmarshal(wsMsg.Params, &orderUpdate)
			if err != nil {
				log.Printf("Ws messageHandler failed to unmarshal spot_order update params, err: %s, data: %s", err, string(wsMsg.Params))
				continue
			}
			g.processOrderUpdate([]WsOrder{orderUpdate})
		} else if wsMsg.Method == "spot_orders" {
			var orderUpdates []WsOrder
			err := json.Unmarshal(wsMsg.Params, &orderUpdates)
			if err != nil {
				log.Printf("Ws messageHandler failed to unmarshal spot_orders update params, err: %s, data: %s", err, string(wsMsg.Params))
				continue
			}
			if len(orderUpdates) > 0 {
				g.processOrderUpdate(orderUpdates)
			}
		}
	}
}

func (g *AccountGateway) processOrderUpdate(orders []WsOrder) {
	events := make([]gateway.Event, 0, len(orders))

	for _, order := range orders {
		events = append(events, gateway.NewOrderUpdateEvent(gateway.Order{
			ID:           order.ClientOrderID,
			Market:       gateway.Market{Symbol: order.Symbol},
			Price:        order.Price,
			State:        mapOrderStatusToState(order.Status),
			Amount:       order.Quantity,
			FilledAmount: order.QuantityCumulative,
		}))
	}

	g.tickCh <- gateway.TickWithEvents(events...)
}

func (g *AccountGateway) Balances() ([]gateway.Balance, error) {
	res, err := g.api.SpotBalance()
	if err != nil {
		return []gateway.Balance{}, err
	}

	balances := make([]gateway.Balance, len(res))
	for index, balance := range res {
		balances[index] = gateway.Balance{
			Asset:     strings.ToUpper(balance.Currency),
			Available: balance.Available,
			Total:     balance.Reserved + balance.Available + balance.ReservedMargin,
		}
	}

	return balances, nil
}

func (g *AccountGateway) OpenOrders(market gateway.Market) ([]gateway.Order, error) {
	params := make(map[string]string)
	params["symbol"] = market.Symbol

	res, err := g.api.ActiveOrders(params)
	if err != nil {
		return []gateway.Order{}, err
	}

	orders := make([]gateway.Order, len(res))

	for index, resOrder := range res {
		orders[index] = gateway.Order{
			Market:       market,
			ID:           resOrder.ClientOrderID,
			State:        mapOrderStatusToState(resOrder.Status),
			Amount:       resOrder.Quantity,
			Price:        resOrder.Price,
			FilledAmount: resOrder.QuantityCumulative,
		}
	}

	return orders, nil
}

func mapOrderStatusToState(status string) gateway.OrderState {
	switch status {
	case "new":
		return gateway.OrderOpen
	case "canceled":
		return gateway.OrderCancelled
	case "expired":
		return gateway.OrderCancelled
	case "partiallyFilled":
		return gateway.OrderPartiallyFilled
	case "filled":
		return gateway.OrderFullyFilled
	default:
		return gateway.OrderUnknown
	}
}

var insuficientBalanceMatch = regexp.MustCompile(`balance`)

func (g *AccountGateway) SendOrder(order gateway.Order) (orderId string, err error) {
	params := make(map[string]string)

	params["symbol"] = order.Market.Symbol

	if order.Side == gateway.Bid {
		params["side"] = "buy"
	} else {
		params["side"] = "sell"
	}

	if order.Type == gateway.MarketOrder {
		params["type"] = "market"
	} else {
		params["type"] = "limit"
	}

	if order.PostOnly {
		params["post_only"] = "true"
	}

	params["quantity"] = utils.FloatToStringWithTick(order.Amount, order.Market.AmountTick)
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

	return res.ClientOrderID, nil
}

func (g *AccountGateway) CancelOrder(order gateway.Order) error {
	return g.api.CancelOrder(order.ID)
}
