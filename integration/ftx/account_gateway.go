package ftx

import (
	"encoding/json"
	"log"
	"strconv"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
)

type AccountGateway struct {
	gateway.BaseAccountGateway
	api     *API
	ws      *WsSession
	tickCh  chan gateway.Tick
	options gateway.Options
}

func NewAccountGateway(api *API, tickCh chan gateway.Tick, options gateway.Options) *AccountGateway {
	return &AccountGateway{
		api:     api,
		tickCh:  tickCh,
		options: options,
	}
}

// Track order updates
func (g *AccountGateway) Connect() error {
	loginReq := WsRequest{
		Op: "login",
	}
	loginReq.Args.Key = g.options.ApiKey
	loginReq.Args.Time = time.Now().UnixMilli()
	loginReq.Args.Sign = g.ws.Sign()

	orderUpdatesreq := WsRequest{
		Channel: "orders",
		Op:      "subscribe",
	}

	err := g.ws.RequestSubscriptions([]WsRequest{loginReq, orderUpdatesreq})
	if err != nil {
		return err
	}

	ch := make(chan WsResponse)
	g.ws.SubscribeMessages(ch, nil)
	go func() {
		for msg := range ch {
			if msg.Channel != "orders" || msg.Type != "update" {
				continue
			}

			wsOrder := WsOrder{}

			if err := json.Unmarshal(msg.Data, &wsOrder); err != nil {
				log.Printf("Error trying to unmarshal FTX order update. Err: %s\n", err)
				continue
			}

			order := parseWsOrderToGatewayOrder(wsOrder)

			g.tickCh <- gateway.TickWithEvents(gateway.NewOrderUpdateEvent(order))
		}
	}()

	return nil
}

func (g *AccountGateway) Balances() ([]gateway.Balance, error) {
	balances, err := g.api.FetchBalances()

	if err != nil {
		return []gateway.Balance{}, err
	}

	gatewayBalances := make([]gateway.Balance, len(balances))
	for i, balance := range balances {
		gatewayBalances[i] = gateway.Balance{
			Asset:     balance.Coin,
			Total:     balance.Total,
			Available: balance.Free,
		}
	}

	return gatewayBalances, nil
}

func (g *AccountGateway) OpenOrders(market gateway.Market) ([]gateway.Order, error) {
	res, err := g.api.FetchOpenOrders(market.Symbol)
	if err != nil {
		return []gateway.Order{}, err
	}

	orders := make([]gateway.Order, len(res))

	for i, order := range res {
		orders[i] = gateway.Order{
			Market:       market,
			ID:           strconv.FormatInt(order.ID, 10),
			State:        normalizeOrderStatus(order.Status),
			Amount:       order.Size,
			Price:        order.Price,
			FilledAmount: order.FilledSize,
			Side:         normalizeOrderSize(order.Side),
		}
	}

	return orders, nil
}

func (g *AccountGateway) SendOrder(order gateway.Order) (orderId string, err error) {
	apiOrder := ApiOrder{
		Market: order.Market.Symbol,
		Side:   string(order.Side),
		Type:   "limit",
		Size:   order.Amount,
		Price:  order.Price,
	}

	res, err := g.api.CreateOrder(apiOrder)
	if err != nil {
		return "", err
	}

	return strconv.FormatInt(res.ID, 10), err
}

func (g *AccountGateway) ReplaceOrder(order gateway.Order) (updatedOrder gateway.Order, err error) {
	args := ModifyOrderReq{
		OrderID: order.ID,
		Price:   order.Price,
		Size:    order.Amount,
	}

	apiOrder, err := g.api.ModifyOrder(args)
	if err != nil {
		return updatedOrder, err
	}

	updatedOrder = parseApiOrderToGatewayOrder(apiOrder)

	return updatedOrder, nil
}

func (g *AccountGateway) CancelOrder(order gateway.Order) error {
	orderID, err := strconv.ParseInt(order.ID, 10, 64)
	if err != nil {
		return err
	}

	err = g.api.CancelOrder(orderID)
	if err != nil {
		return err
	}

	return nil
}

func normalizeOrderStatus(status string) gateway.OrderState {
	switch status {
	case "new":
		return gateway.OrderSent
	case "open":
		return gateway.OrderOpen
	case "closed":
		return gateway.OrderClosed
	default:
		return gateway.OrderUnknown
	}
}

func normalizeOrderSize(side string) gateway.Side {
	if side == "buy" {
		return gateway.Bid
	}

	return gateway.Ask
}

func parseWsOrderToGatewayOrder(order WsOrder) gateway.Order {
	return gateway.Order{
		Market: gateway.Market{
			Symbol: order.Market,
		},
		ID:           strconv.FormatInt(order.ID, 10),
		State:        normalizeOrderStatus(order.Status),
		Amount:       order.Size,
		Price:        order.Price,
		FilledAmount: order.FilledSize,
		Side:         normalizeOrderSize(order.Side),
	}
}

func parseApiOrderToGatewayOrder(order ApiOrder) gateway.Order {
	return gateway.Order{
		Market: gateway.Market{
			Symbol: order.Market,
		},
		ID:           strconv.FormatInt(order.ID, 10),
		State:        normalizeOrderStatus(order.Status),
		Amount:       order.Size,
		Price:        order.Price,
		FilledAmount: order.FilledSize,
		Side:         normalizeOrderSize(order.Side),
	}
}
