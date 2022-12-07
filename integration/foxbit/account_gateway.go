package foxbit

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

var insuficientBalanceMatch = regexp.MustCompile(`Insufficient funds`)

type AccountGateway struct {
	gateway.BaseAccountGateway
	ws         *WsSession
	tickCh     chan gateway.Tick
	api        *API
	openOrders map[string]gateway.Order
}

func NewAccountGateway(ws *WsSession, tickCh chan gateway.Tick, api *API) *AccountGateway {
	return &AccountGateway{
		ws:         ws,
		tickCh:     tickCh,
		api:        api,
		openOrders: make(map[string]gateway.Order),
	}
}

func (g *AccountGateway) Connect() error {
	g.handleAuth()

	// We handle authentication response in the WsSession (ws.go).
	// If the authentication doesn't happen after a few seconds, we return
	// an error because probably something went wrong and the subsequente
	// subscriptions will fail if we're not authenticated.
	tryNumber := 0
	for {
		if g.ws.authenticated == true {
			break
		}

		tryNumber++
		if tryNumber >= 3 {
			return fmt.Errorf("%s AccountGateway authentication timeout.", Exchange.Name)
		}

		time.Sleep(time.Second * 2)
	}

	go g.tradesUpdateHandler()

	return nil
}

func (g *AccountGateway) tradesUpdateHandler() {
	for {
		for id, order := range g.openOrders {
			res, err := g.api.GetOrder(id)
			if err != nil {
				log.Printf("Failed to get order update for order [%s], err: %s", id, err)
				continue
			}
			order.State = apiStateToGateway(res.State)
			order.FilledAmount = res.QuantityExecuted
			order.AvgPrice = res.PriceAvg

			g.tickCh <- gateway.TickWithEvents(gateway.NewOrderUpdateEvent(order))

			// Check if order still open, if not, stop checking for updates
			if order.State != gateway.OrderOpen && order.State != gateway.OrderPartiallyFilled {
				delete(g.openOrders, id)
			}
		}

		time.Sleep(3 * time.Second)
	}
}

func apiStateToGateway(st string) gateway.OrderState {
	switch st {
	case "CANCELED":
		return gateway.OrderCancelled
	case "ACTIVE":
		return gateway.OrderOpen
	case "FILLED":
		return gateway.OrderFullyFilled
	case "PARTIALLY_FILLED":
		return gateway.OrderPartiallyFilled
	case "PARTIALLY_CANCELED":
		return gateway.OrderCancelled
	default:
		log.Printf("st %s", st)
		return gateway.OrderUnknown
	}
}

func (g *AccountGateway) OpenOrders(market gateway.Market) ([]gateway.Order, error) {
	query := url.Values{}
	query.Set("page_size", "100")
	query.Set(
		"market_symbol",
		fmt.Sprintf("%s%s", strings.ToLower(market.Pair.Base), strings.ToLower(market.Pair.Quote)),
	)
	query.Set("state", "ACTIVE")

	res, err := g.api.Orders(query)
	if err != nil {
		return []gateway.Order{}, err
	}

	orders := make([]gateway.Order, 0, len(res))
	for _, resOrder := range res {
		orders = append(orders, apiOrderToGateway(market, resOrder))
	}

	return orders, nil
}

func apiOrderToGateway(market gateway.Market, apiOrder APIOrder) gateway.Order {
	return gateway.Order{
		Market:           market,
		ID:               apiOrder.Sn,
		Side:             apiSideToGateway(apiOrder.Side),
		State:            apiStateToGateway(apiOrder.State),
		Amount:           apiOrder.Quantity,
		Price:            apiOrder.Price,
		AvgPrice:         apiOrder.PriceAvg,
		FilledAmount:     apiOrder.QuantityExecuted,
		FilledMoneyValue: apiOrder.QuantityExecuted * apiOrder.PriceAvg,
	}
}

func (g *AccountGateway) Balances() ([]gateway.Balance, error) {
	functionName := "GetAccountPositions"
	respCh := make(chan WsMessage)
	g.ws.SubscribeMessages(functionName, respCh, nil)

	request := WsMessage{
		Type:           0,
		SequenceNumber: 0,
		FunctionName:   functionName,
		Payload:        fmt.Sprintf(`{"OMSId":"%d", "AccountId":"%s"}`, OMSId, g.ws.options.UserID),
	}

	err := g.ws.SendRequest(request)
	if err != nil {
		return []gateway.Balance{}, err
	}

	msg := <-respCh
	close(respCh)
	g.ws.removeSubscriber(respCh, functionName)

	wsBalances := []WsBalance{}

	if err := json.Unmarshal([]byte(msg.Payload), &wsBalances); err != nil {
		log.Printf("Failed to unmarshal %s ws balances. Err: %s. Data: %s", Exchange.Name, err, msg.Payload)
		return []gateway.Balance{}, nil
	}

	balances := make([]gateway.Balance, len(wsBalances))
	for _, wsBalance := range wsBalances {
		balances = append(balances, g.parseToCommonBalance(wsBalance))
	}

	return balances, nil
}

func (g *AccountGateway) SendOrder(order gateway.Order) (orderId string, err error) {
	req := APIPlaceOrderReq{
		MarketSymbol: fmt.Sprintf(
			"%s%s",
			strings.ToLower(order.Market.Pair.Base),
			strings.ToLower(order.Market.Pair.Quote),
		),
		Price:    utils.FloatToStringWithTick(order.Price, order.Market.PriceTick),
		Quantity: utils.FloatToStringWithTick(order.Amount, order.Market.AmountTick),
		Side:     gatewaySideToAPI(order.Side),
		Type:     "LIMIT",
	}

	res, err := g.api.PlaceOrder(req)
	if err != nil {
		switch {
		case insuficientBalanceMatch.MatchString(err.Error()):
			return "", gateway.InsufficientBalanceErr
		default:
			return "", err
		}
	}

	if res.Sn == "" {
		return "", fmt.Errorf("exptected api res to contain order id (sn), returned nil")
	}

	// Register order
	order.ID = res.Sn
	g.openOrders[res.Sn] = order

	return res.Sn, err
}

func gatewaySideToAPI(side gateway.Side) string {
	if side == gateway.Bid {
		return "BUY"
	} else if side == gateway.Ask {
		return "SELL"
	}
	return ""
}

func apiSideToGateway(side string) gateway.Side {
	if side == "BUY" {
		return gateway.Bid
	} else if side == "SELL" {
		return gateway.Ask
	}
	return ""
}

func (g *AccountGateway) CancelOrder(order gateway.Order) error {
	return g.api.CancelOrder(order.ID)
}

func (g *AccountGateway) handleAuth() {
	if !g.ws.authenticated {
		authData := WsAuthenticateUser{
			Nonce:     fmt.Sprint(time.Now().UnixMilli()),
			UserId:    g.ws.options.UserID,
			ApiKey:    g.ws.options.ApiKey,
			ApiSecret: g.ws.options.ApiSecret,
		}
		authData.Sign()

		jsonAuthData, _ := json.Marshal(authData)

		request := WsMessage{
			Type:           0,
			SequenceNumber: 0,
			FunctionName:   "AuthenticateUser",
			Payload:        string(jsonAuthData),
		}

		g.ws.SendRequest(request)
	}
}

func (g *AccountGateway) parseToCommonBalance(balance WsBalance) gateway.Balance {
	return gateway.Balance{
		Asset:     balance.ProductSymbol,
		Total:     balance.Amount,
		Available: balance.Amount - balance.Hold,
	}
}
