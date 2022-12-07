package mercadobitcoin

import (
	"fmt"
	"log"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

var insuficientBalanceMatch = regexp.MustCompile(`insuficiente para realizar`)
var processingCancelMatch = regexp.MustCompile(`Seu cancelamento está em processamento`)

type AccountGateway struct {
	gateway.BaseAccountGateway
	options         gateway.Options
	markets         []gateway.Market
	tapi            *TAPI
	apiv4           *APIV4
	openOrders      map[gateway.Market]map[int64]gateway.Order
	openOrdersMutex *sync.Mutex
	tickCh          chan gateway.Tick
	accountID       string
}

func NewAccountGateway(options gateway.Options, markets []gateway.Market, tapi *TAPI, tickCh chan gateway.Tick, apiV4 *APIV4, accountID string) *AccountGateway {
	return &AccountGateway{
		options:         options,
		markets:         markets,
		tapi:            tapi,
		openOrders:      make(map[gateway.Market]map[int64]gateway.Order),
		openOrdersMutex: &sync.Mutex{},
		tickCh:          tickCh,
		apiv4:           apiV4,
		accountID:       accountID,
	}
}

func (g *AccountGateway) Connect() error {
	go g.trackOpenOrders()

	return nil
}

func (g *AccountGateway) trackOpenOrders() {
	for {
		for market, _ := range g.openOrders {
			oldestOpenOrderID := g.oldestOpenOrderID(g.openOrders[market])
			if oldestOpenOrderID > 0 {
				if g.options.Verbose {
					log.Printf("Fetching order update from oldest open order id [%d] on market [%s]", oldestOpenOrderID, market)
				}

				params := url.Values{}
				params.Set("id_from", strconv.FormatInt(oldestOpenOrderID, 10))
				resOrders, err := g.apiv4.ListOrders(
					g.accountID,
					pairToAPIV4Symbol(market.Pair),
					&params,
				)
				if err != nil {
					log.Printf("Track open orders failed to fetch open orders from api v4, err: %s", err)
					continue
				}

				// Diff received open orders with currently tracked open orders
				// Look for changes and dispatch order updates
				g.openOrdersMutex.Lock()
				for _, resOrder := range resOrders {
					orderIDInt, err := resOrder.ID.Int64()
					if err != nil {
						log.Printf("Opened order tracker failed to coerce order id [%s] into int64", resOrder.ID)
						continue
					}

					trackedOrder, ok := g.openOrders[market][orderIDInt]
					if ok {
						order := mapV4OrderToCommon(market, resOrder)

						// Check if the currently tracked was updated
						if order.State != trackedOrder.State || order.FilledAmount > trackedOrder.FilledAmount {
							go g.dispatchOrderUpdate(order)
							g.openOrders[market][orderIDInt] = order
						}

						if order.State == gateway.OrderFullyFilled || order.State == gateway.OrderCancelled {
							delete(g.openOrders[market], orderIDInt)
							if g.options.Verbose {
								log.Printf("Order [%s] was in a final state [%s] removed it from open orders, remaining open orders [%d]", order, order.State, len(g.openOrders[market]))
							}
						}
					}

				}
				g.openOrdersMutex.Unlock()
			}
		}

		time.Sleep(1000 * time.Millisecond)
	}
}

func (g *AccountGateway) oldestOpenOrderID(orders map[int64]gateway.Order) int64 {
	var oldestID int64
	for id, _ := range orders {
		if oldestID == 0 || id < oldestID {
			oldestID = id
		}
	}

	return oldestID
}

func (g *AccountGateway) dispatchOrderUpdate(order gateway.Order) {
	g.tickCh <- gateway.TickWithEvents(gateway.NewOrderUpdateEvent(order))
}

func (g *AccountGateway) Balances() ([]gateway.Balance, error) {
	info, err := g.tapi.GetAccountInfo()
	if err != nil {
		return []gateway.Balance{}, err
	}

	balances := make([]gateway.Balance, 0)
	for info, balance := range info.Balance {
		balances = append(balances, gateway.Balance{
			Asset:     strings.ToUpper(info),
			Total:     balance.Total,
			Available: balance.Available,
		})
	}

	return balances, nil
}

func pairToAPIV4Symbol(pair gateway.Pair) string {
	return fmt.Sprintf("%s-%s", pair.Base, pair.Quote)
}
func APIV4SymbolToPair(symbol string) gateway.Pair {
	parts := strings.Split(symbol, "-")
	if len(parts) < 2 {
		log.Printf("APIV4SymbolToPair invalid symbol [%s] cant map to pair, expting '-' separator", symbol)
		return gateway.Pair{}
	}

	return gateway.Pair{Base: parts[0], Quote: parts[1]}
}

func (g *AccountGateway) OpenOrders(market gateway.Market) ([]gateway.Order, error) {
	params := url.Values{}
	params.Set("status", "working")

	res, err := g.apiv4.ListOrders(
		g.accountID,
		pairToAPIV4Symbol(market.Pair),
		&params,
	)
	if err != nil {
		return []gateway.Order{}, err
	}

	orders := make([]gateway.Order, 0)
	for _, resOrder := range res {
		orders = append(orders, mapV4OrderToCommon(market, resOrder))
	}

	return orders, nil
}

func (g *AccountGateway) SendOrder(order gateway.Order) (orderId string, err error) {
	var side string

	if order.Side == gateway.Bid {
		side = "buy"
	} else {
		side = "sell"
	}

	resOrder, err := g.tapi.PlaceOrder(
		order.Market.Symbol,
		side,
		utils.FloatToStringWithTick(order.Amount, order.Market.AmountTick),
		utils.FloatToStringWithTick(order.Price, order.Market.PriceTick),
		order.PostOnly,
	)
	if err != nil {
		switch {
		case insuficientBalanceMatch.MatchString(err.Error()):
			return "", gateway.InsufficientBalanceErr
		default:
			return "", err
		}
	}

	// Register open order
	orderIDInt, err := resOrder.OrderID.Int64()
	if err != nil {
		log.Printf("Failed to convert order id [%s] into integer, can't process future order updates for this order...", resOrder.OrderID)
	} else {
		commonOrder := mapOrderToCommon(order.Market, resOrder)
		g.openOrdersMutex.Lock()
		_, ok := g.openOrders[order.Market]
		if !ok {
			g.openOrders[order.Market] = make(map[int64]gateway.Order)
		}
		g.openOrders[order.Market][orderIDInt] = commonOrder
		g.openOrdersMutex.Unlock()
	}

	return resOrder.OrderID.String(), nil
}

func (g *AccountGateway) CancelOrder(order gateway.Order) error {
	err := g.tapi.CancelOrder(order.Market.Symbol, order.ID)
	if err != nil {
		switch {
		case processingCancelMatch.MatchString(err.Error()):
			return nil
		}
	}
	return err
}

func mapV4OrderToCommon(market gateway.Market, order APIV4Order) gateway.Order {
	amount, _ := order.Qty.Float64()
	limitPrice, _ := order.LimitPrice.Float64()
	avgPrice, _ := order.AvgPrice.Float64()
	filledAmount, _ := order.FilledQty.Float64()

	return gateway.Order{
		Market:           market,
		ID:               order.ID.String(),
		Side:             mapV4SideToCommon(order.Side),
		State:            mapV4StatusToCommon(order.Status, filledAmount),
		Amount:           amount,
		Price:            limitPrice,
		AvgPrice:         avgPrice,
		FilledAmount:     filledAmount,
		FilledMoneyValue: filledAmount * avgPrice,
	}
}

func mapV4SideToCommon(side string) gateway.Side {
	if side == "buy" {
		return gateway.Bid
	} else {
		return gateway.Ask
	}
}

func mapV4StatusToCommon(status string, filledAmount float64) gateway.OrderState {
	switch status {
	case "pending":
		return gateway.OrderSent
	case "working":
		if filledAmount > 0 {
			return gateway.OrderPartiallyFilled
		} else {
			return gateway.OrderOpen
		}
	case "filled":
		return gateway.OrderFullyFilled
	case "cancelled":
		return gateway.OrderCancelled
	}
	return gateway.OrderUnknown
}

func mapOrderToCommon(market gateway.Market, order TAPIOrder) gateway.Order {
	return gateway.Order{
		Market:           market,
		ID:               order.OrderID.String(),
		Side:             mapOrderToCommonSide(order),
		State:            mapOrderToCommonState(order),
		Amount:           order.Quantity,
		Price:            order.LimitPrice,
		AvgPrice:         order.ExecutedPriceAvg,
		FilledAmount:     order.ExecutedQuantity,
		FilledMoneyValue: order.ExecutedQuantity * order.ExecutedPriceAvg,
	}
}

const (
	pendingOrderStatus   int64 = 1
	openOrderStatus      int64 = 2
	cancelledOrderStatus int64 = 3
	filledOrderStatus    int64 = 4
)

func mapOrderToCommonState(order TAPIOrder) gateway.OrderState {
	switch order.Status {
	case pendingOrderStatus:
		return gateway.OrderSent
	case openOrderStatus:
		if order.ExecutedQuantity > 0 {
			return gateway.OrderPartiallyFilled
		} else {
			return gateway.OrderOpen
		}
	case cancelledOrderStatus:
		return gateway.OrderCancelled
	case filledOrderStatus:
		return gateway.OrderFullyFilled
	default:
		log.Printf("MercadoBitcoin unkown order status %d unable to map order state, order: %+v", order.Status, order)
	}

	return ""
}

func mapOrderToCommonSide(order TAPIOrder) gateway.Side {
	if order.OrderType == 1 {
		return gateway.Bid
	} else {
		return gateway.Ask
	}
}
