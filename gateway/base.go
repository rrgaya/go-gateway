package gateway

import (
	"errors"
)

var NotImplementedErr = errors.New("not implemented")

var baseAccountGateway = &BaseAccountGateway{}
var baseTickCh = make(chan Tick)

type BaseGateway struct{}

func (g *BaseGateway) Connect() error {
	return NotImplementedErr
}

func (g *BaseGateway) Close() error {
	return NotImplementedErr
}

func (g *BaseGateway) Exchange() Exchange {
	return Exchange{Name: "BaseGateway"}
}

func (g *BaseGateway) Markets() []Market {
	return []Market{}
}

func (g *BaseGateway) SubscribeMarkets(markets []Market) error {
	return NotImplementedErr
}

func (g *BaseGateway) AccountGateway() AccountGateway {
	return baseAccountGateway
}

func (g *BaseGateway) Tick() chan Tick {
	return baseTickCh
}

type BaseAccountGateway struct{}

func (g *BaseAccountGateway) Balances() ([]Balance, error) {
	return []Balance{}, NotImplementedErr
}

func (g *BaseAccountGateway) OpenOrders(market Market) ([]Order, error) {
	return []Order{}, NotImplementedErr
}

func (g *BaseAccountGateway) SendOrder(order Order) (orderId string, err error) {
	return "", NotImplementedErr
}

func (g *BaseAccountGateway) ReplaceOrder(replaceOrder Order) (Order, error) {
	return Order{}, NotImplementedErr
}

func (g *BaseAccountGateway) CancelOrder(order Order) error {
	return NotImplementedErr
}
