package gateway

import (
	"fmt"
	"log"
	"strings"
)

func EventToSymbol(event Event) string {
	switch event.Type {
	case SnapshotSequenceEvent:
		return event.Data.(SnapshotSequence).Symbol
	case DepthEvent:
		return event.Data.(Depth).Symbol
	case TradeEvent:
		return event.Data.(Trade).Symbol
	case OkEvent:
		return event.Data.(Ok).Symbol
	case CancelEvent:
		return event.Data.(Cancel).Symbol
	case FillEvent:
		return event.Data.(Fill).Symbol
	case OrderUpdateEvent:
		return event.Data.(Order).Market.Symbol
	case BitmexInstrumentEvent:
		return event.Data.(BitmexInstrument).Symbol
	}
	return ""
}

func EventSymbolEqual(symbol string, event Event) bool {
	eventSymbol := EventToSymbol(event)
	return symbol == eventSymbol
}

func FilterEventsBySymbol(symbol string, events []Event) []Event {
	filtered := make([]Event, 0, len(events))
	for _, event := range events {
		if EventSymbolEqual(symbol, event) {
			filtered = append(filtered, event)
		}
	}

	return filtered
}

func EventsBySymbol(events []Event) map[string][]Event {
	data := make(map[string][]Event)

	for _, e := range events {
		symbol := EventToSymbol(e)
		_, ok := data[symbol]
		if !ok {
			data[symbol] = make([]Event, 0)
		}

		data[symbol] = append(data[symbol], e)
	}

	return data
}

func TradesFromEvents(events []Event) []Trade {
	trades := make([]Trade, 0)
	for _, e := range events {
		if e.Type == TradeEvent {
			trades = append(trades, e.Data.(Trade))
		}
	}
	return trades
}

func MarketBySymbol(markets []Market, symbol string) (Market, bool) {
	for _, market := range markets {
		if market.Symbol == symbol {
			return market, true
		}
	}

	return Market{}, false
}

func MarketsByBase(markets []Market, base string) []Market {
	found := make([]Market, 0)
	for _, market := range markets {
		if market.Pair.Base == base {
			found = append(found, market)
		}
	}

	return found
}

func ExcludeMarketFromMarkets(markets []Market, exclude Market) []Market {
	filtered := make([]Market, 0, len(markets))
	for _, mkt := range markets {
		if mkt != exclude {
			filtered = append(filtered, mkt)
		}
	}

	return filtered
}

func MarketsToQuoteAssets(markets []Market) []string {
	assets := make([]string, 0)
uniqueAssetsLoop:
	for _, m := range markets {
		for _, a := range assets {
			if a == m.Pair.Quote {
				continue uniqueAssetsLoop
			}
		}
		assets = append(assets, m.Pair.Quote)
	}

	return assets
}

func MarketsToSymbols(markets []Market) []string {
	symbols := make([]string, len(markets))

	for i, market := range markets {
		symbols[i] = market.Symbol
	}

	return symbols
}

func MarketsToPairs(markets []Market) []Pair {
	pairs := make([]Pair, len(markets))

	for i, market := range markets {
		pairs[i] = market.Pair
	}

	return pairs
}

func MarketByPair(markets []Market, pair Pair) (Market, bool) {
	for _, market := range markets {
		if market.Pair.Base == pair.Base && market.Pair.Quote == pair.Quote {
			return market, true
		}
	}

	return Market{}, false
}

func StringToPair(str string) (Pair, error) {
	parts := strings.Split(str, "/")
	if len(parts) < 2 {
		return Pair{}, fmt.Errorf("unable to split string into base/quote pair")
	}

	return Pair{Base: parts[0], Quote: parts[1]}, nil
}

func BalanceByAsset(balances []Balance, asset string) (Balance, bool) {
	for _, bal := range balances {
		if bal.Asset == asset {
			return bal, true
		}
	}

	return Balance{}, false
}

func TickWithEvents(events ...Event) Tick {
	return Tick{
		EventLog: events,
	}
}

func NewOkEvent(e Ok) Event {
	return Event{
		Type: OkEvent,
		Data: e,
	}
}

func NewCancelEvent(e Cancel) Event {
	return Event{
		Type: CancelEvent,
		Data: e,
	}
}

func NewFillEvent(e Fill) Event {
	return Event{
		Type: FillEvent,
		Data: e,
	}
}

func NewOrderUpdateEvent(e Order) Event {
	return Event{
		Type: OrderUpdateEvent,
		Data: e,
	}
}

func FindAndCloseOpenOrders(gtw Gateway, market Market) error {
	err, _ := FindAndCloseOpenOrdersReturnErrIds(gtw, market)
	return err
}

func FindAndCloseOpenOrdersReturnErrIds(gtw Gateway, market Market) (error, []string) {
	openOrders, err := gtw.AccountGateway().OpenOrders(market)
	if err != nil {
		return err, []string{}
	}

	if len(openOrders) > 0 {
		log.Printf("Found %d open orders for market %s, closing them...", len(openOrders), market)

		errs := make([]string, 0)
		errOrderIds := make([]string, 0)
		for _, order := range openOrders {
			err := gtw.AccountGateway().CancelOrder(order)
			if err != nil {
				errs = append(errs, err.Error())
				errOrderIds = append(errOrderIds, order.ID)
			}
		}

		if len(errs) > 0 {
			return fmt.Errorf("Failed to close %d orders (order ids: %s), errors: %s", len(errs), strings.Join(errOrderIds, ", "), strings.Join(errs, ", ")), errOrderIds
		}
	}

	return nil, []string{}
}
