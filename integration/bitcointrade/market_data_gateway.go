package bitcointrade

import (
	"fmt"
	"log"
	"net/url"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
)

type MarketDataGateway struct {
	options        gateway.Options
	markets        []gateway.Market
	api            *API
	tickCh         chan gateway.Tick
	fromTime       time.Time
	trackedMarkets map[string]gateway.Market
}

func NewMarketDataGateway(options gateway.Options, markets []gateway.Market, api *API, tickCh chan gateway.Tick) *MarketDataGateway {
	return &MarketDataGateway{
		options:        options,
		markets:        markets,
		api:            api,
		tickCh:         tickCh,
		fromTime:       time.Now(),
		trackedMarkets: make(map[string]gateway.Market),
	}
}

func (g *MarketDataGateway) SubscribeMarkets(markets []gateway.Market) error {
	refreshInterval := 60 * time.Second

	// Subscribe to market updates
	log.Printf("BitcoinTrade polling for %d order book updates every %v", len(markets), refreshInterval)

	for _, market := range markets {
		g.trackedMarkets[market.Symbol] = market

		go func(market gateway.Market) {
			for {
				err := g.updateBooksFromAPI(market)
				if err != nil {
					log.Printf("BitcoinTrade market data failed to fetch order book %s, err: %s", market.Symbol, err)
					time.Sleep(5 * time.Second)
					continue
				}

				time.Sleep(refreshInterval)
			}
		}(market)
	}

	return nil
}

func (g *MarketDataGateway) updateBooksFromAPI(market gateway.Market) error {
	params := url.Values{}
	params.Set("pair", market.Symbol)
	params.Set("limit", "200")
	res, err := g.api.GetBookOrders(params)
	if err != nil {
		return err
	}

	return g.processAPIBookOrders(market, res)
}

func (g *MarketDataGateway) processAPIBookOrders(market gateway.Market, book APIBookOrders) error {
	events := make([]gateway.Event, 0, len(book.Buying)+len(book.Selling))
	events = append(events, gateway.Event{
		Type: gateway.SnapshotSequenceEvent,
		Data: gateway.SnapshotSequence{
			Symbol: market.Symbol,
		},
	})

	bidsMap := make(map[float64]float64)
	asksMap := make(map[float64]float64)

	for _, order := range book.Buying {
		depth, _ := bidsMap[order.UnitPrice]
		depth += order.Amount
		bidsMap[order.UnitPrice] = depth
	}

	for _, order := range book.Selling {
		depth, _ := bidsMap[order.UnitPrice]
		depth += order.Amount
		asksMap[order.UnitPrice] = depth
	}

	for price, amount := range bidsMap {
		events = append(events, priceToGtwEvent(market.Symbol, gateway.Bid, price, amount))
	}
	for price, amount := range asksMap {
		events = append(events, priceToGtwEvent(market.Symbol, gateway.Ask, price, amount))
	}

	g.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          events,
	}

	return nil
}

func (g *MarketDataGateway) ProcessOrderUpdate(topic string, update WSOrder) {
	symbol := update.Pair.Code
	_, ok := g.trackedMarkets[symbol]
	if !ok {
		return
	}

	side := wsTypeToSide(update.Type)
	amount := update.Amount
	if topic == "cancel_order" {
		amount = 0.0
	}

	g.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog: []gateway.Event{
			gateway.Event{
				Type: gateway.RawOrderEvent,
				Data: gateway.RawOrder{
					Symbol: symbol,
					ID:     update.ID,
					Side:   side,
					Price:  update.UnitPrice,
					Amount: amount,
				},
			},
		},
	}
}

func (g *MarketDataGateway) ProcessOrderCompleted(topic string, update WSOrderCompleted) {
	symbol := update.Pair.Code
	_, ok := g.trackedMarkets[symbol]
	if !ok {
		return
	}

	side := wsTypeToSide(update.Type)
	id := fmt.Sprintf("%s-%s", update.ActiveOrderCode, update.PassiveOrderCode)

	g.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog: []gateway.Event{
			gateway.Event{
				Type: gateway.TradeEvent,
				Data: gateway.Trade{
					Timestamp: update.CreateDate,
					Symbol:    symbol,
					ID:        id,
					Direction: side,
					OrderID:   update.PassiveOrderCode,
					Amount:    update.Amount,
					Price:     update.UnitPrice,
				},
			},
		},
	}
}

func priceToGtwEvent(symbol string, side gateway.Side, price, amount float64) gateway.Event {
	return gateway.Event{
		Type: gateway.DepthEvent,
		Data: gateway.Depth{
			Symbol: symbol,
			Side:   side,
			Price:  price,
			Amount: amount,
		},
	}
}

func bookExecToGtwEvent(symbol string, e APIBookExecution) gateway.Event {
	side := mapTypeToCommonSide(e.Type)
	id := fmt.Sprintf("%s-%s", e.ActiveOrderCode, e.PassiveOrderCode)

	return gateway.Event{
		Type: gateway.TradeEvent,
		Data: gateway.Trade{
			Timestamp: e.CreateDate,
			Symbol:    symbol,
			ID:        id,
			Direction: side,
			OrderID:   e.PassiveOrderCode,
			Amount:    e.Amount,
			Price:     e.UnitPrice,
		},
	}
}
