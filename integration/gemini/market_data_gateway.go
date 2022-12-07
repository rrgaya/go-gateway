package gemini

import (
	"fmt"
	"log"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
)

type MarketDataGateway struct {
	options    gateway.Options
	marketsMap map[string]gateway.Market
	marketsWs  map[string]*WsSession
	tickCh     chan gateway.Tick
}

func NewMarketDataGateway(tickCh chan gateway.Tick) *MarketDataGateway {
	return &MarketDataGateway{
		marketsMap: make(map[string]gateway.Market),
		marketsWs:  make(map[string]*WsSession),
		tickCh:     tickCh,
	}
}

func (g *MarketDataGateway) Connect() error {
	return nil
}

func (g *MarketDataGateway) SubscribeMarkets(markets []gateway.Market) error {
	for _, market := range markets {
		g.marketsMap[market.Symbol] = market
	}

	log.Printf("%s subscribing to %d markets...", Exchange.Name, len(markets))

	for _, market := range g.marketsMap {
		ws := NewWsSession(market)
		g.marketsWs[market.Symbol] = ws

		ch := make(chan WsResponse)
		ws.SubscribeMessages(ch, nil)

		request := WsRequest{
			Bids:      true,
			Offers:    true,
			Trades:    true,
			Heartbeat: true,
		}

		ws.Connect(request)

		go g.subscriptionMessageHandler(ch, market)
	}

	return nil
}

func (g *MarketDataGateway) subscriptionMessageHandler(ch chan WsResponse, market gateway.Market) {
	for msg := range ch {
		if msg.Type == "update" && len(msg.Events) > 0 {
			eventLogs := make([]gateway.Event, 0)
			firstEvent := msg.Events[0]

			if firstEvent.Type == "change" {
				if firstEvent.Reason == "initial" {
					eventLogs = append(eventLogs, gateway.Event{
						Type: gateway.SnapshotSequenceEvent,
						Data: gateway.SnapshotSequence{
							Symbol: market.Symbol,
						},
					})
				}

				g.processOrderBookEvents(msg.Events, market, &eventLogs)
			} else if firstEvent.Type == "trade" {
				g.processTradeEvents(msg.Events, market, &eventLogs)
			} else {
				return
			}

			g.tickCh <- gateway.Tick{
				ReceivedTimestamp: time.Now(),
				EventLog:          eventLogs,
			}
		}
	}

}

func (g *MarketDataGateway) processOrderBookEvents(event []WsEvent, market gateway.Market, eventLogs *[]gateway.Event) {
	for _, event := range event {
		if event.Type != "change" {
			continue
		}

		var side gateway.Side
		if event.Side == "bid" {
			side = gateway.Bid
		} else {
			side = gateway.Ask
		}

		ev := gateway.Event{
			Type: gateway.DepthEvent,
			Data: gateway.Depth{
				Symbol: market.Symbol,
				Side:   side,
				Price:  event.Price,
				Amount: event.Remaining,
			},
		}

		*eventLogs = append(*eventLogs, ev)
	}
}

func (g *MarketDataGateway) processTradeEvents(events []WsEvent, market gateway.Market, eventLogs *[]gateway.Event) {
	for _, event := range events {
		if event.Type != "trade" {
			continue
		}

		var side gateway.Side
		if event.MakerSide == "bid" {
			side = gateway.Bid
		} else {
			side = gateway.Ask
		}

		e := gateway.Event{
			Type: gateway.TradeEvent,
			Data: gateway.Trade{
				Symbol:    market.Symbol,
				ID:        fmt.Sprint(event.TradeId),
				Direction: side,
				Amount:    event.Amount,
				Price:     event.Price,
			},
		}

		*eventLogs = append(*eventLogs, e)
	}
}
