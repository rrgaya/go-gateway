package probit

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
)

type MarketDataGateway struct {
	gtw    *Gateway
	tickCh chan gateway.Tick
	ws     *WsSession
}

func NewMarketDataGateway(gtw *Gateway, tickCh chan gateway.Tick) *MarketDataGateway {
	return &MarketDataGateway{
		gtw:    gtw,
		tickCh: tickCh,
	}
}

func (g *MarketDataGateway) Connect() error {
	g.ws = NewWsSession()

	err := g.ws.Connect()
	if err != nil {
		return err
	}

	return nil
}

func (g *MarketDataGateway) SubscribeMarkets(markets []gateway.Market) error {
	ch := make(chan WsMessage, 10)

	go g.processMarketData(ch)

	for _, market := range markets {
		err := g.ws.SubscribeOrderBook(market.Symbol, ch, nil)

		if err != nil {
			close(ch)
			return fmt.Errorf("failed to sub to market %s, err %s", market.Symbol, err)
		}
	}

	return nil
}

func (g *MarketDataGateway) processMarketData(ch chan WsMessage) {
	seq := int64(0)
	marketsMap := make(map[string]gateway.Market)
	for _, market := range g.gtw.Markets() {
		marketsMap[market.Symbol] = market
	}

	for msg := range ch {
		if msg.Status != "ok" {
			log.Printf("Received market data update with not ok status, msg: %+v", msg)
			continue
		}

		market, ok := marketsMap[msg.MarketID]
		if !ok {
			log.Printf("ProBit received market data update for unkown market id %s", msg.MarketID)
			continue
		}

		var rows []WsBookRow
		err := json.Unmarshal(msg.OrderBooks, &rows)
		if err != nil {
			log.Printf("ProBit failed to unmarshal order_books market data, msg %s, err: %s", string(msg.OrderBooks), err)
			continue
		}

		events := make([]gateway.Event, len(rows))
		for i, row := range rows {
			var side gateway.Side
			if row.Side == "buy" {
				side = gateway.Bid
			} else if row.Side == "sell" {
				side = gateway.Ask
			} else {
				panic(fmt.Errorf("ProBit orderbook row invalid side %s, can't map it to gateway side, on msg %+v", row.Side, msg))
			}

			events[i] = gateway.Event{
				Type: gateway.DepthEvent,
				Data: gateway.Depth{
					Symbol: market.Symbol,
					Side:   side,
					Price:  row.Price,
					Amount: row.Quantity,
				},
			}
		}

		seq += 1
		g.tickCh <- gateway.Tick{
			Sequence:          seq,
			ReceivedTimestamp: time.Now(),
			EventLog:          events,
		}
	}
}
