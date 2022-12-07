package hotcoin

import (
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
)

type MarketDataGateway struct {
	options gateway.Options
	tickCh  chan gateway.Tick
}

func NewMarketDataGateway(options gateway.Options, tickCh chan gateway.Tick) *MarketDataGateway {
	return &MarketDataGateway{
		options: options,
		tickCh:  tickCh,
	}
}

func (g *MarketDataGateway) Connect() error {
	return nil
}

func (g *MarketDataGateway) SubscribeMarkets(markets []gateway.Market) error {
	batchesOf := 50
	batches := make([][]gateway.Market, ((len(markets)-1)/batchesOf)+1)

	log.Printf("Hotcoin subscribing to %d markets, will need %d websocket connections, maximum of %d markets on each websocket.", len(markets), len(batches), batchesOf)

	for index, market := range markets {
		group := index / batchesOf

		if batches[group] == nil {
			batches[group] = make([]gateway.Market, 0)
		}

		batches[group] = append(batches[group], market)
	}

	for _, batch := range batches {
		err := g.subscribeMarketData(batch)
		if err != nil {
			return err
		}
	}

	return nil
}

func (g *MarketDataGateway) subscribeMarketData(markets []gateway.Market) error {
	ws := NewWsSession()
	err := ws.Connect()
	if err != nil {
		return fmt.Errorf("ws connect err: %s", err)
	}

	for _, market := range markets {
		if err := ws.WriteMessage([]byte(fmt.Sprintf("{\"sub\": \"market.%s.trade.depth\"}", market.Symbol))); err != nil {
			return fmt.Errorf("failed write orderbook [%s] sub msg to ws: %s", market, err)
		}
		if err := ws.WriteMessage([]byte(fmt.Sprintf("{\"sub\": \"market.%s.trade.detail\"}", market.Symbol))); err != nil {
			return fmt.Errorf("failed write trades [%s] sub msg to ws: %s", market, err)
		}
	}

	ch := make(chan WsResponse, 100)
	ws.SubscribeMessages(ch, nil)

	go g.messageHandler(ch)

	return nil
}

type WsDepth struct {
	Asks []gateway.PriceArray `json:"asks"`
	Bids []gateway.PriceArray `json:"bids"`
}

type WsTrade struct {
	TradeID   int64   `json:"tradeId"`
	Ts        string  `json:"ts"`
	Price     float64 `json:"price,string"`
	Amount    float64 `json:"amount,string"`
	Direction string  `json:"direction"`
}

var depthUpdateRegexp = regexp.MustCompile(`market\.(.*)\.trade\.depth`)
var tradeUpdateRegexp = regexp.MustCompile(`market\.(.*)\.trade\.detail`)

func (g *MarketDataGateway) messageHandler(ch chan WsResponse) {
	for msg := range ch {
		if msg.Status != "ok" {
			log.Printf("Hotcoin received err msg [%s] status [%s] code [%d] on ws", msg.Message, msg.Status, msg.Code)

			continue
		}

		if matches := depthUpdateRegexp.FindStringSubmatch(msg.Channel); len(matches) > 1 {
			if err := g.processDepthUpdate(matches[1], msg); err != nil {
				log.Printf("%s error processing \"%s\": %s", Exchange.Name, msg.Channel, err)
			}
		} else if matches := tradeUpdateRegexp.FindStringSubmatch(msg.Channel); len(matches) > 1 {
			if err := g.processTradeUpdate(matches[1], msg); err != nil {
				log.Printf("%s error processing \"%s\": %s", Exchange.Name, msg.Channel, err)
			}
		}
	}
}

func (g *MarketDataGateway) processDepthUpdate(symbol string, msg WsResponse) error {
	if string(msg.Data) == "" {
		return nil
	}

	var depth WsDepth
	err := json.Unmarshal(msg.Data, &depth)
	if err != nil {
		return fmt.Errorf("json umarshal err: %s", err)
	}

	eventLog := make([]gateway.Event, 0)
	appendEvents := func(symbol string, side gateway.Side, prices []gateway.PriceArray) {
		for _, order := range prices {
			event := gateway.Event{
				Type: gateway.DepthEvent,
				Data: gateway.Depth{
					Symbol: symbol,
					Side:   side,
					Price:  order.Price,
					Amount: order.Amount,
				},
			}

			eventLog = append(eventLog, event)
		}
	}

	eventLog = append(eventLog, gateway.Event{
		Type: gateway.SnapshotSequenceEvent,
		Data: gateway.SnapshotSequence{
			Symbol: symbol,
		},
	})
	appendEvents(symbol, gateway.Ask, depth.Asks)
	appendEvents(symbol, gateway.Bid, depth.Bids)

	g.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          eventLog,
	}

	return nil
}

func (g *MarketDataGateway) processTradeUpdate(symbol string, msg WsResponse) error {
	if string(msg.Data) == "" {
		return nil
	}
	var trades []WsTrade
	err := json.Unmarshal(msg.Data, &trades)
	if err != nil {
		return fmt.Errorf("json umarshal err: %s", err)
	}

	eventLog := make([]gateway.Event, 0)
	for _, trade := range trades {

		var side gateway.Side
		if trade.Direction == "buy" {
			side = gateway.Bid
		} else {
			side = gateway.Ask
		}

		var ts time.Time
		tsInt, err := strconv.ParseInt(trade.Ts, 10, 64)
		if err != nil {
			log.Printf("Hotcoin failed to process trade detail time [%s], err: %s", trade.Ts, err)
		} else {
			ts = time.UnixMilli(tsInt)
		}

		event := gateway.Event{
			Type: gateway.TradeEvent,
			Data: gateway.Trade{
				Symbol:    symbol,
				Timestamp: ts,
				ID:        strconv.FormatInt(trade.TradeID, 10),
				Direction: side,
				Price:     trade.Price,
				Amount:    trade.Amount,
			},
		}

		eventLog = append(eventLog, event)
	}

	g.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          eventLog,
	}

	return nil
}
