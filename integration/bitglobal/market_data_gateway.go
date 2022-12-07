package bitglobal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
)

type MarketDataGateway struct {
	options    gateway.Options
	marketsMap map[string]gateway.Market
	tickCh     chan gateway.Tick
}

func NewMarketDataGateway(opts gateway.Options, tickCh chan gateway.Tick) *MarketDataGateway {
	return &MarketDataGateway{
		options:    opts,
		marketsMap: make(map[string]gateway.Market),
		tickCh:     tickCh,
	}
}

func (g *MarketDataGateway) Connect() error {
	return nil
}

func (g *MarketDataGateway) SubscribeMarkets(markets []gateway.Market) error {
	ws := NewWsSession(g.options)
	err := ws.Connect()
	if err != nil {
		return err
	}

	args := make([]string, 0)
	for _, market := range markets {
		args = append(args, fmt.Sprintf("ORDERBOOK:%s", market.Symbol))
		args = append(args, fmt.Sprintf("TRADE:%s", market.Symbol))
	}

	subReq := WsRequest{
		Cmd:  "subscribe",
		Args: args,
	}

	// Check for response
	ch := make(chan WsGenericMessage, 10)
	unsub := make(chan bool, 1)
	ws.SubscribeMessages(ch, unsub)
	subSuccess := make(chan bool)
	go func() {
		for msg := range ch {
			if msg.Code == "00001" {
				subSuccess <- true
			}
		}
	}()

	err = ws.SendRequest(subReq)
	if err != nil {
		unsub <- true
		return fmt.Errorf("send sub request err: %s", err)
	}

	// Wait for auth
	select {
	case <-subSuccess:
		log.Printf("Succesfully subbed to market data...")
	case <-time.After(5 * time.Second):
		unsub <- true
		return fmt.Errorf("sub timeout after 5 seconds...")
	}

	unsub <- true
	ch = make(chan WsGenericMessage, 100)
	ws.SubscribeMessages(ch, nil)
	go g.subscriptionMessageHandler(ch)

	return nil
}

func (g *MarketDataGateway) subscriptionMessageHandler(ch chan WsGenericMessage) {
	for msg := range ch {
		switch msg.Topic {
		case "ORDERBOOK":
			if err := g.processDepthUpdate(msg.Data); err != nil {
				log.Printf("%s error processing \"%s\": %s", Exchange.Name, msg.Topic, err)
			}
		case "TRADE":
			if err := g.processTradesUpdate(msg.Data); err != nil {
				log.Printf("%s error processing \"%s\": %s", Exchange.Name, msg.Topic, err)
			}
		}
	}
}

func (g *MarketDataGateway) processDepthUpdate(data json.RawMessage) error {
	msg := WsDepth{}
	err := json.Unmarshal(data, &msg)
	if err != nil {
		return fmt.Errorf("json.Unmarshal, body: %s err: %s", string(data), err)
	}

	symbol := msg.Symbol

	events := make([]gateway.Event, 0, len(msg.Bids)+len(msg.Asks)+1)

	for _, price := range msg.Bids {
		events = append(events, gateway.Event{
			Type: gateway.DepthEvent,
			Data: gateway.Depth{
				Symbol: symbol,
				Side:   gateway.Bid,
				Price:  price.Price,
				Amount: price.Amount,
			},
		})
	}
	for _, price := range msg.Asks {
		events = append(events, gateway.Event{
			Type: gateway.DepthEvent,
			Data: gateway.Depth{
				Symbol: symbol,
				Side:   gateway.Ask,
				Price:  price.Price,
				Amount: price.Amount,
			},
		})
	}

	g.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          events,
	}

	return nil
}

func (g *MarketDataGateway) processTradesUpdate(data json.RawMessage) error {
	x := bytes.TrimLeft(data, " \t\r\n")

	isArray := len(x) > 0 && x[0] == '['
	isObject := len(x) > 0 && x[0] == '{'

	var trades []WsTrade
	if isArray {
		err := json.Unmarshal(data, &trades)
		if err != nil {
			return fmt.Errorf("json.Unmarshal, body: %s err: %s", string(data), err)
		}
	} else if isObject {
		var trade WsTrade
		err := json.Unmarshal(data, &trade)
		if err != nil {
			return fmt.Errorf("json.Unmarshal, body: %s err: %s", string(data), err)
		}

		trades = append(trades, trade)
	} else {
		return fmt.Errorf("unable process trades data \"%s\"", string(data))
	}

	events := make([]gateway.Event, 0, len(trades))
	for _, trade := range trades {
		direction := gateway.Bid
		if trade.Side == "sell" {
			direction = gateway.Ask
		}

		events = append(events, gateway.Event{
			Type: gateway.TradeEvent,
			Data: gateway.Trade{
				Timestamp: time.Now(),
				Symbol:    trade.Symbol,
				ID:        trade.Ver,
				Direction: direction,
				Price:     trade.Price,
				Amount:    trade.Quantity,
			},
		})
	}

	g.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          events,
	}

	return nil
}
