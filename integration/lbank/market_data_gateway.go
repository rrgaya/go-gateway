package lbank

import (
	"encoding/json"
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

	for _, market := range markets {
		ws.WriteMessage([]byte("{\"action\":\"subscribe\",\"subscribe\":\"depth\",\"depth\":\"100\",\"pair\":\"" + market.Symbol + "\"}"))
		ws.WriteMessage([]byte("{\"action\":\"subscribe\",\"subscribe\":\"trade\",\"pair\":\"" + market.Symbol + "\"}"))
	}

	ch := make(chan WsGenericMessage, 100)
	ws.SubscribeMessages(ch, nil)

	go g.messageHandler(ch)

	return nil
}

type WsTrade struct {
	Ts        string  `json:"TS"`
	Amount    float64 `json:"amount"`
	Direction string  `json:"direction"`
	Price     float64 `json:"price"`
	Volume    float64 `json:"volume"`
}

type WsTradeMsg struct {
	Trade WsTrade `json:"trade"`
	Pair  string  `json:"pair"`
}

type WsDepth struct {
	Asks []gateway.PriceArray `json:"asks"`
	Bids []gateway.PriceArray `json:"bids"`
}

type WsDepthMsg struct {
	Depth WsDepth `json:"depth"`
	Pair  string  `json:"pair"`
}

func (g *MarketDataGateway) messageHandler(ch chan WsGenericMessage) {
	for msg := range ch {
		switch msg.Type {
		case "depth":
			g.processDepthUpdate(msg.Data)
		case "trade":
			g.processTradeUpdate(msg.Data)
		}
	}
}

func (g *MarketDataGateway) processDepthUpdate(data []byte) {
	msg := WsDepthMsg{}
	err := json.Unmarshal(data, &msg)
	if err != nil {
		log.Printf("LBank processDepthUpdate json.Unmarshal, body: %s err: %s", string(data), err)
		return
	}

	symbol := msg.Pair
	depth := msg.Depth

	events := make([]gateway.Event, 0, len(depth.Bids)+len(depth.Asks)+1)

	events = append(events, gateway.Event{
		Type: gateway.SnapshotSequenceEvent,
		Data: gateway.SnapshotSequence{
			Symbol: symbol,
		},
	})

	for _, price := range depth.Bids {
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

	for _, price := range depth.Asks {
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
}

func (g *MarketDataGateway) processTradeUpdate(data []byte) {
	msg := WsTradeMsg{}
	err := json.Unmarshal(data, &msg)
	if err != nil {
		log.Printf("LBank processTradeUpdate json.Unmarshal, body: %s err: %s", string(data), err)
		return
	}

	symbol := msg.Pair
	trade := msg.Trade

	events := make([]gateway.Event, 0, 1)
	events = append(events, gateway.Event{
		Type: gateway.TradeEvent,
		Data: gateway.Trade{
			Timestamp: time.Now(),
			Symbol:    symbol,
			ID:        trade.Ts,
			Direction: tradeSideToGtw(trade.Direction),
			Price:     trade.Price,
			Amount:    trade.Amount,
		},
	})

	g.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          events,
	}
}

func tradeSideToGtw(side string) gateway.Side {
	if side == "buy" {
		return gateway.Bid
	} else if side == "sell" {
		return gateway.Ask
	}
	return ""
}
