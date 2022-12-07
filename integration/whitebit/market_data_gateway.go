package whitebit

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

type MarketDataGateway struct {
	baseURL string
	options gateway.Options
	tickCh  chan gateway.Tick
}

func NewMarketDataGateway(baseURL string, options gateway.Options, tickCh chan gateway.Tick) *MarketDataGateway {
	return &MarketDataGateway{
		baseURL: baseURL,
		options: options,
		tickCh:  tickCh,
	}
}

func (g *MarketDataGateway) SubscribeMarkets(markets []gateway.Market) error {
	batchesOf := 50
	batches := make([][]gateway.Market, ((len(markets)-1)/batchesOf)+1)

	log.Printf("Subscribing on [%s] to %d markets, will need %d websocket connections, maximum of %d markets on each websocket.", g.baseURL, len(markets), len(batches), batchesOf)

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
	ws := utils.NewWsClient()

	err := ws.Connect(g.baseURL)
	if err != nil {
		return fmt.Errorf("ws connect err: %s", err)
	}

	symbols := make([]string, 0)
	for _, market := range markets {
		symbols = append(symbols, fmt.Sprintf("\"%s\"", market.Symbol))
	}

	symbolsStr := strings.Join(symbols, ",")

	if err := ws.WriteMessage([]byte(fmt.Sprintf("{ \"method\": \"subscribe\", \"ch\": \"orderbook/full\", \"params\": { \"symbols\": [%s] }, \"id\": 1 }", symbolsStr))); err != nil {
		return fmt.Errorf("failed write orderbook sub msg to ws: %s", err)
	}

	if err := ws.WriteMessage([]byte(fmt.Sprintf("{ \"method\": \"subscribe\", \"ch\": \"trades\", \"params\": { \"symbols\": [%s] }, \"id\": 2 }", symbolsStr))); err != nil {
		return fmt.Errorf("failed write orderbook sub msg to ws: %s", err)
	}

	ch := make(chan []byte, 100)
	ws.SubscribeMessages(ch)

	go g.messageHandler(ch)

	return nil
}

type WsMessage struct {
	Result   json.RawMessage `json:"result"`
	ID       int64           `json:"id"`
	Ch       string          `json:"ch"`
	Snapshot json.RawMessage `json:"snapshot"`
	Update   json.RawMessage `json:"update"`
	Error    string          `json:"error"`
}

type WsBook struct {
	Ts   int64                `json:"t"` // Millisecond timestamp
	Seq  int64                `json:"s"` // Sequence number
	Asks []gateway.PriceArray `json:"a"` // Asks
	Bids []gateway.PriceArray `json:"b"` // Bids
}

type WsTrade struct {
	Ts     int64   `json:"t"` // Millisecond timestamp
	ID     int64   `json:"i"` // Trade identifier
	Side   string  `json:"s"`
	Price  float64 `json:"p,string"`
	Amount float64 `json:"q,string"`
}

func (g *MarketDataGateway) messageHandler(ch chan []byte) {
	for data := range ch {
		var msg WsMessage
		err := json.Unmarshal(data, &msg)
		if err != nil {
			log.Printf("Failed to unmarhsal WsMessage [%s] err [%s]", string(data), err)
			continue
		}

		// Check errors
		if msg.Error != "" {
			log.Printf("WS [%s] msg w/ error msg: %s", g.baseURL, string(data))
			continue
		}

		switch {
		case msg.Ch == "orderbook/full":
			if err := g.processSnapAndUpdate(msg, g.processBookUpdates); err != nil {
				log.Printf("%s error processing books \"%+v\": %s", g.baseURL, msg, err)
			}
		case msg.Ch == "trades":
			if err := g.processSnapAndUpdate(msg, g.processTradeUpdates); err != nil {
				log.Printf("%s error processing trades \"%+v\": %s", g.baseURL, msg, err)
			}
		}
	}
}

func (g *MarketDataGateway) processSnapAndUpdate(msg WsMessage, fnc func(data json.RawMessage, snapshot bool) error) error {
	if string(msg.Update) == "" && string(msg.Snapshot) == "" {
		return fmt.Errorf("expected depth update msg to contain snapshot or update data, instead received: [%+v]", msg)
	}

	var snapErr, updateErr error

	if string(msg.Snapshot) != "" {
		snapErr = fnc(msg.Snapshot, true)
	}

	if string(msg.Update) != "" {
		updateErr = fnc(msg.Update, false)
	}

	if snapErr != nil || updateErr != nil {
		return fmt.Errorf("snapErr: %s, updateErr: %s", snapErr, updateErr)
	}

	return nil
}

func (g *MarketDataGateway) processBookUpdates(data json.RawMessage, snapshot bool) error {
	var bookUpdates map[string]WsBook
	err := json.Unmarshal(data, &bookUpdates)
	if err != nil {
		return err
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

	for symbol, update := range bookUpdates {
		if snapshot {
			eventLog = append(eventLog, gateway.Event{
				Type: gateway.SnapshotSequenceEvent,
				Data: gateway.SnapshotSequence{
					Symbol: symbol,
				},
			})
		}
		appendEvents(symbol, gateway.Ask, update.Asks)
		appendEvents(symbol, gateway.Bid, update.Bids)
	}

	g.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          eventLog,
	}

	return nil
}

func (g *MarketDataGateway) processTradeUpdates(data json.RawMessage, snapshot bool) error {
	// Ignore initial trades snapshot
	if snapshot {
		return nil
	}

	var tradesUpdate map[string][]WsTrade
	err := json.Unmarshal(data, &tradesUpdate)
	if err != nil {
		return err
	}

	eventLog := make([]gateway.Event, 0)
	for symbol, update := range tradesUpdate {
		for _, trade := range update {
			var side gateway.Side
			if trade.Side == "buy" {
				side = gateway.Bid
			} else {
				side = gateway.Ask
			}

			event := gateway.Event{
				Type: gateway.TradeEvent,
				Data: gateway.Trade{
					Symbol:    symbol,
					Timestamp: time.UnixMilli(trade.Ts),
					ID:        strconv.FormatInt(trade.ID, 10),
					Direction: side,
					Price:     trade.Price,
					Amount:    trade.Amount,
				},
			}

			eventLog = append(eventLog, event)

		}
	}

	g.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          eventLog,
	}

	return nil
}
