package bittrex

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
	"github.com/thebotguys/signalr"
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

	log.Printf("Subscribing on [%s] to %d markets, will need %d websocket connections, maximum of %d markets on each websocket.", Exchange, len(markets), len(batches), batchesOf)

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

type BookDepth struct {
	MarketSymbol string      `json:"marketSymbol"`
	Depth        int         `json:"depth"`
	Sequence     int64       `json:"sequence"`
	BidDeltas    []BookPrice `json:"bidDeltas"`
	AskDeltas    []BookPrice `json:"askDeltas"`
}

type APIBookDepth struct {
	Bid []BookPrice `json:"bid"`
	Ask []BookPrice `json:"ask"`
}

type BookPrice struct {
	Quantity float64 `json:"quantity,string"`
	Rate     float64 `json:"rate,string"`
}

type depthQueue struct {
	ReceivedSnapshot bool
	Queue            []BookDepth
	Lock             sync.Mutex
}

const BOOK_DEPTH = 500

func (g *MarketDataGateway) subscribeMarketData(markets []gateway.Market) error {
	depthQueues := make(map[string]*depthQueue)
	client := signalr.NewWebsocketClient()

	heartbeatRcvd := make(chan bool, 1)
	client.OnClientMethod = func(hub, method string, arguments []json.RawMessage) {
		switch method {
		case "heartbeat":
			heartbeatRcvd <- true
		case "orderBook":
			if len(arguments) > 0 {
				g.processOrderBookMsg(arguments[0], depthQueues)
			}
		case "trade":
			if len(arguments) > 0 {
				g.processTradeMsg(arguments[0])
			}
		}
	}

	err := client.Connect("https", "socket-v3.bittrex.com", []string{"c3"})
	if err != nil {
		return fmt.Errorf("signalr connect err: %s", err)
	}

	log.Printf("Bittrex connected to signalr mkt data stream ws...")

	resList, err := sendWsCmds(client, "c3", "Subscribe", []string{"heartbeat"})
	if err != nil {
		return err
	}

	if len(resList) < 1 || !resList[0].Success {
		return fmt.Errorf("ws heartbeat sub failed, res: %+v", resList)
	}

	go wsHeartbeatChecker(heartbeatRcvd, 10*time.Second)

	topics := make([]string, 0)
	for _, market := range markets {
		queue := &depthQueue{
			Queue: make([]BookDepth, 0),
		}
		bookTopic := fmt.Sprintf("orderbook_%s_%d", market.Symbol, BOOK_DEPTH)
		tradeTopic := fmt.Sprintf("trade_%s", market.Symbol)

		topics = append(topics, bookTopic)
		topics = append(topics, tradeTopic)
		depthQueues[market.Symbol] = queue
	}

	resList, err = sendWsCmds(client, "c3", "Subscribe", topics)
	if err != nil {
		return err
	}
	for i, res := range resList {
		if !res.Success {
			topic := topics[i]
			return fmt.Errorf("ws mktd sub failed, on topic [%s], res: %+v", topic, resList)
		}
	}

	// Wait 1 second before starting to fetch order book snapshots
	// This will allow us to start queing the initial order book partial updates
	time.Sleep(1 * time.Second)

	// Get initial oder book snapshots
	hc := utils.NewHttpClient()
	hc.UseProxies(g.options.Proxies)

	for _, market := range markets {
		uri := fmt.Sprintf("https://api.bittrex.com/v3/markets/%s/orderbook?depth=%d", market.Symbol, BOOK_DEPTH)
		req, err := http.NewRequest(http.MethodGet, uri, nil)
		if err != nil {
			return fmt.Errorf("http new req err: %s", err)
		}

		res, err := hc.SendRequest(req)
		if err != nil {
			return fmt.Errorf("http client send request err: %s", err)
		}

		body, err := io.ReadAll(res.Body)
		if err != nil {
			return fmt.Errorf("read http res body err: %s", err)
		}

		var bookRes APIBookDepth
		err = json.Unmarshal(body, &bookRes)
		if err != nil {
			return fmt.Errorf("unmarshal book data res: %s, err: %s", string(body), err)
		}

		seqHeader := res.Header.Get("Sequence")
		if seqHeader == "" {
			return fmt.Errorf("book snapshot [%s] missing sequence header", market.Symbol)
		}

		snapshotSeq, err := strconv.ParseInt(seqHeader, 10, 64)
		if err != nil {
			return fmt.Errorf("failed parse book snapshot [%s] sequence header [%s] err: %s", market.Symbol, seqHeader, err)
		}

		queue, ok := depthQueues[market.Symbol]
		if !ok {
			return fmt.Errorf("expected depthQueues to contain queue for market %s", market.Symbol)
		}

		queue.Lock.Lock()

		// Process initial snapshot
		g.processDepths(market.Symbol, bookRes.Bid, bookRes.Ask, snapshotSeq, true)
		// Process any queued updates
		i := 0
		for _, depth := range queue.Queue {
			if depth.Sequence > snapshotSeq {
				i += 1
				g.processDepths(market.Symbol, depth.BidDeltas, depth.AskDeltas, depth.Sequence, false)
			}
		}

		if i > 0 {
			log.Printf("Bittrex market %s processed %d queued depth events after fetching OB snapshot", market.Symbol, i)
		}

		queue.ReceivedSnapshot = true
		queue.Queue = nil

		queue.Lock.Unlock()
	}

	return nil
}

func (g *MarketDataGateway) processOrderBookMsg(arg json.RawMessage, depthQueues map[string]*depthQueue) {
	data, err := wsDecodeMsg(arg)
	if err != nil {
		log.Printf("Bittrex mktd gtw failed decode orderBook msg [%s], err: %s", string(arg), err)
		return
	}

	var bookRes BookDepth
	err = json.Unmarshal(data, &bookRes)
	if err != nil {
		log.Printf("Bittrex book unmarshal [%s] err: %s", data, err)
		return
	}

	queue, ok := depthQueues[bookRes.MarketSymbol]
	if !ok {
		log.Printf("Bittrex failed to find depthQueue for market [%s]", bookRes.MarketSymbol)
		return
	}

	queue.Lock.Lock()
	if queue.ReceivedSnapshot {
		g.processDepths(bookRes.MarketSymbol, bookRes.BidDeltas, bookRes.AskDeltas, bookRes.Sequence, false)
	} else {
		queue.Queue = append(queue.Queue, bookRes)
	}
	queue.Lock.Unlock()
}

func (g *MarketDataGateway) processTradeMsg(arg json.RawMessage) {
	data, err := wsDecodeMsg(arg)
	if err != nil {
		log.Printf("Bittrex mktd gtw failed decode trades msg [%s], err: %s", string(arg), err)
		return
	}

	err = g.processTrades(data)
	if err != nil {
		log.Printf("%s failed to processtrades, err: %s", Exchange, err)
	}
}

func (g *MarketDataGateway) processDepths(symbol string, bids, asks []BookPrice, seq int64, snapshot bool) {
	eventLog := make([]gateway.Event, 0)
	appendEvents := func(side gateway.Side, prices []BookPrice) {
		for _, order := range prices {
			event := gateway.Event{
				Type: gateway.DepthEvent,
				Data: gateway.Depth{
					Symbol: symbol,
					Side:   side,
					Price:  order.Rate,
					Amount: order.Quantity,
				},
			}

			eventLog = append(eventLog, event)
		}
	}

	if snapshot {
		eventLog = append(eventLog, gateway.Event{
			Type: gateway.SnapshotSequenceEvent,
			Data: gateway.SnapshotSequence{
				Symbol: symbol,
			},
		})
	}

	appendEvents(gateway.Ask, asks)
	appendEvents(gateway.Bid, bids)

	g.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		Sequence:          seq,
		EventLog:          eventLog,
	}
}

type WsTradeMsg struct {
	MarketSymbol string    `json:"marketSymbol"`
	Sequence     int64     `json:"sequence"`
	Deltas       []WsTrade `json:"deltas"`
}

type WsTrade struct {
	ID         string  `json:"id"`
	Quantity   float64 `json:"quantity,string"`
	Rate       float64 `json:"rate,string"`
	TakerSide  string  `json:"takerSide"`
	ExecutedAt string  `json:"executedAt"`
}

func (g *MarketDataGateway) processTrades(data json.RawMessage) error {
	var msg WsTradeMsg
	err := json.Unmarshal(data, &msg)
	if err != nil {
		return err
	}

	events := make([]gateway.Event, 0)
	for _, trade := range msg.Deltas {
		var direction gateway.Side
		if trade.TakerSide == "BUY" {
			direction = gateway.Bid
		} else {
			direction = gateway.Ask
		}

		ts, err := time.Parse(time.RFC3339, trade.ExecutedAt)
		if err != nil {
			log.Printf("Bittrex failed to parse trade timestamp [%s] err: %s", trade.ExecutedAt, err)
		}

		event := gateway.Trade{
			ID:        trade.ID,
			Timestamp: ts,
			Symbol:    msg.MarketSymbol,
			Price:     trade.Rate,
			Amount:    trade.Quantity,
			Direction: direction,
		}

		events = append(events, gateway.Event{
			Type: gateway.TradeEvent,
			Data: event,
		})
	}

	if len(events) > 0 {
		g.tickCh <- gateway.Tick{
			EventLog: events,
		}
	}

	return nil
}
