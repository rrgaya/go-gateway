package huobi

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/buger/jsonparser"
	"github.com/gorilla/websocket"
	"github.com/herenow/atomic-gtw/gateway"
)

type MarketDataGateway struct {
	options         gateway.Options
	tickCh          chan gateway.Tick
	connWriterMutex *sync.Mutex
}

func NewMarketDataGateway(options gateway.Options, tickCh chan gateway.Tick) *MarketDataGateway {
	return &MarketDataGateway{
		options:         options,
		tickCh:          tickCh,
		connWriterMutex: &sync.Mutex{},
	}
}

func (g *MarketDataGateway) SubscribeMarkets(markets []gateway.Market) error {
	batchesOf := 30
	batches := make([][]gateway.Market, ((len(markets)-1)/batchesOf)+1)
	for index, market := range markets {
		group := index / batchesOf
		batches[group] = append(batches[group], market)
	}

	log.Printf("Huobi subscribing to %d markets, spliting market data in %d websocket connections, maximum of %d markets on each websocket.", len(markets), len(batches), batchesOf)

	errsCh := make(chan error, len(batches))
	wg := sync.WaitGroup{}
	wg.Add(len(batches))

	for i, batch := range batches {
		go func(i int, markets []gateway.Market) {
			defer wg.Done()
			time.Sleep(time.Duration(50*i) * time.Millisecond)
			log.Printf("Huobi ws[%d] subscribing to %d markets...", i, len(markets))
			err := g.subscribeMarketData(markets)
			if err != nil {
				errsCh <- err
			}
		}(i, batch)
	}

	go func() {
		wg.Wait()
		close(errsCh)
	}()

	errs := make([]string, 0)
	for err := range errsCh {
		log.Printf("Huobi connection sub err: %s", err)
		errs = append(errs, err.Error())
	}

	if len(errs) > 0 {
		return fmt.Errorf("Failed to open market data conns, errs: %s", errs)
	}
	return nil
}

func (g *MarketDataGateway) subscribeMarketData(markets []gateway.Market) error {
	bookWsUrl := fmt.Sprintf("wss://api.huobi.pro/feed")
	tradeWsUrl := fmt.Sprintf("wss://api.huobi.pro/ws/1")

	dialer := websocket.Dialer{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	bookWs, _, err := dialer.Dial(bookWsUrl, nil)
	if err != nil {
		return err
	}
	tradeWs, _, err := dialer.Dial(tradeWsUrl, nil)
	if err != nil {
		return err
	}

	go g.websocketHandler("book", bookWs, markets, g.bookMessageHandler)
	go g.websocketHandler("trades", tradeWs, markets, g.tradesMessageHandler)

	err = g.marketsSubscribe(bookWs, tradeWs, markets)
	if err != nil {
		return err
	}

	return nil
}

func (g *MarketDataGateway) marketsSubscribe(bookWs, tradeWs *websocket.Conn, markets []gateway.Market) error {
	for _, market := range markets {
		topic := fmt.Sprintf("market.%s.mbp.400", market.Symbol)
		err := g.websocketWriteMessage(bookWs, HuobiWebsocketSubscribe{
			Id:  fmt.Sprintf("sub-book:%s", topic),
			Sub: fmt.Sprintf("market.%s.mbp.400", market.Symbol),
		})
		if err != nil {
			return err
		}

		topic = fmt.Sprintf("market.%s.trade.detail", market.Symbol)
		err = g.websocketWriteMessage(tradeWs, HuobiWebsocketRequest{
			Id:  fmt.Sprintf("sub-trade:%s", topic),
			Req: fmt.Sprintf("market.%s.trade.detail", market.Symbol),
		})
		if err != nil {
			return err
		}

		time.Sleep(100 * time.Millisecond)
	}

	// Periodically refresh order book, from a snapshot
	go func() {
		for {
			for _, market := range markets {
				topic := fmt.Sprintf("market.%s.mbp.400", market.Symbol)
				err := g.websocketWriteMessage(bookWs, HuobiWebsocketRequest{
					Id:  fmt.Sprintf("req-snap:%s", topic),
					Req: topic,
				})
				if err != nil {
					panic(err)
				}
				time.Sleep(100 * time.Millisecond)
			}
			time.Sleep(60 * time.Second)
		}
	}()

	return nil
}

func (g *MarketDataGateway) websocketWriteMessage(ws *websocket.Conn, msg interface{}) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	g.connWriterMutex.Lock()
	err = ws.WriteMessage(websocket.TextMessage, data)
	g.connWriterMutex.Unlock()
	return err
}

func (g *MarketDataGateway) Tick() chan gateway.Tick {
	return g.tickCh
}

func (g *MarketDataGateway) websocketHandler(_type string, ws *websocket.Conn, markets []gateway.Market, handler func(*websocket.Conn, map[string]MarketSub, []byte)) {
	pingTimer := make(chan bool)
	pingTimeout := 30 * time.Second
	go func() {
		for {
			select {
			case <-pingTimer:
				continue
			case <-time.After(pingTimeout):
				err := fmt.Errorf("Huobi market data websocket handler haven't received ping request in %v", pingTimeout)
				panic(err)
			}
		}
	}()

	marketSubs := make(map[string]MarketSub)
	if _type == "book" {
		subTimeout := 60 * time.Second
		for _, market := range markets {
			sub := NewMarketSub()
			marketSubs[market.Symbol] = sub
			go func(market gateway.Market) {
				var lastReceivedAt time.Time
				var subOk bool
				for {
					select {
					case <-sub.ReceivedTick:
						lastReceivedAt = time.Now()
						continue
					case <-sub.SubOk:
						subOk = true
						continue
					case <-time.After(subTimeout):
						log.Printf("Huobi market %s hasn't received any book updates in %v, subOk: %v, last received at: %v", market.Symbol, subTimeout, subOk, lastReceivedAt)
						if !subOk {
							log.Printf("Sub not ok, trying to resubscribe to market %s", market.Symbol)
							go func() {
								topic := fmt.Sprintf("market.%s.mbp.400", market.Symbol)
								err := g.websocketWriteMessage(ws, HuobiWebsocketSubscribe{
									Id:  fmt.Sprintf("sub-book:%s", topic),
									Sub: topic,
								})
								if err != nil {
									panic(err)
								}
							}()
						}
					}
				}
			}(market)
		}
	}

	for {
		_, message, err := ws.ReadMessage()
		if err != nil {
			ws.Close()
			panic(err)
		}

		msgReader := bytes.NewReader(message)
		reader, err := gzip.NewReader(msgReader)
		if err != nil {
			log.Printf("Huobi market data failed to unzip message, err: %s", err)
			panic(err)
		}

		data, err := ioutil.ReadAll(reader)
		if err != nil {
			log.Printf("Huobi market data failed to read all message, err: %s", err)
			panic(err)
		}

		ping, _ := jsonparser.GetInt(data, "ping")
		if ping > 0 {
			err := g.sendPong(ws)
			if err != nil {
				log.Printf("Huobi failed to send pong, err: %s", err)
				panic(err)
			}
			if g.options.Verbose {
				log.Printf("Huobi received ping of value %d, latency from server %dms", ping, (time.Now().UnixNano()/int64(time.Millisecond))-ping)
			}
			pingTimer <- true
		}

		errMsg, _ := jsonparser.GetString(data, "err-msg")
		if errMsg != "" {
			log.Printf("Huobi ws message received err msg: %s", string(data))
			continue
		}

		handler(ws, marketSubs, data)
	}
}

func (g *MarketDataGateway) extractSymbolFromTopic(topic string) string {
	pieces := strings.Split(topic, ".")
	if len(pieces) < 2 {
		log.Printf("Huobi failed to split symbol from topic %s", topic)
		return ""
	}

	return pieces[1]
}

type MarketSub struct {
	LastSequenceNumber int64
	ReceivedTick       chan bool
	SubOk              chan bool
	Lock               *sync.Mutex
}

func NewMarketSub() MarketSub {
	return MarketSub{
		ReceivedTick: make(chan bool),
		SubOk:        make(chan bool),
		Lock:         &sync.Mutex{},
	}
}

func (g *MarketDataGateway) bookMessageHandler(ws *websocket.Conn, marketSubs map[string]MarketSub, data []byte) {
	var topic string
	var dataKey string
	var snapshot, subOk bool

	ch, _ := jsonparser.GetString(data, "ch")
	if ch != "" {
		topic = ch
		dataKey = "tick"
	} else {
		rep, _ := jsonparser.GetString(data, "rep")
		if rep != "" {
			topic = rep
			dataKey = "data"
			// When we are receiving a message from rep, it means
			// it a response to a request message, containing the book snapshot
			snapshot = true
		} else {
			subbed, _ := jsonparser.GetString(data, "subbed")
			if subbed != "" {
				topic = subbed
				subOk = true
			} else {
				// Ignore message
				//log.Printf("Huobi bookMessageHandler received unprocessable message, ignoring... %s", string(data))
				return
			}
		}
	}

	symbol := g.extractSymbolFromTopic(topic)
	if symbol == "" {
		log.Printf("Huobi bookMessageHandler unable to extract symbol from topic %s...", topic)
		return
	}

	sub, ok := marketSubs[symbol]
	if !ok {
		log.Printf("Huobi expected marketSubs map to contain symbol %s", symbol)
		return
	}

	// Sub confirmation message
	if subOk {
		sub.SubOk <- true
		return
	}

	tickData, _, _, err := jsonparser.Get(data, dataKey)
	if err != nil {
		log.Printf("Huobi bookMessageHandler failed to unmarshal \"%s\" from update, err: %s, msg: %s", dataKey, err, string(data))
		return
	}

	var mbpUpdate HuobiMBP
	err = json.Unmarshal(tickData, &mbpUpdate)
	if err != nil {
		log.Printf("Huobi bookMessageHandler failed to unmarshal tick data, err: %s", err)
		return
	}

	// Partial update, check for message loss
	if !snapshot {
		if sub.LastSequenceNumber > 0 && sub.LastSequenceNumber != mbpUpdate.PrevSeqNum {
			log.Printf("Huobi market %s LastSequenceNumber (%d) was different from message prevSeqNum (%d), msg: %s", symbol, sub.LastSequenceNumber, mbpUpdate.PrevSeqNum, string(data))
			log.Printf("Huobi requesting snapshot for %s", topic)
			go func() {
				err := g.websocketWriteMessage(ws, HuobiWebsocketRequest{
					Id:  fmt.Sprintf("req-snap:2:%s", topic),
					Req: topic,
				})
				if err != nil {
					panic(err)
				}
			}()
		}
		sub.LastSequenceNumber = mbpUpdate.SeqNum
		marketSubs[symbol] = sub
	}

	events := make([]gateway.Event, 0, len(mbpUpdate.Bids)+len(mbpUpdate.Asks)+1)

	if snapshot {
		events = append(events, gateway.Event{
			Type: gateway.SnapshotSequenceEvent,
			Data: gateway.SnapshotSequence{
				Symbol: symbol,
			},
		})
	}

	for _, price := range mbpUpdate.Bids {
		events = append(events, gateway.Event{
			Type: gateway.DepthEvent,
			Data: gateway.Depth{
				Symbol: symbol,
				Side:   gateway.Bid,
				Price:  price[0],
				Amount: price[1],
			},
		})
	}
	for _, price := range mbpUpdate.Asks {
		events = append(events, gateway.Event{
			Type: gateway.DepthEvent,
			Data: gateway.Depth{
				Symbol: symbol,
				Side:   gateway.Ask,
				Price:  price[0],
				Amount: price[1],
			},
		})
	}

	g.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          events,
	}

	if !snapshot {
		sub.ReceivedTick <- true
	}
}

func (g *MarketDataGateway) tradesMessageHandler(ws *websocket.Conn, marketSubs map[string]MarketSub, data []byte) {
	ch, _ := jsonparser.GetString(data, "ch")
	if !strings.Contains(ch, "trade.detail") {
		//log.Printf("Huobi tradesMessageHandler received message of topic \"%s\", unprocessable, ignoring... %s", ch, string(data))
		return
	}

	symbol := g.extractSymbolFromTopic(ch)
	if symbol == "" {
		log.Printf("Huobi tradesMessageHandler unable to extract symbol from topic %s...", ch)
		return
	}

	tickData, _, _, err := jsonparser.Get(data, "tick")
	if err != nil {
		log.Printf("Huobi tradesMessageHandler failed to unmarshal tick data, err: %s, msg: %s", err, string(data))
		return
	}

	var tradesUpdate HuobiTrades
	err = json.Unmarshal(tickData, &tradesUpdate)
	if err != nil {
		log.Printf("Huobi tradesMessageHandler failed to unmarshal tick data, err: %s", err)
		return
	}

	events := make([]gateway.Event, len(tradesUpdate.Data))
	for i, trade := range tradesUpdate.Data {
		var direction gateway.Side
		if trade.Direction == "buy" {
			direction = gateway.Bid
		} else {
			direction = gateway.Ask
		}

		events[i] = gateway.Event{
			Type: gateway.TradeEvent,
			Data: gateway.Trade{
				ID:        string(trade.Id),
				Symbol:    symbol,
				Direction: direction,
				Price:     trade.Price,
				Amount:    trade.Amount,
			},
		}
	}

	g.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          events,
	}
}

func (g *MarketDataGateway) sendPong(ws *websocket.Conn) error {
	return g.websocketWriteMessage(ws, HuobiPong{
		Pong: time.Now().UnixNano() / int64(time.Millisecond),
	})
}

type HuobiWebsocketRequest struct {
	Req string `json:"req"`
	Id  string `json:"id"`
}

type HuobiWebsocketSubscribe struct {
	Sub string `json:"sub"`
	Id  string `json:"id"`
}

type HuobiPing struct {
	ping int64 `json:"ping"`
}

type HuobiPong struct {
	Pong int64 `json:"pong"`
}

type HuobiMBP struct {
	SeqNum     int64              `json:"seqNum"`
	PrevSeqNum int64              `json:"prevSeqNum"`
	Bids       []HuobiPriceVolume `json:"bids"`
	Asks       []HuobiPriceVolume `json:"asks"`
}

type HuobiTrades struct {
	Id        int64 `json:"id"`
	Timestamp int64 `json:"ts"`
	Data      []struct {
		Timestamp int64       `json:"ts"`
		Id        json.Number `json:"id"`
		Amount    float64     `json:"amount"`
		Price     float64     `json:"price"`
		Direction string      `json:"direction"`
	} `json:"data"`
}

type HuobiPriceVolume [2]float64
