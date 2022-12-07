package upbit

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/buger/jsonparser"
	"github.com/gorilla/websocket"
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

type TradeDataWsMsg struct {
	Type      string   `json:"type"`
	Codes     []string `json:"codes"`
	AccessKey string   `json:"accessKey,omitempty"`
}

func (g *MarketDataGateway) SubscribeMarkets(markets []gateway.Market) error {
	batchesOf := 25
	batches := make([][]gateway.Market, ((len(markets)-1)/batchesOf)+1)
	for index, market := range markets {
		group := index / batchesOf
		batches[group] = append(batches[group], market)
	}

	log.Printf("Upbit has %d markets, will need to distribute market data subscriptions in %d websocket connections, maximum of %d subscriptions on each websocket.", len(markets), len(batches), batchesOf)

	wsDone := make(chan bool)
	wsError := make(chan error)
	wsQuit := make([]chan bool, len(batches))

	wsWg := sync.WaitGroup{}
	wsWg.Add(len(batches))

	for index, batch := range batches {
		wsQuit[index] = make(chan bool)

		go func(batch []gateway.Market, quit chan bool) {
			ws, _, err := websocket.DefaultDialer.Dial("wss://crix-ws.upbit.com/websocket", nil)
			if err != nil {
				wsError <- fmt.Errorf("Failed to connect to market data ws, err: %s", err)
				return
			}

			go g.websocketConHandler(ws)

			// Subscribe to market data
			msg := make([]interface{}, 0)
			msg = append(msg, struct {
				Ticket string `json:"ticket"`
			}{"ram macbook"})
			tradeMsg := TradeDataWsMsg{Type: "crixTrade", Codes: make([]string, 0)}
			orderbookMsg := TradeDataWsMsg{Type: "crixOrderbook", Codes: make([]string, 0)}
			for _, market := range batch {
				tradeMsg.Codes = append(tradeMsg.Codes, market.Symbol)
				orderbookMsg.Codes = append(orderbookMsg.Codes, market.Symbol)
			}
			msg = append(msg, tradeMsg, orderbookMsg)
			data, err := json.Marshal(msg)
			if err != nil {
				wsError <- fmt.Errorf("failed to marshal markets subscription msg json, err: %s", err)
				return
			}
			err = ws.WriteMessage(websocket.TextMessage, data)
			if err != nil {
				wsError <- fmt.Errorf("failed to write subscription msg to ws, err: %s", err)
				return
			}

			wsWg.Done()

			shouldQuit := <-quit
			if shouldQuit {
				ws.Close()
			}
		}(batch, wsQuit[index])
	}

	go func() {
		wsWg.Wait()
		wsDone <- true
	}()

	var err error
	select {
	case <-wsDone:
		log.Printf("Finished opening %d ws connections", len(batches))
	case wsErr := <-wsError:
		err = wsErr
		log.Printf("Upbit failed to open ws, err: %s", wsErr)
		log.Printf("Closing all ws connections...")
	}

	hasErr := err != nil
	for _, quit := range wsQuit {
		select {
		case quit <- hasErr:
		default:
			// Doesn't need to quit, never connected
		}
	}

	return err
}

type OrderBookUpdateMessage struct {
	Code           string  `json:"code"`
	StreamType     string  `json:"streamType"`
	Timestamp      int64   `json:"timestamp"`
	TotalAskSize   float64 `json:"totalAskSize"`
	TotalBidSize   float64 `json:"totalBidSize"`
	OrderbookUnits []OrderbookUnit
}

type OrderbookUnit struct {
	AskPrice float64 `json:"askPrice"`
	AskSize  float64 `json:"askSize"`
	BidPrice float64 `json:"bidPrice"`
	BidSize  float64 `json:"bidSize"`
}

func (g *MarketDataGateway) websocketConHandler(ws *websocket.Conn) {
	defer ws.Close()

	wsTimeout := 15 * time.Second
	ws.SetReadDeadline(time.Now().Add(wsTimeout))

	for {
		_, message, err := ws.ReadMessage()
		if err != nil {
			panic(fmt.Errorf("Upbit ws socket error: %s", err))
		}

		ws.SetReadDeadline(time.Now().Add(wsTimeout))

		msgType, err := jsonparser.GetString(message, "type")
		if err != nil {
			log.Printf("Upbit failed to get msg type from websocket message: %s. err: %s", string(message), err)
			continue
		}

		switch msgType {
		case "crixOrderbook":
			var msg OrderBookUpdateMessage
			err := json.Unmarshal(message, &msg)
			if err != nil {
				log.Printf("Upbit failed unmarshal orderbook update message: %s, unmarshal error: %s", string(message), err)
				continue
			}

			g.processOrderBookUpdate(msg)
		case "crixTrade":
		}
	}
}

func (g *MarketDataGateway) processOrderBookUpdate(msg OrderBookUpdateMessage) {
	eventLog := make([]gateway.Event, 0, (len(msg.OrderbookUnits)*2)+1)

	// Every update is a snapshot of the top 15 price levels
	eventLog = append(eventLog, gateway.Event{
		Type: gateway.SnapshotSequenceEvent,
		Data: gateway.SnapshotSequence{
			Symbol: msg.Code,
		},
	})

	for _, bookUnit := range msg.OrderbookUnits {
		eventLog = append(eventLog, gateway.Event{
			Type: gateway.DepthEvent,
			Data: gateway.Depth{
				Symbol: msg.Code,
				Side:   gateway.Ask,
				Price:  bookUnit.AskPrice,
				Amount: bookUnit.AskSize,
			},
		})
	}
	for _, bookUnit := range msg.OrderbookUnits {
		eventLog = append(eventLog, gateway.Event{
			Type: gateway.DepthEvent,
			Data: gateway.Depth{
				Symbol: msg.Code,
				Side:   gateway.Bid,
				Price:  bookUnit.BidPrice,
				Amount: bookUnit.BidSize,
			},
		})
	}

	g.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          eventLog,
	}
}

func (g *MarketDataGateway) Tick() chan gateway.Tick {
	return g.tickCh
}
