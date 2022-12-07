package binance

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/herenow/atomic-gtw/gateway"
)

type MarketDataGateway struct {
	options    gateway.Options
	markets    []gateway.Market
	futuresApi bool
	api        *API
	tickCh     chan gateway.Tick
}

func NewMarketDataGateway(options gateway.Options, markets []gateway.Market, api *API, futuresApi bool, tickCh chan gateway.Tick) *MarketDataGateway {
	symbolsToMarket := make(map[string]gateway.Market)

	for _, market := range markets {
		symbolsToMarket[market.Symbol] = market
	}

	return &MarketDataGateway{
		options:    options,
		markets:    markets,
		futuresApi: futuresApi,
		api:        api,
		tickCh:     tickCh,
	}
}

func (g *MarketDataGateway) Connect() error {
	return nil
}

type binanceStreamMessage struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

func (g *MarketDataGateway) SubscribeMarkets(markets []gateway.Market) error {
	batchesOf := 50
	batches := make([][]gateway.Market, ((len(markets)-1)/batchesOf)+1)

	log.Printf("Binance subscribing to %d markets, will need %d websocket connections, maximum of %d markets on each websocket.", len(markets), len(batches), batchesOf)

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

type binanceDepthEvent struct {
	Type          string     `json:"e"`
	Time          int64      `json:"E"`
	Symbol        string     `json:"s"`
	FirstUpdateID int64      `json:"U"`
	LastUpdateID  int64      `json:"u"`
	BidDelta      []APIPrice `json:"b"`
	AskDelta      []APIPrice `json:"a"`
}

type binanceTradeEvent struct {
	Type          string  `json:"e"`
	Time          int64   `json:"E"`
	Symbol        string  `json:"s"`
	TradeId       int64   `json:"t"`
	Price         float64 `json:"p,string"`
	Quantity      float64 `json:"q,string"`
	BuyerOrderId  int64   `json:"b"`
	SellerOrderId int64   `json:"a"`
	TradeTime     int64   `json:"T"`
	BuyerMaker    bool    `json:"m"`
	// Ignore the "M" field. Not sure what the hell this is.
	// Don't remove this, or it will be parsed as the "m" field
	// for some reason. Seems like go's json.Unmarshal is not case sensitive.
	M bool `json:"M"`
}

type binanceDepthQueue struct {
	ReceivedSnapshot bool
	Queue            []binanceDepthEvent
	Lock             *sync.Mutex
}

func (g *MarketDataGateway) subscribeMarketData(markets []gateway.Market) error {
	depthQueue := make(map[string]binanceDepthQueue)
	streams := make([]string, 0)
	marketsMap := make(map[string]gateway.Market)

	for _, market := range markets {
		streams = append(streams, fmt.Sprintf("%s@depth@100ms", strings.ToLower(market.Symbol)))
		streams = append(streams, fmt.Sprintf("%s@trade", strings.ToLower(market.Symbol)))
		marketsMap[market.Symbol] = market
		depthQueue[market.Symbol] = binanceDepthQueue{
			Queue: make([]binanceDepthEvent, 0),
			Lock:  &sync.Mutex{},
		}
	}

	dialer := websocket.Dialer{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	host := "stream.binance.com:9443"
	if g.futuresApi {
		host = "fstream.binance.com"
	}

	url := fmt.Sprintf("wss://%s/stream?streams=%s", host, strings.Join(streams, "/"))
	ws, _, err := dialer.Dial(url, nil)
	if err != nil {
		return err
	}

	go func() {
		for {
			_, data, err := ws.ReadMessage()
			if err != nil {
				err = fmt.Errorf("Binance market data read message err: %s", err)
				panic(err)
			}

			message := binanceStreamMessage{}
			err = json.Unmarshal(data, &message)
			if err != nil {
				log.Printf("Binance market data unmarshal err: %s", err)
				continue
			}

			// Process ws event message
			slices := strings.Split(message.Stream, "@")
			if len(slices) >= 2 {
				symbol := strings.ToUpper(slices[0])
				_type := slices[1]
				market, ok := marketsMap[symbol]
				if !ok {
					log.Printf("Binance stream %s unable to find market for this stream in marketsMap", message.Stream)
					continue
				}

				if _type == "trade" {
					var tradeEvent binanceTradeEvent
					err = json.Unmarshal(message.Data, &tradeEvent)
					if err != nil {
						log.Printf("Binance failed to unmarshal trade event, err: %s", err)
					}

					g.processTradeEvent(market, tradeEvent)
				} else if _type == "depth" {
					var depthEvent binanceDepthEvent
					err = json.Unmarshal(message.Data, &depthEvent)
					if err != nil {
						log.Printf("Binance failed to unmarshal depth event, err: %s", err)
					}

					queue, _ := depthQueue[symbol]
					queue.Lock.Lock()

					if queue.ReceivedSnapshot {
						// Sanity check
						if queue.Queue != nil && len(queue.Queue) > 0 {
							err = fmt.Errorf("Binance depth stream %s already received snapshot, expected queue to be empty", symbol)
							panic(err)
						}

						g.processDepthEvent(market, depthEvent, false)
					} else {
						queue.Queue = append(queue.Queue, depthEvent)
						depthQueue[symbol] = queue
					}

					queue.Lock.Unlock()
				}
			} else {
				log.Printf("Binance received unprocessable message on stream: \"%s\"", message.Stream)
			}
		}
	}()

	// Wait 1 second before starting to fetch order book snapshots
	// This will allow us to start queing the initial order book partial updates
	time.Sleep(1 * time.Second)

	// Get initial oder book snapshots
	for _, market := range markets {
		snapshot, err := g.getDepthSnapshot(market.Symbol)
		if err != nil {
			ws.Close()
			return err
		}

		queue := depthQueue[market.Symbol]
		queue.Lock.Lock()

		g.processDepthEvent(market, binanceDepthEvent{
			BidDelta: snapshot.Bids,
			AskDelta: snapshot.Asks,
		}, true)

		i := 0
		for _, depthEvent := range queue.Queue {
			if depthEvent.LastUpdateID > snapshot.LastUpdateID {
				i += 1
				g.processDepthEvent(market, depthEvent, false)
			}
		}

		if i > 0 {
			log.Printf("Binance market %s processed %d queued depth events after fetching OB snapshot", market.Symbol, i)
		}

		queue.ReceivedSnapshot = true
		queue.Queue = nil
		depthQueue[market.Symbol] = queue
		queue.Lock.Unlock()
	}

	return nil
}

func (g *MarketDataGateway) getDepthSnapshot(symbol string) (res APIDepth, err error) {
	params := url.Values{}
	params.Set("symbol", symbol)
	params.Set("limit", "100")

	var getDepth = g.api.Depth
	if g.futuresApi {
		getDepth = g.api.FDepth
	}

	return getDepth(params)
}

func (g *MarketDataGateway) processDepthEvent(market gateway.Market, depth binanceDepthEvent, snapshot bool) {
	eventLog := make([]gateway.Event, 0)

	if snapshot {
		eventLog = append(eventLog, gateway.Event{
			Type: gateway.SnapshotSequenceEvent,
			Data: gateway.SnapshotSequence{
				Symbol: market.Symbol,
			},
		})
	}

	for _, ask := range depth.AskDelta {
		eventLog = append(eventLog, gateway.Event{
			Type: gateway.DepthEvent,
			Data: gateway.Depth{
				Symbol: market.Symbol,
				Side:   gateway.Ask,
				Price:  ask.Price,
				Amount: ask.Amount,
			},
		})
	}
	for _, bid := range depth.BidDelta {
		eventLog = append(eventLog, gateway.Event{
			Type: gateway.DepthEvent,
			Data: gateway.Depth{
				Symbol: market.Symbol,
				Side:   gateway.Bid,
				Price:  bid.Price,
				Amount: bid.Amount,
			},
		})
	}

	g.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          eventLog,
	}
}

func (g *MarketDataGateway) processTradeEvent(market gateway.Market, trade binanceTradeEvent) {
	var direction gateway.Side
	if trade.BuyerMaker {
		direction = gateway.Ask
	} else {
		direction = gateway.Bid
	}

	g.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog: []gateway.Event{
			gateway.Event{
				Type: gateway.TradeEvent,
				Data: gateway.Trade{
					ID:        strconv.FormatInt(trade.TradeId, 10),
					Symbol:    trade.Symbol,
					Direction: direction,
					Price:     trade.Price,
					Amount:    trade.Quantity,
				},
			},
		},
	}
}
