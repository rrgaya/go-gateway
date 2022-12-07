package novadax

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	gosocketio "github.com/graarh/golang-socketio"
	"github.com/graarh/golang-socketio/transport"
	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/utils"
)

// https://doc.novadax.com/pt-BR/#websocket
var WsURL = "wss://api.novadax.com/socket.io/?EIO=3&transport=websocket"

type WsSession struct {
	markets []gateway.Market
	tickCh  chan gateway.Tick
}

type wsOrderBookResponse struct {
	Bids []gateway.PriceArray `json:"bids"`
	Asks []gateway.PriceArray `json:"asks"`
}

type wsTrade struct {
	Price     float64 `json:"price,string"`
	Amount    float64 `json:"amount,string"`
	Side      string  `json:"side"`
	Timestamp int64   `json:"timestamp"`
}

func NewWsSession(tickCh chan gateway.Tick) *WsSession {
	return &WsSession{
		tickCh: tickCh,
	}
}

func (s *WsSession) Connect(market gateway.Market) error {
	conn, err := gosocketio.Dial(
		WsURL,
		transport.GetDefaultWebsocketTransport(),
	)
	if err != nil {
		wsError := utils.NewWsError(
			Exchange.Name,
			utils.WsRequestError,
			err.Error(),
			"",
		)

		return wsError.AsError()
	}

	go s.handleConnection(conn, market)

	return nil
}

func (s *WsSession) handleConnection(conn *gosocketio.Client, market gateway.Market) {
	conn.On(gosocketio.OnDisconnection, func(h *gosocketio.Channel) {
		log.Printf("%s ws disconnected from mkt [%s], reconnecting in 5 seconds...", Exchange.Name, market.Symbol)

		s.Connect(market)
	})

	conn.On(gosocketio.OnConnection, func(h *gosocketio.Channel) {
		log.Printf("%s connected to mkt [%s].", Exchange.Name, market.Symbol)
	})

	orderBookWsChannel := fmt.Sprintf("MARKET.%s.DEPTH.LEVEL0", market.Symbol)
	tradesWsChannel := fmt.Sprintf("MARKET.%s.TRADE", market.Symbol)

	conn.On(orderBookWsChannel, func(h *gosocketio.Channel, msg json.RawMessage) {
		book := wsOrderBookResponse{}

		err := json.Unmarshal(msg, &book)
		if err != nil {
			wsError := utils.NewWsError(
				Exchange.Name,
				utils.WsUnmarshalError,
				err.Error(),
				"",
			)

			log.Println(wsError.AsError())
		}

		s.handleOrderBookUpdates(book, market)
	})

	conn.On(tradesWsChannel, func(h *gosocketio.Channel, msg json.RawMessage) {
		trades := make([]wsTrade, 0)

		err := json.Unmarshal(msg, &trades)
		if err != nil {
			wsError := utils.NewWsError(
				Exchange.Name,
				utils.WsUnmarshalError,
				err.Error(),
				"",
			)

			log.Println(wsError.AsError())
		}

		s.handleTradeUpdates(trades, market)
	})

	subMsg := []string{orderBookWsChannel, tradesWsChannel}

	if _, err := conn.Ack("SUBSCRIBE", subMsg, time.Second*5); err != nil {
		wsError := utils.NewWsError(
			Exchange.Name,
			utils.WsConnectionError,
			err.Error(),
			fmt.Sprint(subMsg),
		)

		log.Println(wsError.AsError())
	}
}

func (s *WsSession) handleOrderBookUpdates(book wsOrderBookResponse, market gateway.Market) {
	events := make([]gateway.Event, 0, len(book.Bids)+len(book.Asks))

	events = AppendBookEvent(
		book.Bids,
		market.Symbol,
		gateway.Bid,
		events,
	)

	events = AppendBookEvent(
		book.Asks,
		market.Symbol,
		gateway.Ask,
		events,
	)

	s.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          events,
	}
}

func (s *WsSession) handleTradeUpdates(trades []wsTrade, market gateway.Market) {
	events := make([]gateway.Event, 0, len(trades))

	for _, trade := range trades {
		var direction gateway.Side
		if trade.Side == "BUY" {
			direction = gateway.Bid
		} else {
			direction = gateway.Ask
		}

		event := gateway.Event{
			Type: gateway.TradeEvent,
			Data: gateway.Trade{
				Timestamp: time.Unix(0, trade.Timestamp*int64(time.Millisecond)),
				Symbol:    market.Symbol,
				Direction: direction,
				Amount:    trade.Amount,
				Price:     trade.Price,
			},
		}

		events = append(events, event)
	}

	s.tickCh <- gateway.Tick{
		ReceivedTimestamp: time.Now(),
		EventLog:          events,
	}
}

func AppendBookEvent(
	orders []gateway.PriceArray,
	symbol string,
	side gateway.Side,
	events []gateway.Event,
) []gateway.Event {
	for _, order := range orders {
		event := gateway.Event{
			Type: gateway.DepthEvent,
			Data: gateway.Depth{
				Symbol: symbol,
				Side:   side,
				Price:  order.Price,
				Amount: order.Amount,
			},
		}

		events = append(events, event)
	}

	return events
}
