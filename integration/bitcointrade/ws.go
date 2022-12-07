package bitcointrade

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/herenow/atomic-gtw/gateway"
)

const (
	WsURL = "wss://core-base.bitcointrade.com.br/socket.io/?EIO=3&transport=websocket"
)

type WsSession struct {
	conn              *websocket.Conn
	connWriterMutex   *sync.Mutex
	quit              chan bool
	pingTimer         chan bool
	waitingPong       bool
	accountGateway    *AccountGateway
	marketDataGateway *MarketDataGateway
}

func NewWsSession() *WsSession {
	return &WsSession{
		quit:      make(chan bool),
		pingTimer: make(chan bool),
	}
}

func (w *WsSession) Connect() error {
	dialer := websocket.Dialer{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	ws, _, err := dialer.Dial(WsURL, nil)
	if err != nil {
		return err
	}

	w.conn = ws
	w.connWriterMutex = &sync.Mutex{}

	go w.websocketPinger()
	go w.messageHandler()

	return nil
}

func (w *WsSession) Close() {
	close(w.quit)
	w.conn.Close()
}

type WSPair struct {
	Code  string `json:"code"`
	Base  string `json:"base"`
	Quote string `json:"quote"`
}

type WSOrderCompleted struct {
	Pair                 WSPair    `json:"pair"`
	CreateDate           time.Time `json:"create_date"`
	Type                 int64     `json:"type"`
	Amount               float64   `json:"amount"`
	UnitPrice            float64   `json:"unit_price"`
	ActiveOrderCode      string    `json:"active_order_code"`
	ActiveOrderUserCode  string    `json:"active_order_user_code"`
	PassiveOrderCode     string    `json:"passive_order_code"`
	PassiveOrderUserCode string    `json:"passive_order_user_code"`
}

type WSOrder struct {
	ID         string  `json:"id"`
	Code       string  `json:"code"`
	UserCode   string  `json:"user_code"`
	Pair       WSPair  `json:"pair"`
	Amount     float64 `json:"amount"`
	TotalPrice float64 `json:"total_price"`
	Type       int64   `json:"type"`
	UnitPrice  float64 `json:"unit_price"`
}

func (w *WsSession) SetAccountGateway(accGtw *AccountGateway) {
	w.accountGateway = accGtw
}

func (w *WsSession) SetMarketDataGateway(mktDtGtw *MarketDataGateway) {
	w.marketDataGateway = mktDtGtw
}

func (w *WsSession) messageHandler() {
	for {
		_, data, err := w.conn.ReadMessage()
		if err != nil {
			// Reconnect
			log.Printf("Ws failed to read from ws (%s), reconnecting in 5 seconds...", err)
			w.Close()
			time.Sleep(5 * time.Second)
			err = w.Connect()
			if err != nil {
				panic(fmt.Errorf("Ws failed to reconnect, err: %s", err))
			}
			return
		}

		// Check if this is a market data type of message
		if len(data) > 3 && bytes.Compare(data[0:2], []byte("42")) == 0 {
			data = data[2:]
			dataParts := [2]json.RawMessage{}
			err = json.Unmarshal(data, &dataParts)
			if err != nil {
				log.Printf("Ws failed to unmarshal data (%s) err (%s)", string(data), err)
				continue
			}

			var topic string
			err = json.Unmarshal(dataParts[0], &topic)
			if err != nil {
				log.Printf("Ws failed to unmarshal dataParts[0] into topic (%s) err (%s)", string(dataParts[0]), err)
				continue
			}

			if topic == "order" || topic == "cancel_order" {
				var update WSOrder
				err = json.Unmarshal(dataParts[1], &update)
				if err != nil {
					log.Printf("Ws failed to unmarshal ws order topic (%s) data (%s) err (%s)", topic, string(dataParts[0]), err)
					continue
				}

				if w.accountGateway != nil {
					w.accountGateway.ProcessOrderUpdate(topic, update)
				}
				if w.marketDataGateway != nil {
					w.marketDataGateway.ProcessOrderUpdate(topic, update)
				}
			} else if topic == "order_completed" {
				var update WSOrderCompleted
				err = json.Unmarshal(dataParts[1], &update)
				if err != nil {
					log.Printf("Ws failed to unmarshal ws order topic (%s) data (%s) err (%s)", topic, string(dataParts[0]), err)
					continue
				}

				if w.accountGateway != nil {
					w.accountGateway.ProcessOrderCompleted(topic, update)
				}
				if w.marketDataGateway != nil {
					w.marketDataGateway.ProcessOrderCompleted(topic, update)
				}

			}
		}
	}
}

func (w *WsSession) websocketPinger() {
	for {
		select {
		case <-time.After(5 * time.Second):
			err := w.WriteMessage([]byte("2"))
			if err != nil {
				log.Printf("WS failed to send ping, err: %s", err)
				w.Close()
				return
			}
			w.waitingPong = true
		case <-w.quit:
			return
		}
	}
}

func (w *WsSession) WriteMessage(data []byte) error {
	w.connWriterMutex.Lock()
	err := w.conn.WriteMessage(websocket.TextMessage, data)
	w.connWriterMutex.Unlock()
	if err != nil {
		return err
	}

	return nil
}

func wsTypeToSide(t int64) gateway.Side {
	if t == 1 {
		return gateway.Bid
	}
	return gateway.Ask
}

func wsPairToMarket(p WSPair) gateway.Market {
	return gateway.Market{
		Symbol: p.Code,
		Pair: gateway.Pair{
			Base:  p.Base,
			Quote: p.Quote,
		},
	}
}
