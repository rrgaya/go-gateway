package gateway

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type GatewayState string

const (
	GatewayConnected    GatewayState = "connected"
	GatewayDisconnected GatewayState = "disconnected"
	GatewayReconnecting GatewayState = "reconnecting"
	GatewayLoaded       GatewayState = "loaded"
	GatewayFailed       GatewayState = "failed"
)

var (
	InsufficientBalanceErr error = errors.New("Insufficient Balance on Exchange")
	MinOrderSizeErr        error = errors.New("Order size is too small")
)

type Gateway interface {
	Exchange() Exchange
	Markets() []Market
	AccountGateway() AccountGateway
	Connect() error
	Close() error
	SubscribeMarkets(markets []Market) error
	Tick() chan Tick
}

type AccountGateway interface {
	Balances() ([]Balance, error)
	OpenOrders(Market) ([]Order, error)
	SendOrder(Order) (orderId string, err error)
	ReplaceOrder(Order) (Order, error)
	CancelOrder(Order) error
}

type Tick struct {
	Sequence          int64
	ServerTimestamp   time.Time
	ReceivedTimestamp time.Time
	EventLog          []Event
}

func (t Tick) String() string {
	lines := make([]string, len(t.EventLog))
	for i, event := range t.EventLog {
		line := event.Type.String()
		switch event.Type {
		case SnapshotSequenceEvent:
			snapshot := event.Data.(SnapshotSequence)
			line += fmt.Sprintf("\t%s", snapshot.Symbol)
		case DepthEvent:
			depth := event.Data.(Depth)
			line += fmt.Sprintf("\t%s", depth.Symbol)
			line += fmt.Sprintf("\t%s", depth.Side)
			line += fmt.Sprintf("\t%f", depth.Price)
			line += fmt.Sprintf("\t%f", depth.Amount)
		case TradeEvent:
			trade := event.Data.(Trade)
			line += fmt.Sprintf("\t%s", trade.Symbol)
			line += fmt.Sprintf("\t%s", trade.Direction)
			line += fmt.Sprintf("\t%f", trade.Price)
			line += fmt.Sprintf("\t%f", trade.Amount)
		case OkEvent:
			ok := event.Data.(Ok)
			line += fmt.Sprintf("\t%s", ok.Symbol)
			line += fmt.Sprintf("\t%s", ok.OrderID)
			line += fmt.Sprintf("\t%s", ok.ClientOrderID)
		case CancelEvent:
			cancel := event.Data.(Cancel)
			line += fmt.Sprintf("\t%s", cancel.Symbol)
			line += fmt.Sprintf("\t%s", cancel.OrderID)
			line += fmt.Sprintf("\t%s", cancel.ClientOrderID)
		case FillEvent:
			fill := event.Data.(Fill)
			line += fmt.Sprintf("\t%s", fill.ID)
			line += fmt.Sprintf("\t%s", fill.OrderID)
			line += fmt.Sprintf("\t%s", fill.Symbol)
			line += fmt.Sprintf("\t%s", fill.Side)
			line += fmt.Sprintf("\t%f", fill.Price)
			line += fmt.Sprintf("\t%f", fill.Amount)
		case OrderUpdateEvent:
			order := event.Data.(Order)
			line += fmt.Sprintf("\t%s", order.ID)
			line += fmt.Sprintf("\t%s", order.Market.Symbol)
			line += fmt.Sprintf("\t%s", order.Side)
			line += fmt.Sprintf("\t%s", order.State)
			line += fmt.Sprintf("\t%.12f", order.Price)
			line += fmt.Sprintf("\t%f", order.Amount)
			line += fmt.Sprintf("\t%f", order.FilledAmount)
			line += fmt.Sprintf("\t%f", order.AvgPrice)
		case BitmexInstrumentEvent:
			instr := event.Data.(BitmexInstrument)
			line += fmt.Sprintf("\t%f", instr.LastPrice)
			line += fmt.Sprintf("\t%s", instr.LastTickDirection)
			line += fmt.Sprintf("\t%f", instr.MarkPrice)
			line += fmt.Sprintf("\t%f", instr.FairPrice)
		}
		lines[i] = line
	}

	return fmt.Sprintf("\n%d\t%v\t%v\n%s", t.Sequence, t.ServerTimestamp, t.ReceivedTimestamp, strings.Join(lines, "\n"))
}

type EventType int8

const (
	SnapshotSequenceEvent EventType = 1  // Flags that we should clear the order book
	DepthEvent            EventType = 2  // Depth order book price level update
	RawOrderEvent         EventType = 3  // Raw order book order update
	TradeEvent            EventType = 4  // Market trade occured
	OkEvent               EventType = 5  // User order ok event
	CancelEvent           EventType = 6  // User order cancel event
	FillEvent             EventType = 7  // User order fill event
	OrderUpdateEvent      EventType = 8  // User order fill event
	BitmexInstrumentEvent EventType = 50 // Bitmex instrument update
)

func (e EventType) String() string {
	switch e {
	case SnapshotSequenceEvent:
		return "snapshot"
	case DepthEvent:
		return "depth"
	case RawOrderEvent:
		return "raw_order"
	case TradeEvent:
		return "trade"
	case OkEvent:
		return "ok"
	case CancelEvent:
		return "cancel"
	case FillEvent:
		return "fill"
	case OrderUpdateEvent:
		return "order_update"
	case BitmexInstrumentEvent:
		return "bitmex_instrument"
	}
	return strconv.Itoa(int(e))
}

type Event struct {
	Type EventType   `json:"type"`
	Data interface{} `json:"data"`
}

type SnapshotSequence struct {
	Symbol string `json:"symbol"`
}

type Depth struct {
	Symbol string  `json:"symbol"`
	Side   Side    `json:"side"`
	Amount float64 `json:"amount"`
	Price  float64 `json:"price"`
}

type RawOrder struct {
	Timestamp time.Time `json:"timestamp"`
	Symbol    string    `json:"symbol"`
	ID        string    `json:"id"`
	Side      Side      `json:"side"`
	Amount    float64   `json:"amount"`
	Price     float64   `json:"price"`
}

type Trade struct {
	Timestamp time.Time `json:"timestamp"`
	Symbol    string    `json:"symbol"`
	ID        string    `json:"id"`
	OrderID   string    `json:"order_id"`  // Matched against order on the book
	Direction Side      `json:"direction"` // Aggressor side
	Amount    float64   `json:"amount"`
	Price     float64   `json:"price"`
}

type Ok struct {
	Timestamp     time.Time `json:"timestamp"`
	Symbol        string    `json:"symbol"`
	OrderID       string    `json:"order_id"`
	ClientOrderID string    `json:"client_order_id"`
}

type Cancel struct {
	Timestamp     time.Time `json:"timestamp"`
	Symbol        string    `json:"symbol"`
	OrderID       string    `json:"order_id"`
	ClientOrderID string    `json:"client_order_id"`
}

type Fill struct {
	Timestamp     time.Time `json:"timestamp"`
	Symbol        string    `json:"symbol"`
	ID            string    `json:"id"`
	OrderID       string    `json:"order_id"`
	ClientOrderID string    `json:"client_order_id"`
	Side          Side      `json:"side"`
	Amount        float64   `json:"amount"`
	Price         float64   `json:"price"`
	FullyFilled   bool      `json:"fully_filled"`
	Taker         bool      `json:"taker"`
	Fee           float64   `json:"fee"`
	FeeAsset      string    `json:"fee_asset"`
}

const FeeInMoney string = "_money"
const FeeInStock string = "_stock"

type BitmexInstrument struct {
	Symbol                string    `json:"symbol"`
	LastPrice             float64   `json:"lastPrice"`
	LastTickDirection     string    `json:"lastTickDirection"`
	MarkPrice             float64   `json:"markPrice"`
	LastChangePcnt        float64   `json:"lastChangePcnt"`
	OpenValue             float64   `json:"openValue"`
	IndicativeSettlePrice float64   `json:"indicativeSettlePrice"`
	FairBasis             float64   `json:"fairBasis"`
	FairPrice             float64   `json:"fairPrice"`
	LastPriceProtected    float64   `json:"lastPriceProtected"`
	ImpactMidPrice        float64   `json:"impactMidPrice"`
	ImpactBidPrice        float64   `json:"impactBidPrice"`
	ImpactAskPrice        float64   `json:"impactAskPrice"`
	BidPrice              float64   `json:"bidPrice"`
	MiPrice               float64   `json:"midPrice"`
	AskPrice              float64   `json:"askPrice"`
	Timestamp             time.Time `json:"timestamp"`
}
