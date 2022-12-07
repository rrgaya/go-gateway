package gateway

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/herenow/atomic-gtw/book"
	"github.com/herenow/atomic-gtw/utils"
)

type Market struct {
	Exchange               Exchange `json:"exchange"`
	Pair                   Pair     `json:"pair"`
	Symbol                 string   `json:"symbol"`
	TakerFee               float64  `json:"taker_fee"`
	MakerFee               float64  `json:"maker_fee"`
	PriceTick              float64  `json:"price_tick"`
	AmountTick             float64  `json:"amount_tick"`
	MinimumOrderSize       float64  `json:"minimum_order_size"`
	MinimumOrderMoneyValue float64  `json:"minimum_order_money_value"`
	FuturesContract        bool     `json:"futures_contract"`
	FuturesMarginAsset     string   `json:"futures_margin_asset"`
	// Pays fees in base currency instead of the quote currency.
	// This is not a standard behavior, most markets charge the fee in
	// the quote currency not the base currency.
	PaysFeeInStock bool `json:"pays_fee_in_stock"`
	// Legacy attributes, TODO: stop depending on this
	Book *book.Book `json:"-"`
}

func (m Market) ID() string {
	return m.Exchange.Name + ":" + m.Symbol
}

func (m Market) String() string {
	return m.ID()
}

type Balance struct {
	Asset     string  `json:"asset"`
	Available float64 `json:"available"`
	Total     float64 `json:"total"`
}

type Pair struct {
	Base  string `json:"base"`
	Quote string `json:"quote"`
}

func (p Pair) String() string {
	return p.Base + "/" + p.Quote
}

type OrderType string

const (
	LimitOrder  OrderType = "limit"
	MarketOrder OrderType = "market"
)

type Side string

const (
	Bid Side = "bid"
	Ask Side = "ask"
)

type OrderState string

const (
	OrderSent            OrderState = "sent"
	OrderOpen            OrderState = "open"
	OrderCancelled       OrderState = "cancelled"
	OrderPartiallyFilled OrderState = "partially_filled"
	OrderClosed          OrderState = "closed"
	OrderFullyFilled     OrderState = "fully_filled"
	OrderUnknown         OrderState = "unknown"
)

type Order struct {
	ID               string     `json:"id"`
	ClientOrderID    string     `json:"client_order_id"`
	Market           Market     `json:"market"`
	State            OrderState `json:"state"`
	Type             OrderType  `json:"type"`
	Side             Side       `json:"side"`
	Price            float64    `json:"price"`
	AvgPrice         float64    `json:"avg_price"`
	Amount           float64    `json:"amount"`
	FilledAmount     float64    `json:"filled_amount"`
	FilledMoneyValue float64    `json:"filled_money_value"`
	Fee              float64    `json:"fee"`
	FeeAsset         string     `json:"fee_asset"`
	PostOnly         bool       `json:"post_only"`
	Error            error      `json:"error"`
	NewClientOrderID string     `json:"new_client_order_id"` // For order replace requests
	RemainingAmount  float64    `json:"remaining_amount"`    // Set remaining amount for replacement order
	Tags             []string   `json:"tags"`                // Useful tags when persisting the order to the DB
}

type Execution struct {
	Time    time.Time `json:"time"`
	Market  Market    `json:"market"`
	OrderID string    `json:"order_id"`
	Side    Side      `json:"side"`
	Amount  float64   `json:"amount"`
	Price   float64   `json:"price"`
	Tags    []string  `json:"tags"` // Useful tags when persisting the order to the DB
}

func (o *Order) LeftAmount() float64 {
	return o.Amount - o.FilledAmount
}

func (o Order) String() string {
	return fmt.Sprintf(
		"%s:%s:%s:%s:%s:%s@%s",
		o.Market.String(),
		o.ID,
		o.Type,
		o.Side,
		o.State,
		utils.FloatToStringWithTick(o.Amount, o.Market.AmountTick),
		utils.FloatToStringWithTick(o.Price, o.Market.PriceTick),
	)
}

type PriceArray struct {
	Price  float64
	Amount float64
}

func (p *PriceArray) UnmarshalJSON(b []byte) error {
	var data [2]interface{}
	if err := json.Unmarshal(b, &data); err != nil {
		return err
	}

	price, err := priceArrayParseFloat(data[0])
	if err != nil {
		return err
	}
	amount, err := priceArrayParseFloat(data[1])
	if err != nil {
		return err
	}

	p.Price = price
	p.Amount = amount

	return nil
}

func priceArrayParseFloat(data interface{}) (float64, error) {
	switch v := data.(type) {
	case string:
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, err
		}
		return f, nil
	case int64:
		return float64(v), nil
	case float64:
		return v, nil
	}

	return 0, errors.New(fmt.Sprintf("Failed to type cast %+v", data))
}

type PriceLevel struct {
	Price  float64 `json:"price"`
	Amount float64 `json:"amount"`
}
