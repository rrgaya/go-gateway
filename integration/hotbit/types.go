package hotbit

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/herenow/atomic-gtw/gateway"
)

type Market struct {
	Id                 int64   `json:"id"`
	Count1Precision    int64   `json:"count1"`
	Count2Precision    int64   `json:"count2"`
	MinBuyPrice        float64 `json:"minBuyPrice,string"`
	MinBuyCount        float64 `json:"minBuyCount,string"`
	MinBuyAmount       float64 `json:"minBuyAmount,string"`
	MaxBuyPrice        float64 `json:"maxBuyPrice,string"`
	MaxBuyCount        float64 `json:"maxBuyCount,string"`
	MaxBuyAmount       float64 `json:"maxBuyAmount,string"`
	Name               string  `json:"name"`
	QuoteCoinPrecision int64   `json:"prec1"`
	BaseCoinPrecision  int64   `json:"prec2"`
	BaseCoinName       string  `json:"coin1Name"`
	QuoteCoinName      string  `json:"coin2Name"`
	IsOpenTrade        bool    `json:"isOpenTrade"`
}

func (m *Market) Base() string {
	s := strings.Split(m.Name, "/")
	if len(s) < 2 {
		return ""
	} else {
		return s[0]
	}
}

func (m *Market) Quote() string {
	s := strings.Split(m.Name, "/")
	if len(s) < 2 {
		return ""
	} else {
		return s[1]
	}
}

func (m *Market) Symbol() string {
	return strings.Replace(m.Name, "/", "", 1) // Remove slash dividing base symbol from quote symbol
}

type Order struct {
	Id        int64   `json:"id"`
	Market    string  `json:"market"`
	Source    string  `json:"source"`
	Type      int64   `json:"type"`
	Side      int64   `json:"side"`
	User      int64   `json:"user"`
	Ctime     float64 `json:"ctime"`
	Mtime     float64 `json:"mtime"`
	Price     float64 `json:"price,string"`
	Amount    float64 `json:"amount,string"`
	TakerFee  float64 `json:"taker_fee,string"`
	MakerFee  float64 `json:"maker_fee,string"`
	Left      float64 `json:"left,string"`
	DealStock float64 `json:"deal_stock,string"`
	DealMoney float64 `json:"deal_money,string"`
	DealFee   float64 `json:"deal_fee,string"`
}

type DepthUpdate struct {
	Asks []gateway.PriceArray `json:"asks"`
	Bids []gateway.PriceArray `json:"bids"`
}

type Deal struct {
	Id        int64   `json:"id"`
	Timestamp float64 `json:"time"`
	Price     float64 `json:"price,string"`
	Amount    float64 `json:"amount,string"`
	Type      string  `json:"string"`
}

func (d *Deal) Time() time.Time {
	sec, dec := math.Modf(d.Timestamp)
	return time.Unix(int64(sec), int64(dec*(1e9)))
}

type AssetBalance struct {
	Available float64 `json:"available,string"`
	Freeze    float64 `json:"freeze,string"`
}

type OrderQuery struct {
	Limit   int
	Offset  int
	Total   int
	Records []Order
}

type HotbitWsRequest struct {
	Method string      `json:"method"`
	Params interface{} `json:"params"`
	Id     int64       `json:"id"`
}

type HotbitWsMessage struct {
	Method string          `json:"method"`
	Error  string          `json:"error"`
	Id     int64           `json:"id"`
	Params json.RawMessage `json:"params"`
	Result json.RawMessage `json:"result"`
}

func NewHotbitDepthSubscribeParams(symbol string, maxDepth int64, priceTick float64) [3]interface{} {
	var params [3]interface{}

	params[0] = symbol
	params[1] = maxDepth
	params[2] = strconv.FormatFloat(priceTick, 'f', -1, 64)

	return params
}

type HotbitDepthUpdate struct {
	Asks []gateway.PriceArray `json:"asks"`
	Bids []gateway.PriceArray `json:"bids"`
}

type HotbitDeal struct {
	Id        int64   `json:"id"`
	Timestamp float64 `json:"time"`
	Price     float64 `json:"price,string"`
	Amount    float64 `json:"amount,string"`
	Type      string  `json:"string"`
}

func (d *HotbitDeal) Time() time.Time {
	sec, dec := math.Modf(d.Timestamp)
	return time.Unix(int64(sec), int64(dec*(1e9)))
}
