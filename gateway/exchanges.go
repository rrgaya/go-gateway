package gateway

import (
	"strings"
)

type Exchange struct {
	Name string `json:"name"`
}

func (e Exchange) String() string {
	return e.Name
}

func (e Exchange) ID() string {
	return e.Symbol()
}

func (e Exchange) Symbol() string {
	return exchangeToSymbol(e.Name)
}

var exchangeSymReplacer = strings.NewReplacer(".", "", "-", "", "_", "")

func exchangeToSymbol(str string) string {
	str = strings.ToLower(str)
	str = exchangeSymReplacer.Replace(str)
	return str
}

type NewGateway func(Options) Gateway

var Exchanges = make([]Exchange, 0)
var exchangeMap = make(map[Exchange]NewGateway)

func RegisterExchange(exg Exchange, newGtw NewGateway) {
	_, ok := exchangeMap[exg]
	if !ok {
		Exchanges = append(Exchanges, exg)
	}

	exchangeMap[exg] = newGtw
}

func NewByExchange(exg Exchange, options Options) (Gateway, bool) {
	newGtw, ok := exchangeMap[exg]
	if ok {
		return newGtw(options), true
	}

	return nil, false
}

func ExchangeBySymbol(sym string) (Exchange, bool) {
	for _, exg := range Exchanges {
		if exg.Symbol() == exchangeToSymbol(sym) {
			return exg, true
		}
	}

	return Exchange{}, false
}
