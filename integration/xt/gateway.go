package xt

import (
	"fmt"
	"math"
	"strings"

	"github.com/herenow/atomic-gtw/gateway"
)

var Exchange = gateway.Exchange{
	Name: "XT",
}

type Gateway struct {
	gateway.BaseGateway
	options            gateway.Options
	marketsInitialized bool
	markets            []gateway.Market
	accountGateway     *AccountGateway
	marketDataGateway  *MarketDataGateway
	api                *API
	tickCh             chan gateway.Tick
}

func NewGateway(options gateway.Options) gateway.Gateway {
	return &Gateway{
		options: options,
		api:     NewAPI(options),
		tickCh:  make(chan gateway.Tick, 100),
	}
}

func (g *Gateway) Connect() error {
	err := g.loadMarkets()
	if err != nil {
		return fmt.Errorf("load markets: %s", err)
	}

	g.accountGateway = NewAccountGateway(g.api, g.options, g.tickCh)
	err = g.accountGateway.Connect()
	if err != nil {
		return fmt.Errorf("account gtw connect: %s", err)
	}

	// Init market data
	g.marketDataGateway = NewMarketDataGateway(g.api, g.options, g.tickCh)

	return nil
}

func (g *Gateway) SubscribeMarkets(markets []gateway.Market) error {
	err := g.marketDataGateway.SubscribeMarkets(markets)
	if err != nil {
		return fmt.Errorf("failed to subscribe market data, err: %s", err)
	}

	return nil
}

func (g *Gateway) Close() error {
	return nil
}

func (g *Gateway) Exchange() gateway.Exchange {
	return Exchange
}

func (g *Gateway) Markets() []gateway.Market {
	if g.marketsInitialized == false {
		_ = g.loadMarkets()
	}

	return g.markets
}

func (g *Gateway) loadMarkets() error {
	res, err := g.api.GetSymbols()
	if err != nil {
		return err
	}

	g.marketsInitialized = true
	g.markets = marketsToCommonMarket(res)

	return nil
}

func marketsToCommonMarket(res APISymbolRes) []gateway.Market {
	commonMarkets := make([]gateway.Market, 0, len(res.Symbols))

	for _, symbol := range res.Symbols {
		mkt, err := marketToCommonMarket(symbol)
		if err == nil {
			commonMarkets = append(commonMarkets, mkt)
		}
	}

	return commonMarkets
}

func marketToCommonMarket(symbol APISymbol) (mkt gateway.Market, err error) {
	priceTick := 1 / math.Pow10(int(symbol.PricePrecision))
	amountTick := 1 / math.Pow10(int(symbol.QuantityPrecision))

	minMoneyValue := 0.0
	for _, filter := range symbol.Filters {
		if filter.Filter == "QUOTE_QTY" {
			minMoneyValue = filter.Min
		}
	}

	return gateway.Market{
		Exchange: Exchange,
		Pair: gateway.Pair{
			Base:  strings.ToUpper(symbol.BaseCurrency),
			Quote: strings.ToUpper(symbol.QuoteCurrency),
		},
		Symbol:                 symbol.Symbol,
		PriceTick:              priceTick,
		AmountTick:             amountTick,
		MinimumOrderMoneyValue: minMoneyValue,
	}, nil
}

func (g *Gateway) AccountGateway() gateway.AccountGateway {
	return g.accountGateway
}

func (g *Gateway) Tick() chan gateway.Tick {
	return g.tickCh
}
