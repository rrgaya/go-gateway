package bittrex

import (
	"fmt"
	"math"
	"strings"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/toorop/go-bittrex"
)

var Exchange = gateway.Exchange{
	Name: "Bittrex",
}

type Gateway struct {
	gateway.BaseGateway
	options            gateway.Options
	marketsInitialized bool
	markets            []gateway.Market
	accountGateway     *AccountGateway
	marketDataGateway  *MarketDataGateway
	tickCh             chan gateway.Tick
	api                *bittrex.Bittrex
}

func NewGateway(options gateway.Options) gateway.Gateway {
	return &Gateway{
		options: options,
		tickCh:  make(chan gateway.Tick, 100),
		api:     bittrex.New(options.ApiKey, options.ApiSecret),
	}
}

func (g *Gateway) Connect() error {
	err := g.loadMarkets()
	if err != nil {
		return fmt.Errorf("Failed to load markets, err %s", err)
	}

	g.accountGateway = NewAccountGateway(g.options, g.api, g.tickCh)
	if g.options.ApiKey != "" {
		err = g.accountGateway.Connect()
		if err != nil {
			return fmt.Errorf("Failed to connect to account gateway, err %s", err)
		}
	}

	// Init market data
	g.marketDataGateway = NewMarketDataGateway(g.options, g.tickCh)
	err = g.marketDataGateway.Connect()
	if err != nil {
		return err
	}

	return nil
}

func (g *Gateway) SubscribeMarkets(markets []gateway.Market) error {
	return g.marketDataGateway.SubscribeMarkets(markets)
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
	res, err := g.api.GetMarkets()
	if err != nil {
		return err
	}

	g.marketsInitialized = true
	g.markets = symbolsToCommonMarket(res)

	return nil
}

func symbolsToCommonMarket(markets []bittrex.MarketV3) []gateway.Market {
	commonMarkets := make([]gateway.Market, 0, len(markets))

	for _, market := range markets {
		market := symbolToCommonMarket(market)
		commonMarkets = append(commonMarkets, market)
	}

	return commonMarkets
}

func symbolToCommonMarket(market bittrex.MarketV3) gateway.Market {
	minimumOrderSize, _ := market.MinTradeSize.Float64()

	return gateway.Market{
		Exchange: Exchange,
		Symbol:   market.Symbol,
		Pair: gateway.Pair{
			Base:  strings.ToUpper(market.BaseCurrencySymbol),
			Quote: strings.ToUpper(market.QuoteCurrencySymbol),
		},
		PriceTick:        1 / math.Pow10(int(market.Precision)),
		AmountTick:       0.00000001,
		MinimumOrderSize: minimumOrderSize,
	}
}

func (g *Gateway) AccountGateway() gateway.AccountGateway {
	return g.accountGateway
}

func (g *Gateway) Tick() chan gateway.Tick {
	return g.tickCh
}
