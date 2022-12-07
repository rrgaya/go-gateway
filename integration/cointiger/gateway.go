package cointiger

import (
	"fmt"
	"math"
	"strings"

	"github.com/herenow/atomic-gtw/gateway"
)

var Exchange = gateway.Exchange{
	Name: "CoinTiger",
}

type Gateway struct {
	gateway.BaseGateway
	options            gateway.Options
	marketsInitialized bool
	markets            []gateway.Market
	accountGateway     *AccountGateway
	marketDataGateway  *MarketDataGateway
	tickCh             chan gateway.Tick
	api                *API
}

func NewGateway(options gateway.Options) gateway.Gateway {
	return &Gateway{
		options: options,
		tickCh:  make(chan gateway.Tick, 100),
		api:     NewAPI(options),
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
	g.marketDataGateway = NewMarketDataGateway(g.options, g.Markets(), g.api, g.tickCh)
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
	res, err := g.api.Currencies()
	if err != nil {
		return err
	}

	commonMarkets := make([]gateway.Market, 0)
	for _, pairs := range res {
		for _, pair := range pairs {
			commonMarkets = append(commonMarkets, gateway.Market{
				Exchange: Exchange,
				Symbol:   fmt.Sprintf("%s%s", pair.BaseCurrency, pair.QuoteCurrency),
				Pair: gateway.Pair{
					Base:  strings.ToUpper(pair.BaseCurrency),
					Quote: strings.ToUpper(pair.QuoteCurrency),
				},
				PriceTick:              1 / math.Pow10(int(pair.PricePrecision)),
				AmountTick:             1 / math.Pow10(int(pair.AmountPrecision)),
				MinimumOrderSize:       pair.AmountMin,
				MinimumOrderMoneyValue: pair.MinTurnover,
			})
		}
	}

	g.marketsInitialized = true
	g.markets = commonMarkets

	return nil
}

func (g *Gateway) AccountGateway() gateway.AccountGateway {
	return g.accountGateway
}

func (g *Gateway) Tick() chan gateway.Tick {
	return g.tickCh
}
