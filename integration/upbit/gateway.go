package upbit

import (
	"fmt"
	"math"

	"github.com/herenow/atomic-gtw/gateway"
)

var Exchange = gateway.Exchange{
	Name: "Upbit",
}

type Gateway struct {
	gateway.BaseGateway
	options            gateway.Options
	api                *API
	marketsInitialized bool
	markets            []gateway.Market
	marketDataGateway  *MarketDataGateway
	tickCh             chan gateway.Tick
}

func NewGateway(options gateway.Options) gateway.Gateway {
	gtw := &Gateway{
		options: options,
		api:     NewAPI(options),
		tickCh:  make(chan gateway.Tick),
	}

	return gtw
}

func (g *Gateway) Connect() error {
	err := g.loadMarkets()
	if err != nil {
		return fmt.Errorf("Failed to load markets, err %s", err)
	}

	// Init market data
	g.marketDataGateway = NewMarketDataGateway(g.options, g.tickCh)

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
	markets, err := g.api.ActiveMarkets()
	if err != nil {
		return err
	}

	g.markets = make([]gateway.Market, len(markets))
	for i, market := range markets {
		var minimumOrderMoneyValue float64
		switch market.QuoteCurrencyCode {
		case "KRW":
			minimumOrderMoneyValue = 500
		case "BTC":
			minimumOrderMoneyValue = 0.0005
		case "ETH":
			minimumOrderMoneyValue = 0.0005
		}

		g.markets[i] = gateway.Market{
			Exchange: Exchange,
			Symbol:   market.Code,
			Pair: gateway.Pair{
				Base:  market.BaseCurrencyCode,
				Quote: market.QuoteCurrencyCode,
			},
			MakerFee:               0.0025,
			TakerFee:               0.0025,
			PriceTick:              1 / math.Pow10(int(market.QuoteCurrencyDecimalPlace)),
			AmountTick:             1 / math.Pow10(int(market.BaseCurrencyDecimalPlace)),
			MinimumOrderMoneyValue: minimumOrderMoneyValue,
		}
	}

	g.marketsInitialized = true

	return nil
}

func (g *Gateway) Tick() chan gateway.Tick {
	return g.tickCh
}
