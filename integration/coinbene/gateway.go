package coinbene

import (
	"fmt"
	"math"

	"github.com/herenow/atomic-gtw/gateway"
)

var Exchange = gateway.Exchange{
	Name: "Coinbene",
}

type Gateway struct {
	gateway.BaseGateway
	options            gateway.Options
	marketsInitialized bool
	markets            []gateway.Market
	marketDataGateway  *MarketDataGateway
	accountGateway     *AccountGateway
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
	var err error

	g.accountGateway = NewAccountGateway(g.options, g.api, g.tickCh)
	err = g.accountGateway.Connect()
	if err != nil {
		return fmt.Errorf("Failed to connect to account gateway, err %s", err)
	}

	// Init market data
	g.marketDataGateway = NewMarketDataGateway(g, g.tickCh)

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

func (g *Gateway) SubscribeMarkets(markets []gateway.Market) error {
	return g.marketDataGateway.SubscribeMarkets(markets)
}

func (g *Gateway) SetMarkets(markets []gateway.Market) error {
	g.markets = markets
	return nil
}

func (g *Gateway) loadMarkets() error {
	pairs, err := g.api.TradePairsList()
	if err != nil {
		return err
	}

	markets := make([]gateway.Market, len(pairs))
	for i, pair := range pairs {
		markets[i] = gateway.Market{
			Exchange: Exchange,
			Symbol:   pair.Symbol,
			Pair: gateway.Pair{
				Base:  pair.BaseAsset,
				Quote: pair.QuoteAsset,
			},
			MakerFee:         pair.MakerFeeRate,
			TakerFee:         pair.TakerFeeRate,
			PriceTick:        1 / math.Pow10(int(pair.PricePrecision)),
			AmountTick:       1 / math.Pow10(int(pair.AmountPrecision)),
			MinimumOrderSize: pair.MinAmount,
		}
	}

	g.marketsInitialized = true
	g.markets = markets

	return nil
}

func (g *Gateway) AccountGateway() gateway.AccountGateway {
	return g.accountGateway
}

func (g *Gateway) Tick() chan gateway.Tick {
	return g.tickCh
}
