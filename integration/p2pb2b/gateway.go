package p2pb2b

import (
	"fmt"
	"math"

	"github.com/herenow/atomic-gtw/gateway"
	"github.com/herenow/atomic-gtw/integration/viabtc"
)

var Exchange = gateway.Exchange{
	Name: "P2PB2B",
}

type Gateway struct {
	gateway.BaseGateway
	options            gateway.Options
	accountGateway     *AccountGateway
	marketDataGateway  *viabtc.MarketDataGateway
	marketsInitialized bool
	markets            []gateway.Market
	tickCh             chan gateway.Tick
	api                *API
}

func NewGateway(options gateway.Options) gateway.Gateway {
	return &Gateway{
		api:     NewAPI(options),
		options: options,
		tickCh:  make(chan gateway.Tick, 100),
	}
}

func (g *Gateway) Connect() error {
	err := g.loadMarkets()
	if err != nil {
		return fmt.Errorf("failed to load markets, err %s", err)
	}

	g.accountGateway = NewAccountGateway(g.options, g.Markets(), g.api, g.tickCh)
	if g.options.ApiKey != "" {
		err := g.accountGateway.Connect()
		if err != nil {
			return fmt.Errorf("failed to connect to account gateway, err %s", err)
		}
	}

	//g.marketDataGateway = NewMarketDataGateway(g.options, g.Markets(), g.api, g.tickCh)
	g.marketDataGateway = viabtc.NewMarketDataGateway(
		Exchange,
		g.options,
		g.tickCh,
		"wss://p2pb2b.com/trade_ws",
		"https://p2pb2b.com",
	)

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

func (g *Gateway) AccountGateway() gateway.AccountGateway {
	return g.accountGateway
}

func (g *Gateway) Tick() chan gateway.Tick {
	return g.tickCh
}

func (g *Gateway) loadMarkets() error {
	g.markets = make([]gateway.Market, 0)

	res, err := g.api.Markets()
	if err != nil {
		return err
	}

	g.marketsInitialized = true
	for _, market := range res {
		g.markets = append(g.markets, gateway.Market{
			Exchange: Exchange,
			Symbol:   market.Name,
			Pair: gateway.Pair{
				Base:  market.Stock,
				Quote: market.Money,
			},
			TakerFee:         0.002,
			MakerFee:         0.002,
			PriceTick:        1 / math.Pow10(int(market.Precision.Money)),
			AmountTick:       1 / math.Pow10(int(market.Precision.Stock)),
			MinimumOrderSize: market.Limits.MinAmount,
		})
	}

	// TODO: Remove this once JAM is listed on the API
	jamNotListed := true
	for _, market := range g.markets {
		if market.Symbol == "JAM_USDT" {
			jamNotListed = false
			break
		}
	}
	if jamNotListed {
		g.markets = append(g.markets, gateway.Market{
			Exchange: Exchange,
			Symbol:   "JAM_USDT",
			Pair: gateway.Pair{
				Base:  "JAM",
				Quote: "USDT",
			},
			TakerFee:   0.002,
			MakerFee:   0.002,
			PriceTick:  0.00000001,
			AmountTick: 0.1,
		})
	}

	return nil
}
