package exmarkets

import (
	"fmt"
	"math"
	"strings"

	"github.com/herenow/atomic-gtw/gateway"
)

var Exchange = gateway.Exchange{
	Name: "ExMarkets",
}

type Gateway struct {
	gateway.BaseGateway
	options            gateway.Options
	accountGateway     *AccountGateway
	marketDataGateway  *MarketDataGateway
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

	g.marketDataGateway = NewMarketDataGateway(g.options, g.Markets(), g.api, g.tickCh)

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

	res, err := g.api.GeneralInfo()
	if err != nil {
		return err
	}

	g.marketsInitialized = true
	for _, market := range res.Markets {
		// Only active markets
		if market.Active == false {
			continue
		}

		parts := strings.Split(market.Slug, "-")
		if len(parts) < 2 {
			return fmt.Errorf("failed to split market slug %s into base/quote", market.Slug)
		}
		base := strings.ToUpper(parts[0])
		quote := strings.ToUpper(parts[1])
		priceTick := 1 / math.Pow10(int(market.PricePrecision))
		amountTick := 1 / math.Pow10(int(market.BasePrecision))

		g.markets = append(g.markets, gateway.Market{
			Exchange: Exchange,
			Symbol:   market.Slug,
			Pair: gateway.Pair{
				Base:  base,
				Quote: quote,
			},
			TakerFee:         0.0012,
			MakerFee:         0.0012,
			PriceTick:        priceTick,
			AmountTick:       amountTick,
			MinimumOrderSize: market.MinAmount,
		})
	}

	return nil
}
