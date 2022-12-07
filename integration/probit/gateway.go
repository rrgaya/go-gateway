package probit

import (
	"fmt"
	"log"
	"math"

	"github.com/herenow/atomic-gtw/gateway"
)

var Exchange = gateway.Exchange{
	Name: "ProBit",
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

	err = g.loadMarkets()
	if err != nil {
		log.Printf("Failed to load markets on ProBit, err %s", err)
	}

	g.accountGateway = NewAccountGateway(g.options, g.api, g.tickCh)
	if g.options.ApiKey != "" {
		err = g.api.InitAuthentication()
		if err != nil {
			return fmt.Errorf("Failed to init api authentication, err %s", err)
		}

		err = g.accountGateway.Connect()
		if err != nil {
			return fmt.Errorf("Failed to connect to account gateway, err %s", err)
		}
	}

	// Init market data
	g.marketDataGateway = NewMarketDataGateway(g, g.tickCh)
	err = g.marketDataGateway.Connect()
	if err != nil {
		return err
	}

	return nil
}

func (g *Gateway) Exchange() gateway.Exchange {
	return Exchange
}

func (g *Gateway) Markets() []gateway.Market {
	if g.marketsInitialized == false {
		err := g.loadMarkets()
		if err != nil {
			log.Printf("Failed to load markets on ProBit, err %s", err)
		}
	}

	return g.markets
}

func (g *Gateway) SubscribeMarkets(markets []gateway.Market) error {
	return g.marketDataGateway.SubscribeMarkets(markets)
}

func (g *Gateway) loadMarkets() error {
	apiMarkets, err := g.api.Markets()
	if err != nil {
		return err
	}

	markets := make([]gateway.Market, len(apiMarkets))
	for i, m := range apiMarkets {
		if m.Closed {
			continue
		}

		markets[i] = gateway.Market{
			Exchange: Exchange,
			Symbol:   m.ID,
			Pair: gateway.Pair{
				Base:  m.BaseCurrencyID,
				Quote: m.QuoteCurrencyID,
			},
			MakerFee:         0.002,
			TakerFee:         0.002,
			PriceTick:        m.PriceIncrement,
			AmountTick:       1 / math.Pow10(int(m.QuantityPrecision)),
			MinimumOrderSize: m.MinQuantity,
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
