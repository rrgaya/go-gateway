package ripio

import (
	"fmt"
	"log"

	"github.com/herenow/atomic-gtw/gateway"
)

var Exchange = gateway.Exchange{
	Name: "Ripio",
}

type Gateway struct {
	gateway.BaseGateway
	options           gateway.Options
	accountGateway    *AccountGateway
	marketDataGateway *MarketDataGateway
	tickCh            chan gateway.Tick
	api               *Api
	isMarketsLoaded   bool
	markets           []gateway.Market
}

func NewGateway(options gateway.Options) gateway.Gateway {
	return &Gateway{
		api:     NewApi(options),
		options: options,
		tickCh:  make(chan gateway.Tick, 100),
	}
}

func (g *Gateway) Connect() error {
	g.accountGateway = NewAccountGateway(g.options, g.Markets(), g.api, g.tickCh)
	err := g.accountGateway.Connect()
	if err != nil {
		return fmt.Errorf("Failed to connect to account gateway, err %s", err)
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

var amountTicks = map[string]float64{
	"BTC":   0.0001,
	"ETH":   0.0001,
	"BCH":   0.0001,
	"ZEC":   0.0001,
	"RCN":   1.0,
	"DAI":   0.01,
	"MTB18": 0.0001,
	"USDC":  0.01,
}

var priceTicks = map[string]float64{
	"BTC":  0.0000000001,
	"ETH":  0.00000001,
	"ARS":  0.01,
	"BRL":  0.01,
	"DAI":  0.01,
	"USDC": 0.01,
}

var enabledMarkets = map[string]bool{
	"BTC_USDC": true,
	"ETH_USDC": true,
	"BTC_BRL":  true,
	"ETH_BRL":  true,
	"BTC_ARS":  true,
	"ETH_ARS":  true,
}

func (g *Gateway) Markets() []gateway.Market {
	if g.isMarketsLoaded {
		return g.markets
	}

	pairs, err := g.api.GetPairs()
	if err != nil {
		log.Printf("Ripio failed to fetch markets, err: %s", err)
		return g.markets
	}

	g.markets = make([]gateway.Market, 0)
	for _, pair := range pairs {
		if pair.Enabled {
			amountTick, ok := amountTicks[pair.Base]
			if !ok {
				log.Printf("Ripio failed to find amount tick for %s", pair.Base)
				continue
			}
			priceTick, ok := priceTicks[pair.Quote]
			if !ok {
				log.Printf("Ripio failed to find price tick for %s", pair.Quote)
				continue
			}

			enabled, _ := enabledMarkets[pair.Symbol]
			if enabled {
				g.markets = append(g.markets, gateway.Market{
					Exchange: Exchange,
					Symbol:   pair.Symbol,
					Pair: gateway.Pair{
						Base:  pair.Base,
						Quote: pair.Quote,
					},
					MakerFee:   pair.MakerFee,
					TakerFee:   pair.TakerFee,
					PriceTick:  priceTick,
					AmountTick: amountTick,
				})
			}
		}
	}
	g.isMarketsLoaded = true

	return g.markets
}

func (g *Gateway) AccountGateway() gateway.AccountGateway {
	return g.accountGateway
}

func (g *Gateway) Tick() chan gateway.Tick {
	return g.tickCh
}
