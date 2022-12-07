package bitz

import (
	"log"
	"math"
	"strings"

	"github.com/herenow/atomic-gtw/gateway"
)

var Exchange = gateway.Exchange{
	Name: "BitZ",
}

type Gateway struct {
	gateway.BaseGateway
	options            gateway.Options
	marketsInitialized bool
	markets            []gateway.Market
	marketDataGateway  *MarketDataGateway
	api                *API
	ws                 *WS
	tickCh             chan gateway.Tick
}

func NewGateway(options gateway.Options) gateway.Gateway {
	gtw := &Gateway{
		api:     NewApi(options.ApiKey, options.ApiSecret),
		ws:      NewWS(),
		options: options,
		tickCh:  make(chan gateway.Tick, 100),
	}

	gtw.marketDataGateway = NewMarketDataGateway(gtw)

	return gtw
}

func (g *Gateway) Connect() error {
	err := g.ws.Connect()
	if err != nil {
		return err
	}

	err = g.marketDataGateway.Connect(g.ws, g.Markets())
	if err != nil {
		return err
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
		err := g.loadMarkets()
		if err != nil {
			log.Printf("bitz failed to load markets, err: %s", err)
		}
	}
	return g.markets
}

func (g *Gateway) loadMarkets() error {
	symbols, err := g.api.GetSymbols()
	if err != nil {
		return err
	}

	g.marketsInitialized = true
	g.markets, err = symbolsToCommonMarket(symbols)
	if err != nil {
		return err
	}

	return nil
}

func (g *Gateway) Tick() chan gateway.Tick {
	return g.tickCh
}

func symbolsToCommonMarket(symbols []Symbol) ([]gateway.Market, error) {
	commonMarkets := make([]gateway.Market, 0, len(symbols))

	for _, symbol := range symbols {
		commonMarkets = append(commonMarkets, gateway.Market{
			Exchange: Exchange,
			Pair: gateway.Pair{
				Base:  strings.ToUpper(symbol.CoinFrom),
				Quote: strings.ToUpper(symbol.CoinTo),
			},
			Symbol:           symbol.Name,
			MakerFee:         0.002,
			TakerFee:         0.002,
			PriceTick:        1 / math.Pow10(symbol.PriceFloat),
			AmountTick:       1 / math.Pow10(symbol.NumberFloat),
			MinimumOrderSize: symbol.MinTrade,
		})
	}

	return commonMarkets, nil
}
