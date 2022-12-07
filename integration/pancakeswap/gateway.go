package pancakeswap

import (
	"log"
	"math"

	"github.com/herenow/atomic-gtw/gateway"
)

var Exchange = gateway.Exchange{
	Name: "PancakeSwap",
}

type Gateway struct {
	gateway.BaseGateway
	options            gateway.Options
	markets            []gateway.Market
	marketsInitialized bool
	marketDataGateway  *MarketDataGateway
	api                *API
	tickCh             chan gateway.Tick
}

func NewGateway(options gateway.Options) gateway.Gateway {
	return &Gateway{
		api:     NewAPI(),
		options: options,
		tickCh:  make(chan gateway.Tick, 10),
	}
}

func (g *Gateway) Connect() error {
	g.marketDataGateway = NewMarketDataGateway(g.options, g.api, g.tickCh)

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
	res := struct {
		Datas []PairHourGraph `json:"pairHourDatas"`
	}{}

	query := buildQueryForMarkets(g.options.LoadMarket)
	err := g.api.RunGraphQuery(query, &res)
	if err != nil {
		log.Printf("PancakeSwap failed to load markets while fetching pairs, err: %s", err)
		return err
	}

	g.marketsInitialized = true
	g.markets = make([]gateway.Market, len(res.Datas))
	for i, pairHour := range res.Datas {
		pair := pairHour.Pair
		priceTick := 1 / math.Pow10(int(pair.Token1.Decimals)/3)
		amountTick := 1 / math.Pow10(int(pair.Token0.Decimals)/3)

		g.markets[i] = gateway.Market{
			Exchange: Exchange,
			Pair: gateway.Pair{
				Base:  pair.Token0.Symbol,
				Quote: pair.Token1.Symbol,
			},
			Symbol:     pair.ID,
			PriceTick:  priceTick,
			AmountTick: amountTick,
		}
	}

	return nil
}

func (g *Gateway) Tick() chan gateway.Tick {
	return g.tickCh
}
