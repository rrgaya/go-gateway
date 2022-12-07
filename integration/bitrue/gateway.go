package bitrue

import (
	"fmt"
	"strings"

	"github.com/herenow/atomic-gtw/gateway"
)

var Exchange = gateway.Exchange{
	Name: "Bitrue",
}

var KLineWsUrl = "wss://ws.bitrue.com/kline-api/ws"
var StreamWsUrl = "wss://wsapi.bitrue.com/stream?listenKey=%s"

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
	g.marketDataGateway = NewMarketDataGateway(g.options, g.Markets(), g.api, false, g.tickCh)
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
	res, err := g.api.ExchangeInfo()
	if err != nil {
		return err
	}

	g.marketsInitialized = true
	g.markets, err = symbolsToCommonMarket(res.Symbols)
	if err != nil {
		return err
	}

	return nil
}

func symbolsToCommonMarket(symbols []APISymbol) ([]gateway.Market, error) {
	commonMarkets := make([]gateway.Market, 0, len(symbols))

	for _, symbol := range symbols {
		if symbol.Status == "TRADING" {
			market, err := symbolToCommonMarket(symbol)
			if err != nil {
				return []gateway.Market{}, err
			}

			commonMarkets = append(commonMarkets, market)
		}
	}

	return commonMarkets, nil
}

func symbolToCommonMarket(symbol APISymbol) (gateway.Market, error) {
	var priceTick float64
	var amountTick float64
	var minimumOrderSize float64
	var minimumOrderValue float64

	for _, filter := range symbol.Filters {
		switch filter.FilterType {
		case "PRICE_FILTER":
			priceTick = filter.TickSize
		case "LOT_SIZE":
			minimumOrderSize = filter.MinQty
			minimumOrderValue = filter.MinVal
			amountTick = filter.StepSize
		}
	}

	if priceTick == 0 {
		return gateway.Market{}, fmt.Errorf("Unable to find symbol %s priceTick", symbol.Symbol)
	} else if amountTick == 0 {
		return gateway.Market{}, fmt.Errorf("Unable to find symbol %s amountTick", symbol.Symbol)
	} else if minimumOrderSize == 0 {
		return gateway.Market{}, fmt.Errorf("Unable to find symbol %s minimumOrderSize", symbol.Symbol)
	}

	return gateway.Market{
		Exchange: Exchange,
		Symbol:   symbol.Symbol,
		Pair: gateway.Pair{
			Base:  strings.ToUpper(symbol.BaseAsset),
			Quote: strings.ToUpper(symbol.QuoteAsset),
		},
		TakerFee:         0.001,
		MakerFee:         0.001,
		PriceTick:        priceTick,
		AmountTick:       amountTick,
		MinimumOrderSize: minimumOrderSize,
		// Need to do this, because it needs to be greater, not
		// greater or equal
		MinimumOrderMoneyValue: minimumOrderValue + priceTick,
	}, nil
}

func (g *Gateway) AccountGateway() gateway.AccountGateway {
	return g.accountGateway
}

func (g *Gateway) Tick() chan gateway.Tick {
	return g.tickCh
}
