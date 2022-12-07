package whitebit

import (
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"

	"github.com/herenow/atomic-gtw/gateway"
)

var Exchange = gateway.Exchange{
	Name: "WhiteBIT",
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
		api:     NewAPI(options, apiWhiteBIT),
	}
}

func (g *Gateway) Connect() error {
	err := g.loadMarkets()
	if err != nil {
		return fmt.Errorf("failed to load markets, err %s", err)
	}

	g.accountGateway = NewAccountGateway(g.options, g.api, g.tickCh)
	if g.options.ApiKey != "" {
		err = g.accountGateway.Connect("wss://api.whitebit.com/ws")
		if err != nil {
			return fmt.Errorf("failed to connect to account gateway, err %s", err)
		}
	}

	// Init market data
	g.marketDataGateway = NewMarketDataGateway("wss://api.whitebit.com/ws", g.options, g.tickCh)
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
	if !g.marketsInitialized {
		err := g.loadMarkets()
		if err != nil {
			log.Fatalln(err)
		}
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
	res, err := g.api.ExchangeInfo()
	if err != nil {
		return err
	}

	g.marketsInitialized = true
	g.markets = make([]gateway.Market, 0)

	for _, exchangeInfo := range res {
		g.markets = append(g.markets, exchangeInfoToMarket(exchangeInfo))
	}

	return nil
}

func exchangeInfoToMarket(exchangeInfo ExchangeInfoWhiteBIT) gateway.Market {
	takerFee, err := strconv.ParseFloat(exchangeInfo.TakerFee, 64)
	if err != nil {
		log.Fatalln(err)
	}

	makerFee, err := strconv.ParseFloat(exchangeInfo.MakerFee, 64)
	if err != nil {
		log.Fatalln(err)
	}

	moneyPrec, err := strconv.Atoi(exchangeInfo.MoneyPrec)
	if err != nil {
		log.Fatalln(err)
	}

	stockPrec, err := strconv.Atoi(exchangeInfo.StockPrec)
	if err != nil {
		log.Fatalln(err)
	}
	return gateway.Market{
		Exchange: Exchange,
		Symbol:   exchangeInfo.Name,
		Pair: gateway.Pair{
			Base:  strings.ToUpper(exchangeInfo.Stock),
			Quote: strings.ToUpper(exchangeInfo.Money),
		},
		TakerFee:   takerFee,
		MakerFee:   makerFee,
		PriceTick:  1 / math.Pow10(int(moneyPrec)),
		AmountTick: 1 / math.Pow10(int(stockPrec)),
	}
}

// TODO remove this function
func symbolsToCommonMarket(symbols map[string]ExchangeInfoWhiteBIT) []gateway.Market {
	commonMarkets := make([]gateway.Market, 0, len(symbols))

	for sym, symbol := range symbols {
		market := symbolToCommonMarket(sym, symbol)
		commonMarkets = append(commonMarkets, market)
	}

	return commonMarkets
}

// TODO remove this function
func symbolToCommonMarket(sym string, symbol ExchangeInfoWhiteBIT) gateway.Market {
	takerFee, err := strconv.ParseFloat(symbol.TakerFee, 64)
	if err != nil {
		log.Fatalln(err)
	}

	makerFee, err := strconv.ParseFloat(symbol.MakerFee, 64)
	if err != nil {
		log.Fatalln(err)
	}

	moneyPrec, err := strconv.Atoi(symbol.MoneyPrec)
	if err != nil {
		log.Fatalln(err)
	}

	stockPrec, err := strconv.Atoi(symbol.StockPrec)
	if err != nil {
		log.Fatalln(err)
	}

	return gateway.Market{
		Exchange: Exchange,
		Symbol:   symbol.Name,
		Pair: gateway.Pair{
			Base:  strings.ToUpper(symbol.Stock),
			Quote: strings.ToUpper(symbol.Money),
		},
		TakerFee:   takerFee,
		MakerFee:   makerFee,
		PriceTick:  1 / math.Pow10(int(moneyPrec)),
		AmountTick: 1 / math.Pow10(int(stockPrec)),
	}
}
