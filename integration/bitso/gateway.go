package bitso

import (
	"fmt"
	"log"
	"math"
	"strings"

	"github.com/herenow/atomic-gtw/gateway"
)

const (
	XRPtick     = 0.000001
	TUSDtick    = 0.01
	USDtick     = 0.01
	defaultTick = 0.00000001
)

var Exchange = gateway.Exchange{
	Name: "Bitso",
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
		api:     NewAPI(options),
		tickCh:  make(chan gateway.Tick, 100),
	}
}

func (gtw *Gateway) Connect() error {
	var err error
	if gtw.marketsInitialized == false {
		err = gtw.loadMarkets()
		if err != nil {
			return fmt.Errorf("Failed to load Bitso markets, err %s", err)
		}
	}

	ws := NewWsSession(gtw.options)
	err = ws.Connect()
	if err != nil {
		return err
	}

	gtw.accountGateway = NewAccountGateway(gtw.api, gtw.tickCh, gtw.Markets())
	gtw.marketDataGateway = NewMarketDataGateway(gtw.options, gtw.tickCh)

	err = gtw.marketDataGateway.Connect(ws, gtw.api)
	if err != nil {
		return err
	}

	err = gtw.accountGateway.Connect()
	if err != nil {
		return err
	}

	return nil
}

func (gtw *Gateway) SubscribeMarkets(markets []gateway.Market) error {
	return gtw.marketDataGateway.SubscribeMarkets(markets)
}

func (gtw *Gateway) Close() error {
	return nil
}

func (gtw *Gateway) Exchange() gateway.Exchange {
	return Exchange
}

func (gtw *Gateway) Markets() []gateway.Market {
	if gtw.marketsInitialized == false {
		_ = gtw.loadMarkets()
	}

	return gtw.markets
}

func (gtw *Gateway) loadMarkets() error {
	symbols, err := gtw.api.FetchSymbols()
	if err != nil {
		return err
	}

	gtw.marketsInitialized = true
	gtw.markets = make([]gateway.Market, 0, len(symbols))

	for _, symbol := range symbols {
		gtw.markets = append(gtw.markets, symbolToCommonMarket(symbol))
	}

	return nil
}

func (gtw *Gateway) AccountGateway() gateway.AccountGateway {
	return gtw.accountGateway
}

func (gtw *Gateway) Tick() chan gateway.Tick {
	return gtw.tickCh
}

func symbolToCommonMarket(symbol ApiAvailableBook) gateway.Market {
	splitedSymbol := strings.Split(symbol.Symbol, "_") // e.g., "btc_mxn"
	if len(splitedSymbol) < 2 {
		log.Printf("Bitso market [%s] impossible to extract base/quote assets", symbol.Symbol)
		return gateway.Market{}
	}

	pair := gateway.Pair{
		Base:  strings.ToUpper(splitedSymbol[0]),
		Quote: strings.ToUpper(splitedSymbol[1]),
	}
	calculatePrecision := func(tickSize int) float64 { return 1 / math.Pow10(int(tickSize)) }
	priceTick := calculatePrecision(countDecimalCases(symbol.TickSize))
	amountTick := func() float64 {
		switch pair.Base {
		case "TUSD":
			return TUSDtick
		case "USD":
			return USDtick
		case "XRP":
			return XRPtick
		default:
			return defaultTick
		}
	}()

	return gateway.Market{
		Exchange:               Exchange,
		Pair:                   pair,
		Symbol:                 symbol.Symbol,
		TakerFee:               symbol.Fees.FlatRate.Taker / 100,
		MakerFee:               symbol.Fees.FlatRate.Maker / 100,
		PriceTick:              priceTick,
		AmountTick:             amountTick,
		MinimumOrderSize:       symbol.MinAmount,
		MinimumOrderMoneyValue: 0,
		PaysFeeInStock:         true, // TODO: Check, I'm not sure about the value here
	}
}

func countDecimalCases(val string) int {
	parts := strings.Split(val, ".")
	if len(parts) > 1 {
		return len(parts[1])
	} else {
		return 0
	}
}
