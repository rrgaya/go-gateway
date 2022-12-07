package gateio

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/gateio/gateapi-go/v6"
	"github.com/herenow/atomic-gtw/gateway"
)

var Exchange = gateway.Exchange{
	Name: "Gate.io",
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
			return fmt.Errorf("Failed to load gate.io markets, err %s", err)
		}
	}

	ws := NewWsSession(gtw.options)
	err = ws.Connect()
	if err != nil {
		return err
	}

	gtw.accountGateway = NewAccountGateway(gtw.api, gtw.tickCh, gtw.options)
	if gtw.options.ApiKey != "" {
		err = gtw.accountGateway.Connect(ws)
		if err != nil {
			return err
		}
	}

	gtw.marketDataGateway = NewMarketDataGateway(gtw.options, gtw.tickCh)
	err = gtw.marketDataGateway.Connect(ws)
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
	gtw.markets = make([]gateway.Market, 0, len(*symbols))

	for _, symbol := range *symbols {
		if symbol.TradeStatus == "untradable" {
			continue
		}

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

func symbolToCommonMarket(symbol gateapi.CurrencyPair) gateway.Market {
	priceTick := 1 / math.Pow10(int(symbol.Precision))
	amountTick := 1 / math.Pow10(int(symbol.AmountPrecision))

	pair := gateway.Pair{
		Base:  strings.ToUpper(symbol.Base),
		Quote: strings.ToUpper(symbol.Quote),
	}

	feePct, err := strconv.ParseFloat(symbol.Fee, 64)
	if err != nil {
		panic(fmt.Sprintf("Problem converting Gate.io fee, err: %s", err))
	}

	fee := feePct / 100.0

	var minOrderSize float64 = 0 // zero means there's no limit

	if symbol.MinBaseAmount != "" {
		minOrderSize, err = strconv.ParseFloat(symbol.MinBaseAmount, 64)
		if err != nil {
			panic(fmt.Sprintf("Problem converting Gate.io MinBaseAmount, err: %s", err))
		}
	}

	var minOrderMoneyValue float64 = 0 // zero means there's no limit

	if symbol.MinQuoteAmount != "" {
		minOrderMoneyValue, err = strconv.ParseFloat(symbol.MinQuoteAmount, 64)
		if err != nil {
			panic(fmt.Sprintf("Problem converting Gate.io MinQuoteAmount, err: %s", err))
		}
	}

	return gateway.Market{
		Exchange:               Exchange,
		Pair:                   pair,
		Symbol:                 symbol.Id,
		TakerFee:               fee,
		MakerFee:               fee,
		PriceTick:              priceTick,
		AmountTick:             amountTick,
		MinimumOrderSize:       minOrderSize,
		MinimumOrderMoneyValue: minOrderMoneyValue,
		PaysFeeInStock:         true, // TODO: Check, I'm not sure about the value here
	}
}
