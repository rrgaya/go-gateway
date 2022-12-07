package binance

import (
	"fmt"
	"strings"

	"github.com/herenow/atomic-gtw/gateway"
)

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
			amountTick = filter.StepSize
		case "MIN_NOTIONAL":
			minimumOrderValue = filter.MinNotional
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
		TakerFee:               0.001,
		MakerFee:               0.001,
		PriceTick:              priceTick,
		AmountTick:             amountTick,
		MinimumOrderSize:       minimumOrderSize,
		MinimumOrderMoneyValue: minimumOrderValue,
	}, nil
}
