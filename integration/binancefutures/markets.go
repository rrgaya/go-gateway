package binancefutures

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/adshao/go-binance/v2/futures"
	"github.com/herenow/atomic-gtw/gateway"
)

func symbolsToCommonMarket(symbols []futures.Symbol) ([]gateway.Market, error) {
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

func symbolToCommonMarket(symbol futures.Symbol) (gateway.Market, error) {
	var priceTick float64
	var amountTick float64
	var minimumOrderSize float64
	var minimumOrderValue float64

	for _, filter := range symbol.Filters {
		filterType, ok := filter["filterType"]
		if !ok {
			return gateway.Market{}, fmt.Errorf("Expected symbol %s filter data to contain key \"filterType\"", symbol.Symbol)
		}

		if filterType == "PRICE_FILTER" {
			tickSize, ok := filter["tickSize"]
			if !ok {
				return gateway.Market{}, fmt.Errorf("Expected symbol %s LOT_SIZE filter data to contain key \"tickSize\"", symbol.Symbol)
			}

			tickSizeVal, err := strconv.ParseFloat(tickSize.(string), 64)
			if err != nil {
				return gateway.Market{}, fmt.Errorf("Unable to parse symbol %s tickSize %s into float64", symbol.Symbol, tickSize)
			}

			priceTick = tickSizeVal
		} else if filterType == "LOT_SIZE" {
			minQty, ok := filter["minQty"]
			if !ok {
				return gateway.Market{}, fmt.Errorf("Expected symbol %s LOT_SIZE filter data to contain key \"minQty\"", symbol.Symbol)
			}

			minQtyVal, err := strconv.ParseFloat(minQty.(string), 64)
			if err != nil {
				return gateway.Market{}, fmt.Errorf("Unable to parse symbol %s minQty %s into float64", symbol.Symbol, minQty)
			}

			stepSize, ok := filter["stepSize"]
			if !ok {
				return gateway.Market{}, fmt.Errorf("Expected symbol %s LOT_SIZE filter data to contain key \"stepSize\"", symbol.Symbol)
			}

			stepSizeVal, err := strconv.ParseFloat(stepSize.(string), 64)
			if err != nil {
				return gateway.Market{}, fmt.Errorf("Unable to parse symbol %s stepSize %s into float64", symbol.Symbol, stepSize)
			}

			minimumOrderSize = minQtyVal
			amountTick = stepSizeVal
		} else if filterType == "MIN_NOTIONAL" {
			minNotional, ok := filter["notional"]
			if !ok {
				return gateway.Market{}, fmt.Errorf("Expected symbol %s MIN_NOTIONAL filter data to contain key \"minNotional\"", symbol.Symbol)
			}

			minNotionalVal, err := strconv.ParseFloat(minNotional.(string), 64)
			if err != nil {
				return gateway.Market{}, fmt.Errorf("Unable to parse symbol %s minNotional %s into float64", symbol.Symbol, minNotional)
			}

			minimumOrderValue = minNotionalVal
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
		TakerFee:               0.0004,
		MakerFee:               0.0002,
		PriceTick:              priceTick,
		AmountTick:             amountTick,
		MinimumOrderSize:       minimumOrderSize,
		MinimumOrderMoneyValue: minimumOrderValue,
	}, nil
}
