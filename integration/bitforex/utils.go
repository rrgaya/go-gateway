package bitforex

import (
	"fmt"
	"log"
	"math"
	"strings"

	"github.com/herenow/atomic-gtw/gateway"
)

func symbolsToCommonMarket(symbols []Symbol) ([]gateway.Market, error) {
	commonMarkets := make([]gateway.Market, 0, len(symbols))

	for _, symbol := range symbols {
		market, err := symbolToCommonMarket(symbol)
		if err != nil {
			log.Printf("Bitforex error while converting symbols to market: %s", err)
		} else {
			commonMarkets = append(commonMarkets, market)
		}
	}

	return commonMarkets, nil
}

func symbolToCommonMarket(symbol Symbol) (gateway.Market, error) {
	split := strings.Split(symbol.Symbol, "-")

	if len(split) < 3 {
		return gateway.Market{}, fmt.Errorf("unable to extract base/quote from symbol %s", symbol.Symbol)
	}

	quote := split[1] // Quote actually comes first in thir symbol string
	base := split[2]
	priceTick := 1 / math.Pow10(int(symbol.PricePrecision))
	amountTick := 1 / math.Pow10(int(symbol.AmountPrecision))

	return gateway.Market{
		Exchange: Exchange,
		Pair: gateway.Pair{
			Base:  strings.ToUpper(base),
			Quote: strings.ToUpper(quote),
		},
		Symbol:           symbol.Symbol,
		MakerFee:         0.001,
		TakerFee:         0.001,
		PaysFeeInStock:   true,
		PriceTick:        priceTick,
		AmountTick:       amountTick,
		MinimumOrderSize: symbol.MinOrderAmount,
	}, nil
}

func orderStateToCommon(state int64) gateway.OrderState {
	switch state {
	case 0:
		return gateway.OrderOpen
	case 1:
		return gateway.OrderPartiallyFilled
	case 2:
		return gateway.OrderFullyFilled
	case 3:
		return gateway.OrderCancelled
	case 4:
		return gateway.OrderCancelled
	}

	log.Printf("Failed to parse order state %d into a gateway.OrderState", state)

	return ""
}
