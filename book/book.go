package book

import (
	"sync"
)

type BidAskSide byte

const (
	Bid BidAskSide = 1
	Ask BidAskSide = 2
)

func (s BidAskSide) String() string {
	switch s {
	case 1:
		return "bid"
	case 2:
		return "ask"
	default:
		return ""
	}
}

type Book struct {
	mutex             *sync.Mutex
	symbol            string
	dontCrossBidsAsks bool
	Bids              *Prices
	Asks              *Prices
}

func NewBook(symbol string) *Book {
	b := &Book{
		mutex:  &sync.Mutex{},
		symbol: symbol,
	}
	b.Bids = NewPrices(b, Bid)
	b.Asks = NewPrices(b, Ask)

	return b
}

// This function is useful in cases where the exchange gives us bids/asks
// updates delta, instead of each price level update serially.
// For example, binance, streams order book updates by the second grouped
// together in a single bids/asks delta, with price level changes.
// So there is no way to know what price level changed first. In this case,
// we must first update all price levels, before trying to cross top bids
// and asks, or else, we may cross orders that shouldn't have been crossed.
func (book *Book) DeltaUpdate(bids []Price, asks []Price) {
	book.mutex.Lock()
	defer book.mutex.Unlock()

	book.deltaUpdate(bids, asks)
}

// Resets the bids/asks and repopulates the book witht the given snapshot.
func (book *Book) Snapshot(bids []Price, asks []Price) {
	book.mutex.Lock()
	defer book.mutex.Unlock()

	book.Bids.Reset()
	book.Asks.Reset()
	book.deltaUpdate(bids, asks)
}

func (book *Book) deltaUpdate(bids []Price, asks []Price) {
	var bestBid float64
	var bestAsk float64

	book.Bids.Update(bids)
	book.Asks.Update(asks)

	for _, bid := range bids {
		if bid.Amount > 0 && (bestBid == 0 || bid.Value > bestBid) {
			bestBid = bid.Value
		}
	}

	for _, ask := range asks {
		if ask.Amount > 0 && (bestAsk == 0 || ask.Value < bestAsk) {
			bestAsk = ask.Value
		}
	}

	if bestBid != 0 {
		book.crossAsksWithBidPrice(bestBid)
	}

	if bestAsk != 0 {
		book.crossBidsWithAskPrice(bestAsk)
	}
}

// This will remove any price points from the asks side that the price
// is lower or equal to the given bid
func (book *Book) crossAsksWithBidPrice(price float64) {
	if book.dontCrossBidsAsks {
		return
	}

	for {
		bestAsk, ok := book.Asks.Top()

		if ok && bestAsk.Value <= price {
			//log.Printf("[OrderBook] [%s] Ask price %f crossed with incoming bid %f, removing it.", book.symbol, bestAsk.Value, price)
			book.Asks.RemovePriceIndex(0) // Remove the bestAsk, which is always at index 0
			continue
		}

		break
	}
}

// This will remove any price points from the bids side that the price
// is higher or equal to the given bid
func (book *Book) crossBidsWithAskPrice(price float64) {
	if book.dontCrossBidsAsks {
		return
	}

	for {
		bestBid, ok := book.Bids.Top()

		if ok && bestBid.Value >= price {
			//log.Printf("[OrderBook] [%s] Bid price %f crossed with incoming ask %f, removing it.", book.symbol, bestBid.Value, price)
			book.Bids.RemovePriceIndex(0) // Remove the bestBid, which is always at index0
			continue
		}

		break
	}
}

// Quotes how much you would receive to sell N amount at market.
// It also returns the filled prices levels.
//
// Example:
// Theres are bids of 1 BTC @ $7500 and 2 BTC @ $7000.
// You want to sell 2 BTC, how much would you receive?
// In this case, you would have 1 filled 1 BTC @ $7500 and 1 BTC @ $7000.
// You would have received a total of $14,500.
func (book *Book) QuoteSell(amount float64) (float64, []Price, error) {
	return book.quote(book.Bids, amount)
}

// Quotes how much you would need to buy N amount at market.
// It also returns the filled prices levels.
//
// Example:
// Theres are asks of 1 BTC @ $7000 and 2 BTC @ $7500.
// You want to buy 2 BTC, how much would you need?
// In this case, you would have 1 filled 1 BTC @ $7000 and 1 BTC @ $7500.
// You would have spent a total of $14,500.
func (book *Book) QuoteBuy(amount float64) (float64, []Price, error) {
	return book.quote(book.Asks, amount)
}

// Simulates a buy/sell at given price levels, it will fill each price level until the desired amount.
// This is useful for finding out how much you would receive or spend to buy or sell given amount.
// The return value will be the total value of the filled orders, and how each price level was filled.
// If the book doens't have enough liquidty to fill the entire amount, an error will be returned.
func (book *Book) quote(prices *Prices, amount float64) (float64, []Price, error) {
	priceLevels := prices.PriceLevels()
	filledPrices := make([]Price, 0)

	if len(priceLevels) == 0 {
		return 0.0, filledPrices, &NotEnoughLiquidityErr{Book: book}
	}

	filledAmount := 0.0
	totalValue := 0.0

	for _, priceLevel := range priceLevels {
		if filledAmount+priceLevel.Amount < amount {
			// Fill the entire price level, and continue filling
			filledPrices = append(filledPrices, priceLevel)
			filledAmount += priceLevel.Amount
			totalValue += priceLevel.Amount * priceLevel.Value
		} else {
			// Fill the remaining amount, and break
			fillAmount := amount - filledAmount
			filledPrices = append(filledPrices, NewPrice(priceLevel.Value, fillAmount))
			filledAmount += fillAmount
			totalValue += fillAmount * priceLevel.Value
			break
		}
	}

	if filledAmount < amount {
		return totalValue, filledPrices, &NotEnoughLiquidityErr{Book: book}
	} else {
		return totalValue, filledPrices, nil
	}
}

// Finds out how much you could buy with a given "budget" (total value). This is for finding out
// the amount of the order you would need to open when buying a given asset.
func (book *Book) QuoteBuyWithBudget(budget float64) (float64, []Price, error) {
	priceLevels := book.Asks.PriceLevels()
	filledPrices := make([]Price, 0)

	if len(priceLevels) == 0 {
		return 0.0, filledPrices, &NotEnoughLiquidityErr{Book: book}
	}

	// Since we are dealing with a "budget" and floating values (ratios), we need
	// to track if it was filled "enough".  If our budget is $100 dolars, and we spent
	// $99.99 filling orders, it may not be possible to fill any other order, because
	// of the minimum tick amount of the given order book.
	filledEnough := false

	filledAmount := 0.0
	totalSpent := 0.0

	for _, priceLevel := range priceLevels {
		priceTotal := priceLevel.Value * priceLevel.Amount

		if totalSpent+priceTotal < budget {
			// Fill the entire price level, and continue filling
			filledPrices = append(filledPrices, priceLevel)
			filledAmount += priceLevel.Amount
			totalSpent += priceTotal
		} else {
			// Fill the remaining amount, and break
			remainingBudget := budget - totalSpent
			fillAmount := remainingBudget * (1 / priceLevel.Value)

			filledPrices = append(filledPrices, NewPrice(priceLevel.Value, fillAmount))
			filledAmount += fillAmount
			totalSpent += remainingBudget

			filledEnough = true
			break
		}
	}

	if filledEnough {
		return filledAmount, filledPrices, nil
	} else {
		return filledAmount, filledPrices, &NotEnoughLiquidityErr{Book: book}
	}
}

func (book *Book) Symbol() string {
	return book.symbol
}
