package book

import (
	"fmt"
	"log"
	"sort"
	"sync"
)

type NotEnoughLiquidityErr struct {
	Book *Book
}

// Return error message, this is part of the error interface.
func (e *NotEnoughLiquidityErr) Error() string {
	return fmt.Sprintf("There was not enough liquidty on order book %s", e.Book.Symbol())
}

type Prices struct {
	side     BidAskSide
	maxDepth int
	data     map[float64]Price
	levels   []float64
	mutex    *sync.Mutex
	book     *Book
}

func NewPrices(book *Book, side BidAskSide) *Prices {
	return &Prices{
		book:     book,
		side:     side,
		maxDepth: 25,
		data:     make(map[float64]Price),
		levels:   make([]float64, 0),
		mutex:    &sync.Mutex{},
	}
}

func (prices *Prices) Length() int {
	return len(prices.levels)
}

func (prices *Prices) Reset() {
	prices.mutex.Lock()
	defer prices.mutex.Unlock()

	prices.data = make(map[float64]Price)
	prices.levels = make([]float64, 0)
}

func (prices *Prices) Update(pxs []Price) {
	prices.mutex.Lock()
	defer prices.mutex.Unlock()

	news := make([]Price, 0)
	chgs := make([]Price, 0)
	dels := make([]int, 0)

	for _, px := range pxs {
		idx, ok := prices.priceIndex(px.Value)

		if ok {
			if px.Amount > 0 {
				chgs = append(chgs, px)
			} else {
				if intsContains(dels, idx) {
					log.Printf(
						"There is something wrong with the market data! "+
							"Received del twice for price [%f] on side [%s] "+
							"on book [%s] at index [%d].",
						px.Value, prices.side, prices.book.Symbol(), idx,
					)
				} else {
					dels = append(dels, idx)
				}
			}
		} else {
			if px.Amount > 0 {
				news = append(news, px)
			} else {
				// Ignore, we don't event have this price
				// registered, we don't need to delete it
			}
		}
	}

	// We need to sort our dels, we need to delete the
	// lowest index first, and move from there.
	sort.Ints(dels)

	for i, idx := range dels {
		// Each time we remove an index, the price index
		// array gets, smaller, so we need to account for
		// that by removing N from the "removal index"
		prices.removePriceIndex(idx - i)
	}
	for _, px := range chgs {
		prices.updatePrice(px)
	}
	for _, px := range news {
		prices.addPrice(px)
	}

	if len(news) > 0 {
		prices.sortPrices()
	}
}

func (prices *Prices) PriceLevels() []Price {
	prices.mutex.Lock()
	defer prices.mutex.Unlock()

	priceLevels := make([]Price, len(prices.levels))

	for index, price := range prices.levels {
		priceLevels[index] = prices.data[price]
	}

	return priceLevels
}

func (prices *Prices) PriceIndex(price float64) (int, bool) {
	prices.mutex.Lock()
	defer prices.mutex.Unlock()

	return prices.priceIndex(price)
}

func (prices *Prices) priceIndex(price float64) (int, bool) {
	for index, value := range prices.levels {
		if price == value {
			return index, true
		}
	}

	return -1, false
}

func (prices *Prices) RemovePriceIndex(index int) {
	prices.mutex.Lock()
	defer prices.mutex.Unlock()

	prices.removePriceIndex(index)
}

func (prices *Prices) removePriceIndex(index int) {
	delete(prices.data, prices.levels[index])
	prices.levels = append(prices.levels[:index], prices.levels[index+1:]...)
}

func (prices *Prices) addPrice(px Price) {
	prices.levels = append(prices.levels, px.Value)
	prices.data[px.Value] = px
}

func (prices *Prices) updatePrice(px Price) {
	prices.data[px.Value] = px
}

func (prices *Prices) sortPrices() {
	if prices.side == Bid {
		sort.Sort(sort.Reverse(sort.Float64Slice(prices.levels)))
	} else {
		sort.Sort(sort.Float64Slice(prices.levels))
	}
}

func (prices *Prices) Top() (Price, bool) {
	return prices.TopN(0)
}

func (prices *Prices) TopN(n int) (Price, bool) {
	prices.mutex.Lock()
	defer prices.mutex.Unlock()

	if len(prices.levels) <= n {
		return Price{}, false
	}

	price := prices.levels[n]

	return prices.data[price], true
}

func (prices *Prices) TopRange(n int) []Price {
	prices.mutex.Lock()
	defer prices.mutex.Unlock()

	if n > len(prices.levels) {
		n = len(prices.levels)
	}

	levels := prices.levels[:n]
	data := make([]Price, n)
	for i, p := range levels {
		data[i] = prices.data[p]
	}

	return data
}

func (prices *Prices) Bottom() (Price, bool) {
	prices.mutex.Lock()
	defer prices.mutex.Unlock()

	if len(prices.levels) == 0 {
		return Price{}, false
	}

	var lastIndex int

	if len(prices.levels) >= prices.maxDepth {
		lastIndex = prices.maxDepth - 1
	} else {
		lastIndex = len(prices.levels) - 1
	}

	lastPrice := prices.levels[lastIndex]

	return prices.data[lastPrice], true
}

func intsContains(ints []int, i int) bool {
	for _, v := range ints {
		if v == i {
			return true
		}
	}
	return false
}
