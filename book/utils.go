package book

func PricesEqual(a, b []Price) bool {
	if (a == nil) != (b == nil) {
		return false
	}

	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

func BidAskSpread(bid, ask float64) float64 {
	return 1.0 - (bid / ask)
}

func AverageBidAskSpread(bid, ask float64) float64 {
	avg := (bid + ask) / 2
	rang := ask - bid
	return rang / avg
}
