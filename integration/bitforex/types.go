package bitforex

type Symbol struct {
	Symbol          string  `json:"symbol"`
	PricePrecision  int64   `json:"pricePrecision"`
	AmountPrecision int64   `json:"amountPrecision"`
	MinOrderAmount  float64 `json:"minOrderAmount"`
}

type PriceLevel struct {
	Price  float64 `json:"price"`
	Amount float64 `json:"amount"`
}
