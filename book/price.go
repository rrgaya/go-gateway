package book

import "fmt"

type Price struct {
	Value  float64
	Amount float64
}

func (p Price) String() string {
	return fmt.Sprintf("%g@%g", p.Amount, p.Value)
}

func NewPrice(price float64, amount float64) Price {
	return Price{
		Value:  price,
		Amount: amount,
	}
}
