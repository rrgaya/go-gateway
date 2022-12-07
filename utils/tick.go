package utils

import (
	"fmt"
	"math"
)

func FloatToStringWithTick(val float64, tick float64) string {
	val = FloorToTick(val, tick)
	precision := TickPrecision(tick)
	truncated := fmt.Sprintf(fmt.Sprintf("%%.%df", precision), val)

	return truncated
}

func FloatToStringWithPrec(val float64, precision int) string {
	truncated := fmt.Sprintf(fmt.Sprintf("%%.%df", precision), val)

	return truncated
}

func FloorToTick(val, tick float64) float64 {
	if tick == 0.0 {
		return val
	}

	return math.Floor((val/tick)+0.000000001) * tick
}

// Gets the decimal precision of a given tick. Example:
// Tick of 0.0125 has a precision 4
// Tick of 0.00001 has a precision of 5
// Tick of 10 has a precision of 0
// Tick of 0.05 has a precision of 2
func TickPrecision(tick float64) int {
	// If the tick is zero, it probably means the market amount/price tick
	// was not defined, we cannot use an tick of zero, because this would
	// cause a divide by zero error. I'll default the precision to zero in this case.
	if tick == 0.0 {
		return 0
	}

	precision := 0
	if tick > 0 {
		remainder := tick
		for remainder-math.Floor(remainder+0.0000000000001) >= 0.0000000000001 {
			remainder = remainder / 0.1
			precision += 1
		}
	}

	return precision
}
