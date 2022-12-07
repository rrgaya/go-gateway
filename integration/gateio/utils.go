package gateio

import (
	"strconv"

	"github.com/herenow/atomic-gtw/gateway"
)

func orderStatusToGatewayState(status string) gateway.OrderState {
	switch status {
	case "open":
		return gateway.OrderOpen
	case "closed":
		return gateway.OrderFullyFilled
	case "cancelled":
		return gateway.OrderCancelled
	}

	return ""
}

func gatewayOrderSideToGateSide(side gateway.Side) string {
	if side == gateway.Ask {
		return askSide
	} else {
		return bidSide
	}
}

func gateSideToGatewaySide(side string) gateway.Side {
	if side == bidSide {
		return gateway.Bid
	} else {
		return gateway.Ask
	}
}

func stringToFloat64(str string) float64 {
	num, _ := strconv.ParseFloat(str, 64)

	return num
}
