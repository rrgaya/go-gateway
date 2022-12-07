package utils

import (
	"fmt"
)

type WsErrorOrigin string

const (
	WsUnmarshalError  WsErrorOrigin = "unmarshal"
	WsRequestError    WsErrorOrigin = "request"
	WsReadError       WsErrorOrigin = "read"
	WsConnectionError WsErrorOrigin = "connection"
)

type WsError struct {
	Exchange string        `json:"exchange"`
	Origin   WsErrorOrigin `json:"origin"`
	Error    string        `json:"error"`
	Body     string        `json:"body"`
}

func NewWsError(exchange string, origin WsErrorOrigin, err string, body string) *WsError {
	return &WsError{
		Exchange: exchange,
		Origin:   origin,
		Error:    err,
		Body:     body,
	}
}

func (e *WsError) AsError() error {
	str := fmt.Sprintf("%s ws error - %s\n", e.Exchange, e.Origin)
	str += fmt.Sprintf("Error: %s\n", e.Error)

	if e.Body != "" {
		str += fmt.Sprintf("Body: %s", e.Body)
	}

	return fmt.Errorf(str)
}
