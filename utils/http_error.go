package utils

import (
	"fmt"
	"net/http"
)

type HttpErrorOrigin string

const (
	UnmarshalError   HttpErrorOrigin = "unmarshal"
	HttpRequestError HttpErrorOrigin = "request"
)

type HttpError struct {
	Exchange   string          `json:"exchange"`
	Origin     HttpErrorOrigin `json:"origin"`
	HttpMethod string          `json:"http_method"`
	Url        string          `json:"url"`
	Error      string          `json:"error"`
	Body       string          `json:"body"`
}

func (e *HttpError) AsError() error {
	str := fmt.Sprintf("%s api error - %s - %s %s\n", e.Exchange, e.Origin, e.HttpMethod, e.Url)
	str += fmt.Sprintf("Error: %s\n", e.Error)

	if e.Body != "" {
		str += fmt.Sprintf("Body: %s", e.Body)
	}

	return fmt.Errorf(str)
}

func NewHttpError(exchange string, origin HttpErrorOrigin, req *http.Request, err string, body string) *HttpError {
	return &HttpError{
		Exchange:   exchange,
		Origin:     origin,
		HttpMethod: req.Method,
		Url:        req.URL.String(),
		Body:       body,
		Error:      err,
	}
}
