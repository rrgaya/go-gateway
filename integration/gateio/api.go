package gateio

import (
	"context"

	"github.com/gateio/gateapi-go/v6"
	"github.com/herenow/atomic-gtw/gateway"
)

type API struct {
	options    gateway.Options
	httpClient *gateapi.APIClient
}

func NewAPI(options gateway.Options) *API {
	client := gateapi.NewAPIClient(gateapi.NewConfiguration())

	return &API{
		options:    options,
		httpClient: client,
	}
}

// https://github.com/gateio/gateapi-go/blob/master/docs/SpotApi.md#listcurrencypairs
// Return type: https://github.com/gateio/gateapi-go/blob/master/docs/CurrencyPair.md
func (api *API) FetchSymbols() (*[]gateapi.CurrencyPair, error) {
	ctx := api.buildRequestContext()
	symbols, _, err := api.httpClient.SpotApi.ListCurrencyPairs(ctx)
	if err != nil {
		return nil, err
	}

	return &symbols, nil
}

// https://github.com/gateio/gateapi-go/blob/master/docs/SpotApi.md#listspotaccounts
// Return type: https://github.com/gateio/gateapi-go/blob/master/docs/SpotAccount.md
func (api *API) FetchBalances() ([]gateapi.SpotAccount, error) {
	ctx := api.buildRequestContext()
	result, _, err := api.httpClient.SpotApi.ListSpotAccounts(ctx, nil)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// https://github.com/gateio/gateapi-go/blob/master/docs/SpotApi.md#listallopenorders
// Return type: https://github.com/gateio/gateapi-go/blob/master/docs/OpenOrders.md
func (api *API) FetchOrders(pair, status string) ([]gateapi.Order, error) {
	ctx := api.buildRequestContext()
	result, _, err := api.httpClient.SpotApi.ListOrders(ctx, pair, status, nil)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// https://github.com/gateio/gateapi-go/blob/master/docs/SpotApi.md#createorder
// Return type: https://github.com/gateio/gateapi-go/blob/master/docs/Order.md
func (api *API) SendOrder(order gateapi.Order) (*gateapi.Order, error) {
	ctx := api.buildRequestContext()
	result, _, err := api.httpClient.SpotApi.CreateOrder(ctx, order)
	if err != nil {
		return nil, err
	}

	return &result, nil
}

// https://github.com/gateio/gateapi-go/blob/master/docs/SpotApi.md#cancelorder
// Return type: https://github.com/gateio/gateapi-go/blob/master/docs/Order.md
func (api *API) CancelOrder(orderID string, pair string) (*gateapi.Order, error) {
	ctx := api.buildRequestContext()
	result, _, err := api.httpClient.SpotApi.CancelOrder(ctx, orderID, pair, nil)
	if err != nil {
		return nil, err
	}

	return &result, nil
}

func (api *API) buildRequestContext() context.Context {
	ctx := context.WithValue(context.Background(), gateapi.ContextGateAPIV4, gateapi.GateAPIV4{
		Key:    api.options.ApiKey,
		Secret: api.options.ApiSecret,
	})

	return ctx
}
