package utils

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"log"
	"sync"

	"github.com/gorilla/websocket"
)

type WsDeflateMode string

const (
	WsDeflateGzip WsDeflateMode = "gzip"
)

type WsClient struct {
	conn             *websocket.Conn
	connWriterMutex  *sync.Mutex
	subscribers      map[chan []byte]bool
	subscribersMutex *sync.Mutex
	quitCb           sync.Map
	deflateMode      WsDeflateMode
}

func NewWsClient() *WsClient {
	return &WsClient{
		subscribers:      make(map[chan []byte]bool),
		subscribersMutex: &sync.Mutex{},
		connWriterMutex:  &sync.Mutex{},
		quitCb:           sync.Map{},
		deflateMode:      WsDeflateGzip,
	}
}

func (w *WsClient) SetDeflate(m WsDeflateMode) {
	w.deflateMode = m
}

func (w *WsClient) Connect(uri string) error {
	dialer := websocket.Dialer{}

	conn, _, err := dialer.Dial(uri, nil)
	if err != nil {
		return err
	}

	w.conn = conn
	go w.messageHandler()

	return nil
}

func (w *WsClient) messageHandler() {
	for {
		dataType, data, err := w.conn.ReadMessage()
		if err != nil {
			w.quitCb.Range(func(key any, val any) bool {
				quit := key.(chan bool)
				quit <- true
				return true
			})
			panic(fmt.Errorf("failed to read from ws, err: %s", err))
		}

		if dataType == websocket.BinaryMessage {
			switch w.deflateMode {
			case WsDeflateGzip:
				msgReader := bytes.NewReader(data)
				reader, err := gzip.NewReader(msgReader)
				if err != nil {
					log.Printf("Failed to initialize gzip reader, err %s", err)
					continue
				}
				decompressedData, err := ioutil.ReadAll(reader)
				if err != nil {
					log.Printf("Failed to read gzip, err %s", err)
					continue
				}

				reader.Close()
				data = decompressedData
			}

		}

		w.subscribersMutex.Lock()
		for ch := range w.subscribers {
			ch <- data
		}
		w.subscribersMutex.Unlock()
	}
}

func (w *WsClient) WriteMessage(data []byte) error {
	w.connWriterMutex.Lock()
	err := w.conn.WriteMessage(websocket.TextMessage, data)
	w.connWriterMutex.Unlock()
	if err != nil {
		return err
	}

	return nil
}

func (w *WsClient) OnDisconnect(quit chan bool) {
	w.quitCb.Store(quit, struct{}{})
}

func (w *WsClient) SubscribeMessages(ch chan []byte) {
	w.addSubscriber(ch)
}

func (w *WsClient) RemoveSubscriber(ch chan []byte) {
	w.removeSubscriber(ch)
}

func (w *WsClient) addSubscriber(ch chan []byte) {
	w.subscribersMutex.Lock()
	defer w.subscribersMutex.Unlock()

	w.subscribers[ch] = true
}

func (w *WsClient) removeSubscriber(ch chan []byte) {
	w.subscribersMutex.Lock()
	defer w.subscribersMutex.Unlock()

	delete(w.subscribers, ch)
}
