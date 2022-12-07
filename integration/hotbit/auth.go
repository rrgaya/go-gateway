package hotbit

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

type WsUserSign2Params struct {
	From string `json:"from"`
}

func NewWsUserSign2Request() WsRequest {
	return NewWsRequest("user.sign2", WsUserSign2Params{
		From: "web",
	})
}

func (r *WsUserSign2) ToServerAuth2() WsServerAuth2 {
	return WsServerAuth2{
		Uid:  r.Uid,
		Time: r.Time,
		From: r.From,
		Sign: r.Sign,
	}
}

type WsServerAuth2 struct {
	Uid  int64
	Time int64
	From string
	Sign string
}

func (a WsServerAuth2) MarshalJSON() ([]byte, error) {
	array := []interface{}{a.Uid, a.Time, a.From, a.Sign}
	return json.Marshal(array)
}

type WsUserSign2 struct {
	From string `json:"from"`
	Sign string `json:"sign"`
	Time int64  `json:"time"`
	Uid  int64  `json:"uid"`
}

var AuthWsUrl = "wss://www.hotbit.mobi/ws"

func getServerAuth2(userAgent, cookies string) (WsServerAuth2, error) {
	dialer := websocket.Dialer{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	headers := http.Header{}
	headers.Add("User-Agent", userAgent)
	headers.Add("Cookie", cookies)

	ws, _, err := dialer.Dial(AuthWsUrl, headers)

	if err != nil {
		log.Printf(" error connecting to auth ws: %v", err)
		return WsServerAuth2{}, err
	}
	defer ws.Close()

	signinRequest := NewWsUserSign2Request()

	requestData, err := json.Marshal(signinRequest)
	if err != nil {
		return WsServerAuth2{}, err
	}

	err = ws.WriteMessage(websocket.TextMessage, requestData)
	if err != nil {
		return WsServerAuth2{}, err
	}

	wsAuthRes := make(chan WsServerAuth2)
	wsErr := make(chan error)

	go func() {
		for {
			_, message, err := ws.ReadMessage()
			if err != nil {
				wsErr <- err
				return
			}

			wsMessage := WsMessage{}
			err = json.Unmarshal(message, &wsMessage)
			if err != nil {
				wsErr <- err
				return
			}

			if wsMessage.Error != "" {
				wsErr <- errors.New(wsMessage.Error)
				return
			}

			if wsMessage.Method == "user.sign2" {
				result := WsUserSign2{}
				err = json.Unmarshal(wsMessage.Result, &result)
				if err != nil {
					wsErr <- err
					return
				}
				wsAuthRes <- result.ToServerAuth2()
				return
			}
		}
	}()

	select {
	case res := <-wsAuthRes:
		return res, nil
	case err := <-wsErr:
		return WsServerAuth2{}, errors.New(fmt.Sprintf("Auth ws returned error while reading responses, err: %s", err))
	case <-time.After(10 * time.Second):
		return WsServerAuth2{}, errors.New("Timed out waiting for auth response from ws")
	}
}
