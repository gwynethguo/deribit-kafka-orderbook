package handler

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/segmentio/kafka-go"
)

type OrderBook struct {
	Instrument   string          `json:"instrument_name"`
	Bids         [][]interface{} `json:"bids"`
	Asks         [][]interface{} `json:"asks"`
	Timestamp    int64           `json:"timestamp"`
	Type         string          `json:"type"`
	ChangeId     int64           `json:"change_id"`
	PrevChangeId *int64          `json:"prev_change_id,omitempty"`
}

type DeribitMessage struct {
	Method string `json:"method"`
	Params struct {
		Channel string    `json:"channel"`
		Data    OrderBook `json:"data"`
	} `json:"params"`
}

type InstrumentHandler struct {
	wg   *sync.WaitGroup
	conn *websocket.Conn
}

func (h *InstrumentHandler) Init(ctx context.Context, wg *sync.WaitGroup, kafkaWriter *kafka.Writer, instrumentCh chan<- DeribitMessage, reconnectCh chan<- string) {
	h.wg = wg

	sendCh := make(chan DeribitMessage)
	h.wg.Add(2)
	go h.sendToKafka(ctx, sendCh, kafkaWriter)
	go h.handleMessages(ctx, sendCh, instrumentCh, reconnectCh)
}

// func (h *InstrumentHandler) connectToDeribit() bool {
// 	log.Println("Connecting to Deribit WebSocket...")
// 	defer log.Println("Successfully connected to Deribit WebSocket.")

// 	conn, _, err := websocket.DefaultDialer.Dial(constants.DeribitWSURL, nil)
// 	if err != nil {
// 		log.Printf("Failed to connect to Deribit: %v\n", err)
// 		return false
// 	}

// 	h.conn = conn
// 	return true
// }

// func (h *InstrumentHandler) subscribeToOrderBook() bool {

// 	log.Println("Subscribing to order book...")
// 	defer log.Println("Successfully subscribed to order book updates.")

// 	subscribeMsg := map[string]any{
// 		"jsonrpc": "2.0",
// 		"id":      1,
// 		"method":  "public/subscribe",
// 		"params": map[string]any{
// 			"channels": []string{
// 				fmt.Sprintf("book.%s.100ms", h.instrument), // 100ms granularity
// 			},
// 		},
// 	}
// 	if err := h.conn.WriteJSON(subscribeMsg); err != nil {
// 		log.Fatalf("Subscribe failed: %v", err)
// 	}

// 	_, _, err := h.conn.ReadMessage()
// 	// TODO: handle instrument doesn't exist
// 	if err != nil {
// 		log.Printf("Websocket read error: %v.", err)
// 		return false
// 	}

// 	return true
// }

func (h *InstrumentHandler) sendToKafka(ctx context.Context, sendCh <-chan DeribitMessage, kafkaWriter *kafka.Writer) {
	defer h.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case deribitMsg, ok := <-sendCh:
			if !ok {
				return
			}
			kafkaMessage := kafka.Message{
				Key: []byte(deribitMsg.Params.Data.Instrument),
				Value: func() []byte {
					dataBytes, err := json.Marshal(deribitMsg.Params.Data)
					if err != nil {
						log.Fatalf("Error marshaling data: %v", err)
					}
					return dataBytes
				}(),
				Time: time.Unix(0, deribitMsg.Params.Data.Timestamp*int64(time.Millisecond)),
			}
			log.Printf("[KAFKA PRODUCER] KEY: %s, VALUE: %s\n", string(kafkaMessage.Key), string(kafkaMessage.Value))
			err := kafkaWriter.WriteMessages(ctx, kafkaMessage)

			if err != nil {
				log.Printf("Failed to send to Kafka: %v", err)
			}
		}
	}
}

func (h *InstrumentHandler) handleMessages(ctx context.Context, instrumentCh <-chan DeribitMessage, sendCh chan<- DeribitMessage, reconnectCh chan<- string) {
	defer h.wg.Done()
	var prevChangeId *int64

	for {
		select {
		case <-ctx.Done():
			close(sendCh)
			return
		case deribitMsg := <-instrumentCh:
			log.Println(deribitMsg)

			if deribitMsg.Method != "subscription" {
				continue // Skip non-orderbook messages
			}

			if (prevChangeId != nil) && (deribitMsg.Params.Data.PrevChangeId != nil) && (*prevChangeId != *deribitMsg.Params.Data.PrevChangeId) {
				// reconnect to websocket
				reconnectCh <- deribitMsg.Params.Data.Instrument
			}

			prevChangeId = &deribitMsg.Params.Data.ChangeId

			// Send to kafka
			h.wg.Add(1)
			go func() {
				defer h.wg.Done()
				sendCh <- deribitMsg
			}()
		}
	}
}
