package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/gorilla/websocket"
	"github.com/segmentio/kafka-go"
)

const (
	deribitWSURL = "wss://test.deribit.com/ws/api/v2"
	kafkaTopic   = "deribit_orderbook"
)

type OrderBook struct {
	Instrument string          `json:"instrument_name"`
	Bids       [][]interface{} `json:"bids"`
	Asks       [][]interface{} `json:"asks"`
	Timestamp  int64           `json:"timestamp"`
	Type       string          `json:"type"`
	ChangeId   int64           `json:"change_id"`
}

type DeribitMessage struct {
	Method string `json:"method"`
	Params struct {
		Channel string    `json:"channel"`
		Data    OrderBook `json:"data"`
	} `json:"params"`
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kafkaWriter := &kafka.Writer{
		Addr:         kafka.TCP("localhost:9092"),
		Topic:        kafkaTopic,
		RequiredAcks: kafka.RequireAll,
	}
	defer kafkaWriter.Close()

	log.Println("Connecting to Deribit...")
	conn, _, err := websocket.DefaultDialer.Dial(deribitWSURL, nil)
	if err != nil {
		log.Fatalf("Failed to connect to Deribit: %v", err)
	}
	log.Println("Successfully connected to Deribit.")
	defer conn.Close()

	log.Println("Subscribing to BTC options order books...")
	// Subscribe to BTC options order books
	subscribeMsg := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "public/subscribe",
		"params": map[string]interface{}{
			"channels": []string{
				"book.BTC-30MAY25-74000-C.100ms", // 100ms granularity
			},
		},
	}
	if err := conn.WriteJSON(subscribeMsg); err != nil {
		log.Fatalf("Subscribe failed: %v", err)
	}
	log.Println("Successfully subscribed to order book updates.")

	for {
		select {
		case <-ctx.Done():
			return
		default:
			_, msg, err := conn.ReadMessage()
			if err != nil {
				log.Printf("Websocket read error: %v. Retrying...", err)
				continue
			}

			log.Printf("%#s\n", string(msg))

			var deribitMsg DeribitMessage
			if err := json.Unmarshal(msg, &deribitMsg); err != nil {
				log.Printf("Failed to unmarshal message: %v", err)
				continue
			}

			log.Println(deribitMsg)

			if deribitMsg.Method != "subscription" {
				continue // Skip non-orderbook messages
			}

			// Send to kafka
			err = kafkaWriter.WriteMessages(ctx,
				kafka.Message{
					Key: []byte(deribitMsg.Params.Data.Instrument),
					Value: func() []byte {
						dataBytes, err := json.Marshal(deribitMsg.Params.Data)
						if err != nil {
							log.Fatalf("Error marshaling data: %v", err)
						}
						return dataBytes
					}(),
					Time: time.Unix(0, deribitMsg.Params.Data.Timestamp*int64(time.Millisecond)),
				},
			)

			log.Println("Sending data to Kafka...")
			if err != nil {
				log.Printf("Failed to send to Kafka: %v", err)
				continue
			}
			log.Println("Successfully sent data to Kafka.")
		}
	}
}
