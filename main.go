package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/segmentio/kafka-go"
)

const (
	deribitWSURL = "wss://test.deribit.com/ws/api/v2"
	kafkaTopic   = "deribit_orderbook"
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
	instrument string
	wg         *sync.WaitGroup
	conn       *websocket.Conn
}

func (h *InstrumentHandler) init(ctx context.Context, kafkaWriter *kafka.Writer) {
	h.conn = h.connectToDeribit()
	h.subscribeToOrderBook()

	sendCh := make(chan DeribitMessage)
	h.wg.Add(1)
	go h.sendToKafka(sendCh, ctx, kafkaWriter)

	go h.handleMessages(sendCh, ctx)
}

func (h *InstrumentHandler) connectToDeribit() *websocket.Conn {
	log.Println("Connecting to Deribit WebSocket...")
	defer log.Println("Successfully connected to Deribit WebSocket.")

	conn, _, err := websocket.DefaultDialer.Dial(deribitWSURL, nil)
	if err != nil {
		log.Fatalf("Failed to connect to Deribit: %v", err)
	}
	return conn
}

func (h *InstrumentHandler) subscribeToOrderBook() {

	log.Println("Subscribing to BTC options order books...")
	defer log.Println("Successfully subscribed to order book updates.")

	subscribeMsg := map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "public/subscribe",
		"params": map[string]any{
			"channels": []string{
				fmt.Sprintf("book.%s.100ms", h.instrument), // 100ms granularity
			},
		},
	}
	if err := h.conn.WriteJSON(subscribeMsg); err != nil {
		log.Fatalf("Subscribe failed: %v", err)
	}
}

func (h *InstrumentHandler) sendToKafka(sendCh <-chan DeribitMessage, ctx context.Context, kafkaWriter *kafka.Writer) {
	defer h.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case deribitMsg := <-sendCh:
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
			log.Printf("Successfully sent message with change_id = %d to Kafka...\n", deribitMsg.Params.Data.ChangeId)

			if err != nil {
				log.Printf("Failed to send to Kafka: %v", err)
			}
		}
	}
}

func (h *InstrumentHandler) handleMessages(sendCh chan<- DeribitMessage, ctx context.Context) {
	defer h.wg.Done()
	var prevChangeId *int64

	for {
		select {
		case <-ctx.Done():
			return
		default:
			_, msg, err := h.conn.ReadMessage()
			if err != nil {
				log.Printf("Websocket read error: %v. Retrying...", err)
				continue
			}

			log.Printf("msg = %s\n", string(msg))

			var deribitMsg DeribitMessage
			if err := json.Unmarshal(msg, &deribitMsg); err != nil {
				log.Printf("Failed to unmarshal message: %v", err)
				continue
			}

			log.Println(deribitMsg)

			if deribitMsg.Method != "subscription" {
				continue // Skip non-orderbook messages
			}

			if (prevChangeId != nil) && (deribitMsg.Params.Data.PrevChangeId != nil) && (*prevChangeId != *deribitMsg.Params.Data.PrevChangeId) {
				h.conn.Close()
				h.conn = h.connectToDeribit()
				h.subscribeToOrderBook()
				prevChangeId = nil
				continue
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

func main() {
	logFile, err := os.OpenFile("app.log", os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer logFile.Close()

	log.SetOutput(logFile)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	var checkerWg sync.WaitGroup

	var c Checker
	c.Init(ctx, &checkerWg)

	kafkaWriter := &kafka.Writer{
		Addr:         kafka.TCP("localhost:9092"),
		Topic:        kafkaTopic,
		RequiredAcks: kafka.RequireAll,
	}
	defer kafkaWriter.Close()

	instruments := []string{"BTC-30MAY25-74000-C", "BTC-30MAY25-74000-P", "BTC-4APR25-97000-C", "BTC-27JUN25-44000-P"}

	for _, instrument := range instruments {
		instrumentHandler := InstrumentHandler{instrument: instrument, wg: &wg}
		instrumentHandler.init(ctx, kafkaWriter)
	}

	wg.Wait()
	checkerWg.Wait()
}
