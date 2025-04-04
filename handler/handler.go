package handler

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type JsonRpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type SubscribeResponse struct {
	Error  JsonRpcError `json:"error"`
	Result []string     `json:"result"`
}

type InstrumentResponse struct {
	Error  JsonRpcError `json:"error"`
	Result []struct {
		InstrumentName string `json:"instrument_name"`
	}
}

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
	Error  JsonRpcError `json:"error"`
	Method string       `json:"method"`
	Params struct {
		Channel string    `json:"channel"`
		Data    OrderBook `json:"data"`
	} `json:"params"`
}

type InstrumentHandler struct {
	wg     *sync.WaitGroup
	sendCh chan DeribitMessage
}

func (h *InstrumentHandler) Init(ctx context.Context, wg *sync.WaitGroup, kafkaWriter *kafka.Writer, instrumentCh <-chan DeribitMessage, resubscribeCh chan<- string) {
	h.wg = wg
	h.sendCh = make(chan DeribitMessage)

	h.wg.Add(2)
	go h.sendToKafka(ctx, kafkaWriter)
	go h.handleMessages(ctx, instrumentCh, resubscribeCh)
}

func (h *InstrumentHandler) sendToKafka(ctx context.Context, kafkaWriter *kafka.Writer) {
	defer h.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case deribitMsg := <-h.sendCh:
			// Construct Kafka message
			dataBytes, err := json.Marshal(deribitMsg.Params.Data)
			if err != nil {
				log.Printf("Error marshaling data: %v", err)
				continue
			}

			kafkaMessage := kafka.Message{
				Key:   []byte(deribitMsg.Params.Data.Instrument),
				Value: dataBytes,
				Time:  time.Unix(0, deribitMsg.Params.Data.Timestamp*int64(time.Millisecond)),
			}
			log.Printf("[KAFKA PRODUCER] KEY: %s, VALUE: %s\n", string(kafkaMessage.Key), string(kafkaMessage.Value))

			// Send to Kafka
			err = kafkaWriter.WriteMessages(ctx, kafkaMessage)
			if err != nil {
				log.Printf("Failed to send to Kafka: %v", err)
			}
		}
	}
}

func (h *InstrumentHandler) handleMessages(ctx context.Context, instrumentCh <-chan DeribitMessage, resubscribeCh chan<- string) {
	defer h.wg.Done()
	var prevChangeId *int64

	for {
		select {
		case <-ctx.Done():
			close(h.sendCh)
			return
		case deribitMsg := <-instrumentCh:
			// Check if there's a message lost
			if (prevChangeId != nil) && (deribitMsg.Params.Data.PrevChangeId != nil) && (*prevChangeId != *deribitMsg.Params.Data.PrevChangeId) {
				// Reconnect to websocket
				resubscribeCh <- deribitMsg.Params.Data.Instrument
			}

			prevChangeId = &deribitMsg.Params.Data.ChangeId

			// Send to kafka
			h.wg.Add(1)
			go func() {
				defer h.wg.Done()
				h.sendCh <- deribitMsg
			}()
		}
	}
}
