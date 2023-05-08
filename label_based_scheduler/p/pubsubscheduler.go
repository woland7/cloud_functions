/*
Package p PubSubScheduler version
*/
package p

import (
	"encoding/json"
	"log"
)

/*
PubSubMessage struct received from GoogleScheduler
*/
type PubSubMessage struct {
	Data []byte `json:",omitempty"`
}

/*
PubSubScheduler wrapper around Scheduler
*/
func PubSubScheduler(m PubSubMessage) error {
	var payload Payload
	err := json.Unmarshal(m.Data, &payload)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(payload)
	ValidatePayload(payload)
	Scheduler(payload)
	return nil
}
