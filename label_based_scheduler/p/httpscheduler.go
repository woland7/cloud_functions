/*
Package p HTTPScheduler version
*/
package p

import (
	"encoding/json"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"io"
	"log"
	"net/http"
)

func init() {
	functions.HTTP("HTTPScheduler", HTTPScheduler)
}

/*
HTTPScheduler wrapper around Scheduler
*/
func HTTPScheduler(_ http.ResponseWriter, r *http.Request) {
	var payload Payload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		switch err {
		case io.EOF:
			log.Fatal("Error, exiting")
		default:
			log.Printf("json.NewDecoder: %v\n", err)
		}
	}

	log.Println(payload)
	ValidatePayload(payload)
	err := Scheduler(payload)
	if err != nil {
		log.Println("All good.")
	}
}
