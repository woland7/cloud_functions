/*
Package p collection of utils function
*/
package p

import (
	container "cloud.google.com/go/container/apiv1"
	"cloud.google.com/go/container/apiv1/containerpb"
	"context"
	"golang.org/x/exp/slices"
	"log"
	"time"
)

/*
NodePools array of NodePoolCustomConf
*/
type NodePools struct {
	NodePools map[string]NodePoolCustomConf `json:"node_pools"`
}

/*
NodePoolCustomConf custom config for a NodePool
*/
type NodePoolCustomConf struct {
	AsEnabled bool  `json:"as_enabled"`
	MinCount  int32 `json:"mincount,omitempty"`
	MaxCount  int32 `json:"maxcount"`
}

/*
Payload struct representing the payload to pass to Scheduler
*/
type Payload struct {
	Labels  map[string]string `json:"labels"`
	Project string            `json:"project"`
	Action  string            `json:"action"`
	Bucket  string            `json:"bucket"`
	Object  string            `json:"object"`
}

/*
AreLabelsContained check if a label is a subset of another
*/
func AreLabelsContained(subset map[string]string, set map[string]string) bool {
	check := true
	for key, val := range subset {
		if val != set[key] {
			log.Printf("Label %s=%v is not contained.\n", key, val)
			check = false
			break
		}
	}
	return check
}

/*
ValidatePayload validate payload function
*/
func ValidatePayload(m Payload) {
	valideActions := []string{"start", "stop"}
	if !slices.Contains(valideActions, m.Action) {
		log.Fatalf("Action %s is not valid. Exiting\n", m.Action)
	}
}

/*
AwaitsOperation whenever GoogleAPIs do not provide a blocking call, use this to block until call completion
*/
func AwaitsOperation(ctx context.Context, name string, c *container.ClusterManagerClient) {
	operation, err := c.GetOperation(ctx, &containerpb.GetOperationRequest{Name: name})
	if err != nil {
		log.Fatal(err)
	}
	for operation.Status != 3 {
		log.Println("Waiting for the operation to complete...")
		operation, _ = c.GetOperation(ctx, &containerpb.GetOperationRequest{Name: name})
		time.Sleep(10 * time.Second)
	}
}
