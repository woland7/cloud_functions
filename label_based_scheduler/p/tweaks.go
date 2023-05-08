/*
Package p collection of tweaking function
*/
package p

import (
	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	container "cloud.google.com/go/container/apiv1"
	"cloud.google.com/go/container/apiv1/containerpb"
	"cloud.google.com/go/storage"
	"context"
	"encoding/json"
	"fmt"
	"google.golang.org/api/iterator"
	"log"
	"strings"
	"time"
)

/*
TweakAutoScaler func autoscaling given NodePool
*/
func TweakAutoScaler(ctx context.Context, nodePoolName string, minCount int32, maxCount int32, enabled bool, c *container.ClusterManagerClient) string {
	log.Printf("First, %s's autoscaling will be set to %t", nodePoolName, enabled)
	req := &containerpb.SetNodePoolAutoscalingRequest{
		Name:        nodePoolName,
		Autoscaling: &containerpb.NodePoolAutoscaling{Enabled: enabled, MinNodeCount: minCount, MaxNodeCount: maxCount},
	}
	resp, err := c.SetNodePoolAutoscaling(ctx, req)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(resp.String())
	return resp.Name
}

/*
TweakVM func either starting or stopping given VM instance
*/
func TweakVM(ctx context.Context, vmName string, project string, zone string, action string, c *compute.InstancesClient) {
	resp, err := c.Get(ctx, &computepb.GetInstanceRequest{Instance: vmName, Project: project, Zone: zone})
	if err != nil {
		log.Fatal(err)
	}
	status := resp.GetStatus()
	if action == "stop" && status != (computepb.Instance_TERMINATED).String() {
		log.Printf("%s will be shut down...", vmName)
		req := &computepb.StopInstanceRequest{Zone: zone, Instance: vmName, Project: project}
		resp, err := c.Stop(ctx, req)
		if err != nil {
			log.Fatal(err)
		}
		err = resp.Wait(ctx)
		if err != nil {
			log.Fatal(err.Error())
		}
	} else if action == "start" && status != (computepb.Instance_RUNNING).String() {
		log.Printf("%s will be started up...", vmName)
		req := &computepb.StartInstanceRequest{Zone: zone, Instance: vmName, Project: project}
		resp, err := c.Start(ctx, req)
		if err != nil {
			log.Fatal(err)
		}
		err = resp.Wait(ctx)
		if err != nil {
			log.Fatal(err.Error())
		}
	}
}

/*
TweakNP func either starting or stopping given NodePool
*/
func TweakNP(ctx context.Context, nodePoolName string, nodeCount int32, c *container.ClusterManagerClient) string {
	log.Printf("%s will be resized to %d", nodePoolName, nodeCount)
	req := &containerpb.SetNodePoolSizeRequest{
		NodeCount: nodeCount,
		Name:      nodePoolName,
	}
	resp, err := c.SetNodePoolSize(ctx, req)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(resp.String())
	return resp.Name
}

func init() {
	log.SetFlags(log.Llongfile)
}

/*
Scheduler func containing main logic to schedule NodePools and VMs
*/
func Scheduler(payload Payload) error {
	log.Println("Initialize clusterClient...")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	c, err := container.NewClusterManagerClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer func(c *container.ClusterManagerClient) {
		_ = c.Close()
	}(c)

	log.Println("Initialize storageClient...")
	s, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer func(s *storage.Client) {
		_ = s.Close()
	}(s)

	rc, _ := s.Bucket(payload.Bucket).Object(payload.Object).NewReader(ctx)
	defer func(rc *storage.Reader) {
		_ = rc.Close()
	}(rc)

	log.Println("Initialize computeClient...")
	computeClient, err := compute.NewInstancesRESTClient(ctx)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer func(computeClient *compute.InstancesClient) {
		_ = computeClient.Close()
	}(computeClient)

	log.Println("Checking if any cluster operations are pending...")
	reqList := &containerpb.ListOperationsRequest{Parent: fmt.Sprintf("projects/%s/locations/-", payload.Project)}
	respList, err := c.ListOperations(ctx, reqList)
	if err != nil {
		log.Fatal(err.Error())
	}
	for _, op := range respList.GetOperations() {
		if op.GetStatus().String() == (containerpb.Operation_RUNNING).String() &&
			(op.GetOperationType().String() != (containerpb.Operation_SET_NODE_POOL_SIZE).String() ||
				op.GetOperationType().String() != (containerpb.Operation_UPDATE_CLUSTER).String()) {
			log.Printf("Waiting for %s to complete...", op.GetName())
			AwaitsOperation(ctx, fmt.Sprintf("projects/%s/locations/%s", payload.Project,
				op.GetSelfLink()[strings.LastIndex(op.GetSelfLink(), "zones/")+6:]), c)
			log.Printf("Operation %s completed...", op.GetName())
		}
	}

	nodePools := NodePools{}
	err = json.NewDecoder(rc).Decode(&nodePools)
	if err != nil {
		log.Fatal(err.Error())
	}

	reqListClusters := &containerpb.ListClustersRequest{
		Parent: fmt.Sprintf("projects/%s/locations/-", payload.Project),
	}
	respListClusters, err := c.ListClusters(ctx, reqListClusters)
	if err != nil {
		log.Fatal(err)
	}
	for _, cl := range respListClusters.Clusters {
		log.Printf("Tweaking %s cluster's nodePools\n", cl.Name)
		for _, np := range cl.NodePools {
			log.Printf("Tweaking nodepool %s\n", np.Name)
			if !AreLabelsContained(payload.Labels, np.Config.Labels) {
				log.Printf("Skipping %s...\n", np.Name)
				continue
			}
			log.Printf("The labels in the payload are a subset of the labels of the nodepool %s\n", np.Name)
			nodePoolCustom := nodePools.NodePools[np.Name]
			nodePoolName := fmt.Sprintf("projects/%s/locations/%s/clusters/%s/nodePools/%s",
				payload.Project, cl.Location, cl.Name, np.Name)
			var minCount int32
			var maxCount int32
			var asEnabled = false
			var formatString = "projects/%s/locations/%s/operations/%s"
			if payload.Action == "start" {
				asEnabled = true
				minCount = nodePoolCustom.MinCount
				maxCount = nodePoolCustom.MaxCount
				if nodePoolCustom.AsEnabled && !np.Autoscaling.Enabled {
					AwaitsOperation(context.Background(), fmt.Sprintf(formatString, payload.Project, cl.Location,
						TweakAutoScaler(context.Background(), nodePoolName, minCount, maxCount, asEnabled, c)), c)
				} else {
					AwaitsOperation(context.Background(), fmt.Sprintf(formatString, payload.Project, cl.Location,
						TweakNP(context.Background(), nodePoolName, maxCount, c)), c)
				}
			} else if payload.Action == "stop" {
				log.Println(nodePoolCustom)
				if nodePoolCustom.AsEnabled && np.Autoscaling.Enabled {
					AwaitsOperation(context.Background(), fmt.Sprintf(formatString, payload.Project, cl.Location,
						TweakAutoScaler(context.Background(), nodePoolName, minCount, maxCount, asEnabled, c)), c)
				}
				newNep, _ := c.GetNodePool(ctx, &containerpb.GetNodePoolRequest{
					Name: nodePoolName,
				})
				log.Printf("%t %d %d\n", newNep.Autoscaling.Enabled, newNep.Autoscaling.MinNodeCount, newNep.Autoscaling.MaxNodeCount)
				log.Printf("%d", newNep.InitialNodeCount)
				AwaitsOperation(context.Background(), fmt.Sprintf(formatString, payload.Project, cl.Location,
					TweakNP(context.Background(), nodePoolName, maxCount, c)), c)
			}
		}
	}

	log.Println("Working with VMs...")
	reqCompute := &computepb.AggregatedListInstancesRequest{Project: payload.Project}
	it := computeClient.AggregatedList(ctx, reqCompute)
	for {
		resp, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatal(err.Error())
		}
		for _, i := range resp.Value.GetInstances() {
			if AreLabelsContained(payload.Labels, i.Labels) {
				TweakVM(ctx, i.GetName(), payload.Project, i.GetZone()[strings.LastIndex(i.GetZone(), "/")+1:], payload.Action, computeClient)
			}
		}
	}
	log.Printf("Exiting...\n")
	return nil
}
