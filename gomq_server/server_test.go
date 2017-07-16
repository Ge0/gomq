package main

import (
	pb "../routeguide"
	"github.com/rs/xid"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"testing"
	"time"
	//	"os"
	"os/exec"
)

func setupClient() (pb.RouteGuideClient, *grpc.ClientConn) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	conn, _ := grpc.Dial(":10001", opts...)
	return pb.NewRouteGuideClient(conn), conn
}

func TestServer(t *testing.T) {

	cmd := exec.Command("./gomq_server")
	err := cmd.Start()
	if err != nil {
		t.Fatal("Could not start server for tests!\n")
	}
	time.Sleep(5 * time.Second)
	clt1, conn1 := setupClient()
	clt2, conn2 := setupClient()
	clt3, conn3 := setupClient()

	defer conn1.Close()
	defer conn2.Close()
	defer conn3.Close()

	var messagesSent []string
	var messagesReceived []string

	key := xid.New().String()

	clt1.Subscribe(context.Background(), &pb.Subscription{key, "CLIENT"})
	time.Sleep(1 * time.Second)

	for i := 0; i < 1000; i++ {
		val1 := xid.New().String()
		val2 := xid.New().String()
		clt2.Publish(context.Background(), &pb.PublishRecord{key, []byte(val1)})
		clt3.Publish(context.Background(), &pb.PublishRecord{key, []byte(val2)})
		messagesSent = append(messagesSent, val1, val2)
	}

	identification := pb.Identification{"CLIENT"}
	for true {
		recordSet, _ := clt1.Observe(context.Background(), &identification)
		if len(recordSet.Records) == 0 {
			break
		}
		for _, record := range recordSet.Records {
			messagesReceived = append(messagesReceived, string(record.Value.Payload))
		}
	}

	if len(messagesReceived) != len(messagesSent) {
		t.Fatal("Messages received and messages sent do not match.")
	}
	cmd.Process.Kill()
}
