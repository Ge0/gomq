package main

import (
	pb "../routeguide"
	"github.com/rs/xid"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	//"log"
	"os/exec"
	"testing"
	"time"
)

func setupClient() (pb.RouteGuideClient, *grpc.ClientConn) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	conn, _ := grpc.Dial(":10001", opts...)
	return pb.NewRouteGuideClient(conn), conn
}

func TestMultipleConsumersMultipleSubscribers(t *testing.T) {
	cmd := exec.Command("./gomq_server")
	err := cmd.Start()
	if err != nil {
		t.Fatal("Could not start server for tests!\n")
	}
	time.Sleep(5 * time.Second)

	consumer1, connConsumer1 := setupClient()
	consumer2, connConsumer2 := setupClient()

	publisher1, connPublisher1 := setupClient()
	publisher2, connPublisher2 := setupClient()

	defer connConsumer1.Close()
	defer connConsumer2.Close()

	defer connPublisher1.Close()
	defer connPublisher2.Close()

	key1 := "key1"
	key2 := "key2"

	var messagesReceivedConsumer1 []string
	var messagesReceivedConsumer2 []string

	var messagesSentPublisher1 []string
	var messagesSentPublisher2 []string

	for i := 0; i < 1000; i++ {
		val1 := xid.New().String()
		val2 := xid.New().String()
		publisher1.Publish(context.Background(), &pb.PublishRecord{key1, []byte(val1)})
		messagesSentPublisher1 = append(messagesSentPublisher1, val1)

		if i%30 == 0 {
			publisher2.Publish(context.Background(), &pb.PublishRecord{key2, []byte(val2)})
			messagesSentPublisher2 = append(messagesSentPublisher2, val2)
		}
	}

	consumer1.Subscribe(context.Background(), &pb.Subscription{key1, "CLIENT_1"})
	consumer1.Subscribe(context.Background(), &pb.Subscription{key2, "CLIENT_1"})
	consumer2.Subscribe(context.Background(), &pb.Subscription{key2, "CLIENT_2"})
	time.Sleep(1 * time.Second) // Flaky: wait for subscriptions

	identification1 := pb.Identification{"CLIENT_1"}
	for true {
		recordSet, _ := consumer1.Observe(context.Background(), &identification1)
		if len(recordSet.Records) == 0 {
			break
		}
		for _, record := range recordSet.Records {
			messagesReceivedConsumer1 = append(messagesReceivedConsumer1, string(record.Value.Payload))
		}
	}

	identification2 := pb.Identification{"CLIENT_2"}
	time.Sleep(1 * time.Second)
	for true {
		recordSet, _ := consumer2.Observe(context.Background(), &identification2)
		if len(recordSet.Records) == 0 {
			break
		}
		for _, record := range recordSet.Records {
			messagesReceivedConsumer2 = append(messagesReceivedConsumer2, string(record.Value.Payload))
		}
	}

	cmd.Process.Kill()

	if len(messagesReceivedConsumer1) != len(messagesSentPublisher1)+len(messagesSentPublisher2) {
		t.Fatal("Number of messages received by the first consumer do not match.")
	}

	if len(messagesReceivedConsumer2) != len(messagesSentPublisher2) {
		t.Fatal("Number of messages received bu the second consumer do not match.")
	}
}

func TestUnsubscribe(t *testing.T) {
	cmd := exec.Command("./gomq_server")
	err := cmd.Start()
	if err != nil {
		t.Fatal("Could not start server for tests!\n")
	}
	time.Sleep(5 * time.Second) // Wait for server to spawn
	client, connClient := setupClient()
	defer connClient.Close()

	key := xid.New().String()

	client.Subscribe(context.Background(), &pb.Subscription{key, "CLIENT"})
	client.Publish(context.Background(), &pb.PublishRecord{key, []byte(xid.New().String())})
	client.Publish(context.Background(), &pb.PublishRecord{key, []byte(xid.New().String())})
	time.Sleep(1 * time.Second) // Wait for subscription

	// Fetch messages
	identification := pb.Identification{"CLIENT"}
	recordSet, _ := client.Observe(context.Background(), &identification)
	if len(recordSet.Records) != 2 {
		t.Fatal("Number of messages received is invalid.")
	}

	client.Unsubscribe(context.Background(), &pb.Subscription{key, "CLIENT"})
	time.Sleep(1 * time.Second) // Wait for unsubscription

	client.Publish(context.Background(), &pb.PublishRecord{key, []byte(xid.New().String())})
	client.Publish(context.Background(), &pb.PublishRecord{key, []byte(xid.New().String())})

	time.Sleep(1 * time.Second)

	// Fetch messages
	identification = pb.Identification{"CLIENT"}
	recordSet, _ = client.Observe(context.Background(), &identification)
	if len(recordSet.Records) != 0 {
		t.Fatal("There should not be any message received.")
	}

}

func TestOneConsumerMultipleSubscribers(t *testing.T) {

	cmd := exec.Command("./gomq_server")
	err := cmd.Start()
	if err != nil {
		t.Fatal("Could not start server for tests!\n")
	}
	time.Sleep(5 * time.Second) // Wait for server to spawn
	consumer, connConsumer := setupClient()
	publisher1, connPublisher1 := setupClient()
	publisher2, connPublisher2 := setupClient()

	defer connConsumer.Close()
	defer connPublisher1.Close()
	defer connPublisher2.Close()

	var messagesSent []string
	var messagesReceived []string

	key := xid.New().String()

	consumer.Subscribe(context.Background(), &pb.Subscription{key, "CLIENT"})
	time.Sleep(1 * time.Second)

	for i := 0; i < 1000; i++ {
		val1 := xid.New().String()
		val2 := xid.New().String()
		publisher1.Publish(context.Background(), &pb.PublishRecord{key, []byte(val1)})
		publisher2.Publish(context.Background(), &pb.PublishRecord{key, []byte(val2)})
		messagesSent = append(messagesSent, val1, val2)
	}

	identification := pb.Identification{"CLIENT"}
	for true {
		recordSet, _ := consumer.Observe(context.Background(), &identification)
		if len(recordSet.Records) == 0 {
			break
		}
		for _, record := range recordSet.Records {
			messagesReceived = append(messagesReceived, string(record.Value.Payload))
		}
	}

	cmd.Process.Kill()
	if len(messagesReceived) != len(messagesSent) {
		t.Fatal("Messages received and messages sent do not match.")
	}
}
