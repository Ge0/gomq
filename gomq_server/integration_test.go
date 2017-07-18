package main

import (
	pb "../routeguide"
	"github.com/rs/xid"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	//"log"
	"json"
	"os/exec"
	"strconv"
	"testing"
	"time"
)

type Client struct {
	route      pb.RouteGuideClient
	connection *grpc.ClientConn
	consumerID string
}

func setupClient() Client {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	conn, _ := grpc.Dial(":10001", opts...)
	return Client{
		route:      pb.NewRouteGuideClient(conn),
		connection: conn,
	}
}

func TestLotsOfConsumersLotsOfSubscribersServer(t *testing.T) {
	cmd := exec.Command("./gomq_server")
	err := cmd.Start()
	if err != nil {
		t.Fatal("Could not start server for tests!\n")
	}
	time.Sleep(5 * time.Second) // Wait for server to spawn

	key := "KEY"

	consumers := make([]Client, 100)
	for i := 0; i < 100; i++ {
		consumers[i] = setupClient()
		consumers[i].consumerID = "CONSUMER_" + strconv.Itoa(i)
		consumers[i].route.Subscribe(context.Background(), &pb.Subscription{key, consumers[i].consumerID})
	}

	time.Sleep(2 * time.Second) // Wait for subscriptions

	publishers := make([]Client, 100)
	for i := 0; i < 100; i++ {
		publishers[i] = setupClient()
		publishers[i].consumerID = "PUBLISHER_" + strconv.Itoa(i)
	}

	for i := 0; i < 100; i++ {
		for j := 0; j < 10; j++ {
			publishers[i].route.Publish(context.Background(), &pb.PublishRecord{key, []byte(xid.New().String())})
		}
	}

	for i := 0; i < 100; i++ {
		var messagesReceived []string
		identification := pb.Identification{consumers[i].consumerID}
		for true {
			recordSet, _ := consumers[i].route.Observe(context.Background(), &identification)
			if len(recordSet.Records) == 0 {
				break
			}
			for _, record := range recordSet.Records {
				messagesReceived = append(messagesReceived, string(record.Value.Payload))
			}
		}
		if len(messagesReceived) != 100*10 {
			t.Fatal("Number of messages received by consumer", consumers[i].consumerID, "(", len(messagesReceived), ") do not match.")
		}
	}

	cmd.Process.Kill()
}

func TestMultipleConsumersMultipleSubscribers(t *testing.T) {
	cmd := exec.Command("./gomq_server")
	err := cmd.Start()
	if err != nil {
		t.Fatal("Could not start server for tests!\n")
	}
	time.Sleep(5 * time.Second)

	consumer1 := setupClient()
	consumer2 := setupClient()

	publisher1 := setupClient()
	publisher2 := setupClient()

	defer consumer1.connection.Close()
	defer consumer2.connection.Close()

	defer publisher1.connection.Close()
	defer publisher2.connection.Close()

	key1 := "key1"
	key2 := "key2"

	var messagesReceivedConsumer1 []string
	var messagesReceivedConsumer2 []string

	var messagesSentPublisher1 []string
	var messagesSentPublisher2 []string

	for i := 0; i < 1000; i++ {
		val1 := xid.New().String()
		val2 := xid.New().String()
		publisher1.route.Publish(context.Background(), &pb.PublishRecord{key1, []byte(val1)})
		messagesSentPublisher1 = append(messagesSentPublisher1, val1)

		if i%30 == 0 {
			publisher2.route.Publish(context.Background(), &pb.PublishRecord{key2, []byte(val2)})
			messagesSentPublisher2 = append(messagesSentPublisher2, val2)
		}
	}

	consumer1.route.Subscribe(context.Background(), &pb.Subscription{key1, "CLIENT_1"})
	consumer1.route.Subscribe(context.Background(), &pb.Subscription{key2, "CLIENT_1"})
	consumer2.route.Subscribe(context.Background(), &pb.Subscription{key2, "CLIENT_2"})

	time.Sleep(5 * time.Second) // Wait for queues to be dequeued

	identification1 := pb.Identification{"CLIENT_1"}
	for true {
		recordSet, _ := consumer1.route.Observe(context.Background(), &identification1)
		if len(recordSet.Records) == 0 {
			break
		}
		for _, record := range recordSet.Records {
			messagesReceivedConsumer1 = append(messagesReceivedConsumer1, string(record.Value.Payload))
		}
	}

	identification2 := pb.Identification{"CLIENT_2"}
	for true {
		recordSet, _ := consumer2.route.Observe(context.Background(), &identification2)
		if len(recordSet.Records) == 0 {
			break
		}
		for _, record := range recordSet.Records {
			messagesReceivedConsumer2 = append(messagesReceivedConsumer2, string(record.Value.Payload))
		}
	}

	if len(messagesReceivedConsumer1) != len(messagesSentPublisher1)+len(messagesSentPublisher2) {
		t.Fatal("Number of messages received by the first consumer do not match.")
	}

	if len(messagesReceivedConsumer2) != len(messagesSentPublisher2) {
		t.Fatal("Number of messages received by the second consumer do not match.")
	}
	cmd.Process.Kill()
}

func TestUnsubscribe(t *testing.T) {
	cmd := exec.Command("./gomq_server")
	err := cmd.Start()
	if err != nil {
		t.Fatal("Could not start server for tests!\n")
	}
	time.Sleep(5 * time.Second) // Wait for server to spawn
	client := setupClient()
	defer client.connection.Close()

	key := xid.New().String()

	client.route.Subscribe(context.Background(), &pb.Subscription{key, "CLIENT"})
	client.route.Publish(context.Background(), &pb.PublishRecord{key, []byte(xid.New().String())})
	client.route.Publish(context.Background(), &pb.PublishRecord{key, []byte(xid.New().String())})
	time.Sleep(1 * time.Second) // Wait for subscription

	// Fetch messages
	identification := pb.Identification{"CLIENT"}
	recordSet, _ := client.route.Observe(context.Background(), &identification)
	if len(recordSet.Records) != 2 {
		t.Fatal("Number of messages received (", len(recordSet.Records), ") is invalid.")
	}

	client.route.Unsubscribe(context.Background(), &pb.Subscription{key, "CLIENT"})
	time.Sleep(1 * time.Second) // Wait for unsubscription

	client.route.Publish(context.Background(), &pb.PublishRecord{key, []byte(xid.New().String())})
	client.route.Publish(context.Background(), &pb.PublishRecord{key, []byte(xid.New().String())})

	time.Sleep(1 * time.Second)

	// Fetch messages
	identification = pb.Identification{"CLIENT"}
	recordSet, _ = client.route.Observe(context.Background(), &identification)
	if len(recordSet.Records) != 0 {
		t.Fatal("There should not be any message received.")
	}

	cmd.Process.Kill()
}

func TestOneConsumerMultipleSubscribers(t *testing.T) {

	cmd := exec.Command("./gomq_server")
	err := cmd.Start()
	if err != nil {
		t.Fatal("Could not start server for tests!\n")
	}
	time.Sleep(5 * time.Second) // Wait for server to spawn
	consumer := setupClient()
	publisher1 := setupClient()
	publisher2 := setupClient()

	defer consumer.connection.Close()
	defer publisher1.connection.Close()
	defer publisher2.connection.Close()

	var messagesSent []string
	var messagesReceived []string

	key := xid.New().String()

	consumer.route.Subscribe(context.Background(), &pb.Subscription{key, "CLIENT"})
	time.Sleep(1 * time.Second)

	for i := 0; i < 1000; i++ {
		val1 := xid.New().String()
		val2 := xid.New().String()
		publisher1.route.Publish(context.Background(), &pb.PublishRecord{key, []byte(val1)})
		publisher2.route.Publish(context.Background(), &pb.PublishRecord{key, []byte(val2)})
		messagesSent = append(messagesSent, val1, val2)
	}

	identification := pb.Identification{"CLIENT"}
	for true {
		recordSet, _ := consumer.route.Observe(context.Background(), &identification)
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
