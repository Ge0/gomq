package main

import (
	"flag"
	"fmt"
	"golang.org/x/net/context"
	"math/rand"
	//"encoding/json"
	pb "../routeguide"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
	//"io"
	"time"
	//"os"
	//"os/signal"
)

var exitLoop bool
var src = rand.NewSource(time.Now().UnixNano())
var messagesCache map[string]pb.Message

func main() {

	var keyArg, consumerIdArg string
	flag.StringVar(&keyArg, "key", "key", "Key to observe and publish to.")
	flag.StringVar(&consumerIdArg, "consumer_id", "BASE_CLIENT", "Consumer ID to use.")
	flag.Parse()

	fmt.Printf("Launching client...\n")
	//messagesCache := make(map[string]pb.Message)
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	conn, err := grpc.Dial(":10001", opts...)
	if err != nil {
		grpclog.Fatalf("Fail to dial: %v", err)
		return
	}
	defer conn.Close()

	client := pb.NewRouteGuideClient(conn)

	// TODO: subscribe to key
	result, err := client.Subscribe(context.Background(), &pb.Subscription{keyArg, consumerIdArg})
	fmt.Printf("Result is: %v & error is %v\n", result.Code, err)

	for true {
		identification := pb.Identification{consumerIdArg}
		recordSet, _ := client.Observe(context.Background(), &identification)
		for _, record := range recordSet.Records {
			fmt.Printf("[%s] %s\n", record.Key, record.Value.Payload)
		}
	}
}
