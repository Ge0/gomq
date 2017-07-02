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
	"io"
	"time"
)

var src = rand.NewSource(time.Now().UnixNano())
var messagesCache map[string]pb.Message

func main() {

	var keyArg, consumerIdArg string
	flag.StringVar(&keyArg, "key", "key", "Key to observe and publish to.")
	flag.StringVar(&consumerIdArg, "consumer_id", "BASE_CLIENT", "Consumer ID to use.")
	flag.Parse()

	fmt.Printf("Launching client...\n")
	messagesCache := make(map[string]pb.Message)
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	conn, err := grpc.Dial("kojiro.gcir.ovh:10001", opts...)
	if err != nil {
		grpclog.Fatalf("Fail to dial: %v", err)
		return
	}
	defer conn.Close()

	client := pb.NewRouteGuideClient(conn)

	// TODO: subscribe to key
	result, err := client.Subscribe(context.Background(), &pb.Subscription{&pb.Key{keyArg}, consumerIdArg})
	fmt.Printf("Result is: %v & error is %v\n", result.Code, err)

	// Observation
	for true {
		identification := pb.Identification{consumerIdArg}
		stream, _ := client.Observe(context.Background(), &identification)
		for {
			in, err := stream.Recv()
			if err == io.EOF {
				break
			}

			if _, ok := messagesCache[in.Value.Id]; !ok {
				// Message not in cache: save it
				messagesCache[in.Value.Id] = *in.Value
				fmt.Printf("[+] (ID: %s) %s\n", in.Value.Id, string(in.Value.Payload))
				if len(messagesCache) > 30000 {
					messagesCache = make(map[string]pb.Message)
				}
			}
		}
	}
}
