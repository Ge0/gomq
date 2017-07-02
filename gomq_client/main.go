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

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func RandStringBytesMaskImprSrc(n int) string {
	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}

type DatabaseRecord struct {
	Timestamp time.Time
	Payload   []byte
}

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
		// Publish something?
		key := pb.Key{keyArg}
		record := pb.PublishRecord{&key, []byte(RandStringBytesMaskImprSrc(8))}
		_, err = client.Publish(context.Background(), &record)
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
			}

		}
	}
}
