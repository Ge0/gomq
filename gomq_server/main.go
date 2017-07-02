package main

import (
	pb "../routeguide"
	"encoding/json"
	"github.com/go-redis/redis"
	"github.com/rs/xid"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"net"
	"sync"
	"time"
)

var subscribers map[string]map[string]bool
var lockSubscribers, lockRedisQueue sync.RWMutex

type routeGuideServer struct {
}

func GenerateNewId() string {
	guid := xid.New()
	return guid.String()
}

func IsSubscriber(consumerID string, key string) bool {
	var ret bool = false
	lockSubscribers.RLock()
	if subscriptions, ok := subscribers[consumerID]; ok {
		if _, ok := subscriptions[key]; ok {
			ret = true
		}
	}
	lockSubscribers.RUnlock()
	return ret
}

func Subscribe(consumerID string, key string) {
	lockSubscribers.Lock()
	if _, ok := subscribers[consumerID]; !ok {
		subscribers[consumerID] = make(map[string]bool)
	}
	subscribers[consumerID][key] = true // Register consumer
	lockSubscribers.Unlock()
}

func Unsubscribe(consumerID string, key string) {
	if IsSubscriber(key, consumerID) {
		lockSubscribers.Lock()
		delete(subscribers[consumerID], key)
		lockSubscribers.Unlock()
	}
}

func GetRedisConnection() (*redis.Client, error) {
	var err error = nil
	redis_client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	_, err = redis_client.Ping().Result()
	return redis_client, err
}

func CleanQueue(redisConn *redis.Client, key string) {
	queueSize, _ := redisConn.LLen(key).Result()
	if queueSize > 50 {
		redisConn.LTrim(key, queueSize-50, -1)
	}
}

func (s *routeGuideServer) Publish(ctx context.Context, record *pb.PublishRecord) (*pb.Result, error) {
	// Check the key
	redisConn, err := GetRedisConnection()
	if err != nil {
		return &pb.Result{-1}, err
	}
	defer redisConn.Close()

	db_record := pb.Message{GenerateNewId(), time.Now().Format(time.UnixDate), record.Payload}
	marshalled_value, err := json.Marshal(db_record)
	redisConn.RPush(record.Key.Content, string(marshalled_value)).Err()
	return &pb.Result{0}, nil
}

func (s *routeGuideServer) Subscribe(ctx context.Context, subscription *pb.Subscription) (*pb.Result, error) {
	key := subscription.Key.Content
	consumerID := subscription.ConsumerID

	if IsSubscriber(consumerID, key) {
		log.Printf("%s already observing %s!\n", consumerID, key)
	} else {
		Subscribe(consumerID, key)
	}
	log.Printf("Subscribe %s to %s!\n", consumerID, key)

	result := pb.Result{0}
	return &result, nil
}

func (s *routeGuideServer) Unsubscribe(ctx context.Context, subscription *pb.Subscription) (*pb.Result, error) {
	key := subscription.Key.Content
	consumerID := subscription.ConsumerID
	Unsubscribe(consumerID, key)
	log.Printf("Unsubscribe %s from %s!\n", consumerID, key)
	result := pb.Result{0}
	return &result, nil
}

func (s *routeGuideServer) Observe(identification *pb.Identification, stream pb.RouteGuide_ObserveServer) error {
	redisConn, _ := GetRedisConnection()
	defer redisConn.Close()
	var consumerID string = identification.ConsumerID
	for key, _ := range subscribers[consumerID] {
		results, _ := redisConn.LRange(key, 0, -1).Result()
		for _, result := range results {
			dbRecord := new(pb.Message)
			json.Unmarshal([]byte(result), &dbRecord)
			record := pb.Record{&pb.Key{key}, dbRecord}
			stream.Send(&record)
		}
		CleanQueue(redisConn, key)
	}
	return nil
}

func main() {
	subscribers = make(map[string]map[string]bool)
	lis, err := net.Listen("tcp", ":10001")

	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterRouteGuideServer(grpcServer, &routeGuideServer{})
	log.Println("Server listening...")
	grpcServer.Serve(lis)
}
