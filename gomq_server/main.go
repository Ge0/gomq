package main

import (
	pb "../routeguide"
	"encoding/json"
	"github.com/go-redis/redis"
	"github.com/rs/xid"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"log"
	"net"
	"sync"
	"time"
)

// First key: subscribed key; second key: consumer IDs
var subscribedKeys map[string]map[string]bool

// First key: consumer id ; second key: subscribed key
var consumers map[string]map[string]bool

var subscribersCursor map[string]int

var lockSubscribers, lockRedisQueue sync.RWMutex

type routeGuideServer struct {
}

type Subscription struct {
	Key        string
	ConsumerID string
	PeerInfo   string
}

type Unsubscription struct {
	Key        string
	ConsumerID string
}

type IncomingMessage struct {
	Key     string
	Payload []byte
}

var subscriptionChannel chan Subscription
var unsubscriptionChannel chan Unsubscription
var messageToStoreChannel chan IncomingMessage
var messageToDispatchChannel chan string
var messageToFetchChannel chan Subscription
var newMessageChannel chan string

var redisConnection *redis.Client

const CONSUMERS_PREFIX = "CONSUMERS_"
const SUBSCRIPTIONS_PREFIX = "SUBSCRIPTIONS_"
const PEERS_PREFIX = "PEERS_"
const MESSAGES_PREFIX = "MESSAGES_"
const QUEUE_PREFIX = "QUEUE_"
const REFERENCED_KEYS = "REFERENCED_KEYS"
const TTL_KEY = 30 * time.Second

func GenerateNewId() string {
	guid := xid.New()
	return guid.String()
}

func Subscriptor() {
	for true {
		newSubscription := <-subscriptionChannel

		redisConn, _ := GetRedisConnection()

		// Add the subscription
		redisKey := CONSUMERS_PREFIX + newSubscription.Key
		redisValue := newSubscription.ConsumerID
		isMember, _ := redisConn.SIsMember(redisKey, redisValue).Result()
		if !isMember {
			redisConn.SAdd(redisKey, redisValue)
		}

		redisKey = SUBSCRIPTIONS_PREFIX + newSubscription.ConsumerID
		redisValue = newSubscription.Key
		isMember, _ = redisConn.SIsMember(redisKey, redisValue).Result()
		if !isMember {
			redisConn.SAdd(redisKey, redisValue)
		}

		// In case where several peers have the same consumer ID,
		// Save the peer
		redisKey = PEERS_PREFIX + newSubscription.ConsumerID
		redisValue = newSubscription.PeerInfo
		isMember, _ = redisConn.SIsMember(redisKey, redisValue).Result()
		if !isMember {
			redisConn.SAdd(redisKey, redisValue)
		}

	}
}

func Unsubscriptor() {
	unsubscriptionQuery := <-unsubscriptionChannel

	redisConn, _ := GetRedisConnection()

	// Warning: if there are more than one peer for one consumer ID,
	// Then the other peers will be unsubscribed as well.
	redisKey := CONSUMERS_PREFIX + unsubscriptionQuery.Key
	redisValue := unsubscriptionQuery.ConsumerID
	isMember, _ := redisConn.SIsMember(redisKey, redisValue).Result()
	if !isMember {
		redisConn.SRem(redisKey, redisValue)
	}

	redisKey = SUBSCRIPTIONS_PREFIX + unsubscriptionQuery.ConsumerID
	redisValue = unsubscriptionQuery.Key
	isMember, _ = redisConn.SIsMember(redisKey, redisValue).Result()
	if !isMember {
		redisConn.SRem(redisKey, redisValue)
	}
}

func MessageReceiver() {
	for true {
		incomingMessage := <-messageToStoreChannel
		redisConn, _ := GetRedisConnection()
		redisKey := QUEUE_PREFIX + incomingMessage.Key
		db_record := pb.Message{GenerateNewId(), time.Now().Format(time.UnixDate), incomingMessage.Payload}
		marshalled_value, _ := json.Marshal(db_record)
		redisConn.RPush(redisKey, string(marshalled_value)).Err()
		redisConn.Expire(redisKey, 30*time.Second)

		messageToDispatchChannel <- incomingMessage.Key
	}
}

func MessageDispatcher() {
	for true {
		key := <-messageToDispatchChannel
		redisConn, _ := GetRedisConnection()
		message, _ := redisConn.LPop(QUEUE_PREFIX + key).Result()

		// Round robin through different consumers
		subscribers, cursor, _ := redisConn.SScan(CONSUMERS_PREFIX+key, 0, "*", 10).Result()
		if len(subscribers) == 0 {
			// If there is no subscribers yet, Push the message back to the main queue
			redisConn.LPush(QUEUE_PREFIX+key, message)
		} else {
			for ok := true; ok; ok = (cursor != 0) {
				for _, subscriber := range subscribers {
					dispatchMessageToPeers(redisConn, key, subscriber, message)
				}
				subscribers, cursor, _ = redisConn.SScan(CONSUMERS_PREFIX+key, cursor, "*", 10).Result()
			}
		}
	}
}

func dispatchMessageToPeers(redisConn *redis.Client, key string, consumerID string, message string) {
	redisPeersKey := PEERS_PREFIX + consumerID

	// Round robin through different peers with same consumer ID
	peers, cursor, _ := redisConn.SScan(redisPeersKey, 0, "*", 10).Result()
	for ok := true; ok; ok = (cursor != 0) {
		for _, peer := range peers {
			redisMessagesKey := MESSAGES_PREFIX + key + "_" + consumerID + "_" + peer
			redisConn.RPush(redisMessagesKey, message)
			redisConn.Expire(redisMessagesKey, 30*time.Second)
		}
		peers, cursor, _ = redisConn.SScan(redisPeersKey, 0, "*", 10).Result()
	}
}

func LaunchGoRoutines() {
	go Subscriptor()
	go Unsubscriptor()
	go MessageReceiver()
	go MessageDispatcher()
}

func IsSubscriber(consumerID string, key string) bool {
	var ret bool = false
	lockSubscribers.RLock()
	if subscriptions, ok := consumers[consumerID]; ok {
		if _, ok := subscriptions[key]; ok {
			ret = true
		}
	}
	lockSubscribers.RUnlock()
	return ret
}
func GetRedisConnection() (*redis.Client, error) {
	_, err := redisConnection.Ping().Result()
	return redisConnection, err
}

func CleanQueue(redisConn *redis.Client, key string) {
	queueSize, _ := redisConn.LLen(key).Result()
	if queueSize > 50 {
		redisConn.LTrim(key, queueSize-50, -1)
	}
}

func (s *routeGuideServer) Publish(ctx context.Context, record *pb.PublishRecord) (*pb.Result, error) {
	messageToStoreChannel <- IncomingMessage{record.Key, record.Payload}
	return &pb.Result{0}, nil
}

func (s *routeGuideServer) Subscribe(ctx context.Context, subscription *pb.Subscription) (*pb.Result, error) {
	peer, _ := peer.FromContext(ctx)
	subscriptionChannel <- Subscription{subscription.Key, subscription.ConsumerID, peer.Addr.String()}
	return &pb.Result{0}, nil
}

func (s *routeGuideServer) Unsubscribe(ctx context.Context, subscription *pb.Subscription) (*pb.Result, error) {
	unsubscriptionChannel <- Unsubscription{subscription.Key, subscription.ConsumerID}
	return &pb.Result{0}, nil
}

func (s *routeGuideServer) Observe(ctx context.Context, identification *pb.Identification) (*pb.RecordSet, error) {
	redisConn, _ := GetRedisConnection()
	// Get observed keys
	recordSet := pb.RecordSet{}
	peer, _ := peer.FromContext(ctx)

	redisKey := SUBSCRIPTIONS_PREFIX + identification.ConsumerID
	subscribedKeys, cursor, _ := redisConn.SScan(redisKey, 0, "*", 10).Result()
	for ok := true; ok; ok = (cursor != 0) {
		for _, key := range subscribedKeys {
			dequeuedRecords := Dequeue(redisConn, identification.ConsumerID, key, peer.Addr.String())
			recordSet.Records = append(recordSet.Records, dequeuedRecords...)
		}
		subscribedKeys, cursor, _ = redisConn.SScan(redisKey, cursor, "*", 10).Result()
	}
	return &recordSet, nil
}

func Dequeue(redisConn *redis.Client, consumerID string, key string, peerInfo string) []*pb.Record {
	dequeuedRecords := make([]*pb.Record, 0, 50)
	redisKey := MESSAGES_PREFIX + key + "_" + consumerID + "_" + peerInfo
	results, _ := redisConn.LRange(redisKey, 0, 50).Result()
	for _, result := range results {
		dbRecord := new(pb.Message)
		json.Unmarshal([]byte(result), &dbRecord)
		dequeuedRecords = append(dequeuedRecords, &pb.Record{key, dbRecord})
	}
	redisConn.LTrim(redisKey, 50, -1)
	return dequeuedRecords
}

func main() {
	redisConnection = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	defer redisConnection.Close()

	consumers = make(map[string]map[string]bool)

	// Create channels for goroutines
	subscriptionChannel = make(chan Subscription)
	unsubscriptionChannel = make(chan Unsubscription)
	messageToStoreChannel = make(chan IncomingMessage)
	messageToDispatchChannel = make(chan string)
	messageToFetchChannel = make(chan Subscription)
	newMessageChannel = make(chan string)

	// Launch goroutines
	LaunchGoRoutines()

	lis, err := net.Listen("tcp", ":10001")

	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterRouteGuideServer(grpcServer, &routeGuideServer{})
	log.Println("Server listening...")
	grpcServer.Serve(lis)
}
