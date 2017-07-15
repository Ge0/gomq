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
	//"sync"
	"net"
	"time"
)

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
var messageToReceivedChannel chan IncomingMessage
var messageToDispatchChannel chan IncomingMessage
var messageToFetchChannel chan Subscription
var newMessageChannel chan string

var redisConnection *redis.Client

const CONSUMERS_PREFIX = "CONSUMERS_"
const SUBSCRIPTIONS_PREFIX = "SUBSCRIPTIONS_"
const PEERS_PREFIX = "PEERS_"
const MESSAGES_PREFIX = "MESSAGES_"
const QUEUE_PREFIX = "QUEUE_"
const LASTACTION_PREFIX = "LASTACTION_"
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
		RefreshPeer(newSubscription.ConsumerID, newSubscription.PeerInfo)

	}
}

func RemovePeer(redisConn *redis.Client, consumerID string, peer string) {
	redisKey := PEERS_PREFIX + consumerID
	isMember, _ := redisConn.SIsMember(redisKey, peer).Result()
	if isMember {
		redisConn.SRem(redisKey, peer)
	}
}

func ReferenceKey(key string) {
	redisConn, _ := GetRedisConnection()
	isMember, _ := redisConn.SIsMember(REFERENCED_KEYS, QUEUE_PREFIX+key).Result()
	if !isMember {
		redisConn.SAdd(REFERENCED_KEYS, QUEUE_PREFIX+key)
	}
}

func RefreshPeer(consumerID string, peerInfo string) {
	redisConn, _ := GetRedisConnection()
	redisConn.Set(LASTACTION_PREFIX+consumerID+"_"+peerInfo, time.Now().String(), 30*time.Second)
}

func MessageNotifier() {
	redisConn, _ := GetRedisConnection()
	for true {
		referencedKeys, _ := redisConn.SMembers(REFERENCED_KEYS).Result()
		if len(referencedKeys) == 0 {
			time.Sleep(1 * time.Second)
			continue
		}
		info, err := redisConn.BLPop(1*time.Second, referencedKeys...).Result()
		if err == nil {
			// info[0] = key
			// info[1] = value
			info[0] = info[0][len("QUEUE_"):]
			messageToDispatchChannel <- IncomingMessage{info[0], []byte(info[1])}
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
		ReferenceKey(incomingMessage.Key)
	}
}

func MessageDispatcher() {
	for true {
		incomingMessage := <-messageToDispatchChannel
		redisConn, _ := GetRedisConnection()

		// Round robin through different consumers
		subscribers, cursor, _ := redisConn.SScan(CONSUMERS_PREFIX+incomingMessage.Key, 0, "*", 10).Result()
		if len(subscribers) == 0 {
			// If there is no subscribers yet, Push the message back to the main queue
			redisConn.LPush(QUEUE_PREFIX+incomingMessage.Key, incomingMessage.Payload)
		} else {
			for ok := true; ok; ok = (cursor != 0) {
				for _, subscriber := range subscribers {
					dispatchMessageToPeers(redisConn, incomingMessage.Key, subscriber, string(incomingMessage.Payload))
				}
				subscribers, cursor, _ = redisConn.SScan(CONSUMERS_PREFIX+incomingMessage.Key, cursor, "*", 10).Result()
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
			lastAction, _ := redisConn.Get(LASTACTION_PREFIX + consumerID + "_" + peer).Result()
			if lastAction != "" {
				redisMessagesKey := MESSAGES_PREFIX + key + "_" + consumerID + "_" + peer
				redisConn.RPush(redisMessagesKey, message)
				redisConn.Expire(redisMessagesKey, 30*time.Second)
			} else {
				// Remove inactive peer
				RemovePeer(redisConn, consumerID, peer)
			}
		}
		peers, cursor, _ = redisConn.SScan(redisPeersKey, 0, "*", 10).Result()
	}
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

	RefreshPeer(identification.ConsumerID, peer.Addr.String())

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

func LaunchGoRoutines() {
	// Create channels for goroutines
	subscriptionChannel = make(chan Subscription)
	unsubscriptionChannel = make(chan Unsubscription)
	messageToStoreChannel = make(chan IncomingMessage)
	messageToDispatchChannel = make(chan IncomingMessage)
	messageToFetchChannel = make(chan Subscription)
	newMessageChannel = make(chan string)

	// Launch them
	go Subscriptor()
	go Unsubscriptor()
	go MessageReceiver()
	go MessageDispatcher()
	go MessageNotifier()
}

func main() {
	redisConnection = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	redisConnection.FlushDB()

	defer redisConnection.Close()

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
