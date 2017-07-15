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
	RedisConnection          *redis.Client
	SubscriptionChannel      chan Subscription
	UnsubscriptionChannel    chan Unsubscription
	MessageToStoreChannel    chan IncomingMessage
	MessageToDispatchChannel chan IncomingMessage
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

const CONSUMERS_PREFIX = "CONSUMERS_"
const SUBSCRIPTIONS_PREFIX = "SUBSCRIPTIONS_"
const PEERS_PREFIX = "PEERS_"
const MESSAGES_PREFIX = "MESSAGES_"
const QUEUE_PREFIX = "QUEUE_"
const LASTACTION_PREFIX = "LASTACTION_"
const REFERENCED_KEYS = "REFERENCED_KEYS"

const TTL_KEY = 30 * time.Second

func Subscriptor(redisConnection *redis.Client, subscriptionChannel chan Subscription) {
	for true {
		newSubscription := <-subscriptionChannel

		// Add the subscription
		redisKey := CONSUMERS_PREFIX + newSubscription.Key
		redisValue := newSubscription.ConsumerID
		isMember, _ := redisConnection.SIsMember(redisKey, redisValue).Result()
		if !isMember {
			redisConnection.SAdd(redisKey, redisValue)
		}

		redisKey = SUBSCRIPTIONS_PREFIX + newSubscription.ConsumerID
		redisValue = newSubscription.Key
		isMember, _ = redisConnection.SIsMember(redisKey, redisValue).Result()
		if !isMember {
			redisConnection.SAdd(redisKey, redisValue)
		}

		// In case where several peers have the same consumer ID,
		// Save the peer
		redisKey = PEERS_PREFIX + newSubscription.ConsumerID
		redisValue = newSubscription.PeerInfo
		isMember, _ = redisConnection.SIsMember(redisKey, redisValue).Result()
		if !isMember {
			redisConnection.SAdd(redisKey, redisValue)
		}
		RefreshPeer(redisConnection, newSubscription.ConsumerID, newSubscription.PeerInfo)

	}
}

func RemovePeer(redisConnection *redis.Client, consumerID string, peer string) {
	redisKey := PEERS_PREFIX + consumerID
	isMember, _ := redisConnection.SIsMember(redisKey, peer).Result()
	if isMember {
		redisConnection.SRem(redisKey, peer)
	}
}

func ReferenceKey(redisConnection *redis.Client, key string) {
	isMember, _ := redisConnection.SIsMember(REFERENCED_KEYS, QUEUE_PREFIX+key).Result()
	if !isMember {
		redisConnection.SAdd(REFERENCED_KEYS, QUEUE_PREFIX+key)
	}
}

func RefreshPeer(redisConnection *redis.Client, consumerID string, peerInfo string) {
	redisConnection.Set(LASTACTION_PREFIX+consumerID+"_"+peerInfo, time.Now().String(), 30*time.Second)
}

func MessageNotifier(redisConnection *redis.Client, messageToDispatchChannel chan IncomingMessage) {
	for true {
		referencedKeys, _ := redisConnection.SMembers(REFERENCED_KEYS).Result()
		if len(referencedKeys) == 0 {
			time.Sleep(1 * time.Second)
			continue
		}
		info, err := redisConnection.BLPop(1*time.Second, referencedKeys...).Result()
		if err == nil {
			// info[0] = key
			// info[1] = value
			info[0] = info[0][len("QUEUE_"):]
			messageToDispatchChannel <- IncomingMessage{info[0], []byte(info[1])}
		}
	}
}

func Unsubscriptor(redisConnection *redis.Client, unsubscriptionChannel chan Unsubscription) {
	unsubscriptionQuery := <-unsubscriptionChannel

	// Warning: if there are more than one peer for one consumer ID,
	// Then the other peers will be unsubscribed as well.
	redisKey := CONSUMERS_PREFIX + unsubscriptionQuery.Key
	redisValue := unsubscriptionQuery.ConsumerID
	isMember, _ := redisConnection.SIsMember(redisKey, redisValue).Result()
	if !isMember {
		redisConnection.SRem(redisKey, redisValue)
	}

	redisKey = SUBSCRIPTIONS_PREFIX + unsubscriptionQuery.ConsumerID
	redisValue = unsubscriptionQuery.Key
	isMember, _ = redisConnection.SIsMember(redisKey, redisValue).Result()
	if !isMember {
		redisConnection.SRem(redisKey, redisValue)
	}
}

func MessageReceiver(redisConnection *redis.Client, messageToStoreChannel chan IncomingMessage) {
	for true {
		incomingMessage := <-messageToStoreChannel
		redisKey := QUEUE_PREFIX + incomingMessage.Key
		db_record := pb.Message{xid.New().String(), time.Now().Format(time.UnixDate), incomingMessage.Payload}
		marshalled_value, _ := json.Marshal(db_record)

		redisConnection.RPush(redisKey, string(marshalled_value)).Err()

		redisConnection.Expire(redisKey, 30*time.Second)
		ReferenceKey(redisConnection, incomingMessage.Key)
	}
}

func MessageDispatcher(redisConnection *redis.Client, messageToDispatchChannel chan IncomingMessage) {
	for true {
		incomingMessage := <-messageToDispatchChannel

		// Round robin through different consumers
		subscribers, cursor, _ := redisConnection.SScan(CONSUMERS_PREFIX+incomingMessage.Key, 0, "*", 10).Result()
		if len(subscribers) == 0 {
			// If there is no subscribers yet, Push the message back to the main queue
			redisConnection.LPush(QUEUE_PREFIX+incomingMessage.Key, incomingMessage.Payload)
		} else {
			for ok := true; ok; ok = (cursor != 0) {
				for _, subscriber := range subscribers {
					dispatchMessageToPeers(redisConnection, incomingMessage.Key, subscriber, string(incomingMessage.Payload))
				}
				subscribers, cursor, _ = redisConnection.SScan(CONSUMERS_PREFIX+incomingMessage.Key, cursor, "*", 10).Result()
			}
		}
	}
}

func dispatchMessageToPeers(redisConnection *redis.Client, key string, consumerID string, message string) {
	redisPeersKey := PEERS_PREFIX + consumerID

	// Round robin through different peers with same consumer ID
	peers, cursor, _ := redisConnection.SScan(redisPeersKey, 0, "*", 10).Result()
	for ok := true; ok; ok = (cursor != 0) {
		for _, peer := range peers {
			lastAction, _ := redisConnection.Get(LASTACTION_PREFIX + consumerID + "_" + peer).Result()
			if lastAction != "" {
				redisMessagesKey := MESSAGES_PREFIX + key + "_" + consumerID + "_" + peer
				redisConnection.RPush(redisMessagesKey, message)
				redisConnection.Expire(redisMessagesKey, 30*time.Second)
			} else {
				// Remove inactive peer
				RemovePeer(redisConnection, consumerID, peer)
			}
		}
		peers, cursor, _ = redisConnection.SScan(redisPeersKey, 0, "*", 10).Result()
	}
}

func (s *routeGuideServer) Publish(ctx context.Context, record *pb.PublishRecord) (*pb.Result, error) {
	s.MessageToStoreChannel <- IncomingMessage{record.Key, record.Payload}
	return &pb.Result{0}, nil
}

func (s *routeGuideServer) Subscribe(ctx context.Context, subscription *pb.Subscription) (*pb.Result, error) {
	peer, _ := peer.FromContext(ctx)
	s.SubscriptionChannel <- Subscription{subscription.Key, subscription.ConsumerID, peer.Addr.String()}
	return &pb.Result{0}, nil
}

func (s *routeGuideServer) Unsubscribe(ctx context.Context, subscription *pb.Subscription) (*pb.Result, error) {
	s.UnsubscriptionChannel <- Unsubscription{subscription.Key, subscription.ConsumerID}
	return &pb.Result{0}, nil
}

func (s *routeGuideServer) Observe(ctx context.Context, identification *pb.Identification) (*pb.RecordSet, error) {
	// Get observed keys
	recordSet := pb.RecordSet{}
	peer, _ := peer.FromContext(ctx)

	RefreshPeer(s.RedisConnection, identification.ConsumerID, peer.Addr.String())

	redisKey := SUBSCRIPTIONS_PREFIX + identification.ConsumerID
	subscribedKeys, cursor, _ := s.RedisConnection.SScan(redisKey, 0, "*", 10).Result()
	for ok := true; ok; ok = (cursor != 0) {
		for _, key := range subscribedKeys {
			dequeuedRecords := Dequeue(s.RedisConnection, identification.ConsumerID, key, peer.Addr.String())
			recordSet.Records = append(recordSet.Records, dequeuedRecords...)
		}
		subscribedKeys, cursor, _ = s.RedisConnection.SScan(redisKey, cursor, "*", 10).Result()
	}
	return &recordSet, nil
}

func Dequeue(redisConnection *redis.Client, consumerID string, key string, peerInfo string) []*pb.Record {
	dequeuedRecords := make([]*pb.Record, 0, 50)
	redisKey := MESSAGES_PREFIX + key + "_" + consumerID + "_" + peerInfo
	results, _ := redisConnection.LRange(redisKey, 0, 50).Result()
	for _, result := range results {
		dbRecord := new(pb.Message)
		json.Unmarshal([]byte(result), &dbRecord)
		dequeuedRecords = append(dequeuedRecords, &pb.Record{key, dbRecord})
	}
	redisConnection.LTrim(redisKey, 50, -1)
	return dequeuedRecords
}

func LaunchGoRoutines(redisConnection *redis.Client, subscriptionChannel chan Subscription, unsubscriptionChannel chan Unsubscription, messageToStoreChannel chan IncomingMessage, messageToDispatchChannel chan IncomingMessage) {
	go Subscriptor(redisConnection, subscriptionChannel)
	go Unsubscriptor(redisConnection, unsubscriptionChannel)
	go MessageReceiver(redisConnection, messageToStoreChannel)
	go MessageDispatcher(redisConnection, messageToDispatchChannel)
	go MessageNotifier(redisConnection, messageToDispatchChannel)
}

func main() {
	redisConnection := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	redisConnection.FlushDB()

	defer redisConnection.Close()

	lis, err := net.Listen("tcp", ":10001")

	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	// Create channels
	subscriptionChannel := make(chan Subscription)
	unsubscriptionChannel := make(chan Unsubscription)
	messageToStoreChannel := make(chan IncomingMessage)
	messageToDispatchChannel := make(chan IncomingMessage)

	// Launch goroutines
	LaunchGoRoutines(redisConnection, subscriptionChannel, unsubscriptionChannel, messageToStoreChannel, messageToDispatchChannel)

	svr := routeGuideServer{
		redisConnection,
		subscriptionChannel,
		unsubscriptionChannel,
		messageToStoreChannel,
		messageToDispatchChannel,
	}
	pb.RegisterRouteGuideServer(grpcServer, &svr)
	log.Println("Server listening...")
	grpcServer.Serve(lis)
}
