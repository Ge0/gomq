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
	redisConnection *redis.Client
	channels        GoMQChannels
}

type GoMQChannels struct {
	subscription      chan Subscription
	unsubscription    chan Unsubscription
	messageToStore    chan IncomingMessage
	messageToDispatch chan IncomingMessage
}

type Subscription struct {
	key        string
	consumerID string
	peerInfo   string
}

type Unsubscription struct {
	key        string
	consumerID string
}

type IncomingMessage struct {
	key     string
	payload []byte
}

const CONSUMERS_PREFIX = "CONSUMERS_"
const SUBSCRIPTIONS_PREFIX = "SUBSCRIPTIONS_"
const PEERS_PREFIX = "PEERS_"
const MESSAGES_PREFIX = "MESSAGES_"
const QUEUE_PREFIX = "QUEUE_"
const SUBSCRIBED_KEYS = "SUBSCRIBED_KEYS"

const TTL_KEY = 30 * time.Second

func Subscriptor(redisConnection *redis.Client, subscriptionChannel chan Subscription) {
	for true {
		newSubscription := <-subscriptionChannel

		// Add the subscription
		redisKey := CONSUMERS_PREFIX + newSubscription.key
		redisValue := newSubscription.consumerID
		isMember, _ := redisConnection.SIsMember(redisKey, redisValue).Result()
		if !isMember {
			redisConnection.SAdd(redisKey, redisValue)
		}

		redisKey = SUBSCRIPTIONS_PREFIX + newSubscription.consumerID
		redisValue = newSubscription.key
		isMember, _ = redisConnection.SIsMember(redisKey, redisValue).Result()
		if !isMember {
			redisConnection.SAdd(redisKey, redisValue)
		}

		// In case where several peers have the same consumer ID,
		// Save the peer
		redisKey = PEERS_PREFIX + newSubscription.consumerID
		redisValue = newSubscription.peerInfo
		isMember, _ = redisConnection.SIsMember(redisKey, redisValue).Result()
		if !isMember {
			redisConnection.SAdd(redisKey, redisValue)
		}
		ReferenceKey(redisConnection, newSubscription.key)
	}
}

func ReferenceKey(redisConnection *redis.Client, key string) {
	isMember, _ := redisConnection.SIsMember(SUBSCRIBED_KEYS, QUEUE_PREFIX+key).Result()
	if !isMember {
		redisConnection.SAdd(SUBSCRIBED_KEYS, QUEUE_PREFIX+key)
	}
}

func MessageNotifier(redisConnection *redis.Client, messageToDispatchChannel chan IncomingMessage) {
	for true {
		var info []string
		var err error
		referencedKeys, _ := redisConnection.SMembers(SUBSCRIBED_KEYS).Result()
		if len(referencedKeys) == 0 {
			info, err = redisConnection.BLPop(1*time.Second, SUBSCRIBED_KEYS).Result()
			if err == nil {
				redisConnection.LPush(info[0], info[1])
			}
			continue
		} else {
			info, err = redisConnection.BLPop(1*time.Second, referencedKeys...).Result()
		}

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
	redisKey := CONSUMERS_PREFIX + unsubscriptionQuery.key
	redisValue := unsubscriptionQuery.consumerID
	isMember, _ := redisConnection.SIsMember(redisKey, redisValue).Result()
	if isMember {
		redisConnection.SRem(redisKey, redisValue)
	}

	redisKey = SUBSCRIPTIONS_PREFIX + unsubscriptionQuery.consumerID
	redisValue = unsubscriptionQuery.key
	isMember, _ = redisConnection.SIsMember(redisKey, redisValue).Result()
	if isMember {
		redisConnection.SRem(redisKey, redisValue)
	}
}

func MessageReceiver(redisConnection *redis.Client, messageToStoreChannel chan IncomingMessage) {
	for true {
		incomingMessage := <-messageToStoreChannel
		redisKey := QUEUE_PREFIX + incomingMessage.key
		db_record := pb.Message{xid.New().String(), time.Now().Format(time.UnixDate), incomingMessage.payload}
		marshalled_value, _ := json.Marshal(db_record)

		redisConnection.RPush(redisKey, string(marshalled_value)).Err()

		redisConnection.Expire(redisKey, 30*time.Second)
	}
}

func MessageDispatcher(redisConnection *redis.Client, messageToDispatchChannel chan IncomingMessage) {
	for true {
		incomingMessage := <-messageToDispatchChannel

		// Round robin through different consumers
		subscribers, cursor, _ := redisConnection.SScan(CONSUMERS_PREFIX+incomingMessage.key, 0, "*", 10).Result()
		for ok := true; ok; ok = (cursor != 0) {
			for _, subscriber := range subscribers {
				dispatchMessageToPeers(redisConnection, incomingMessage.key, subscriber, string(incomingMessage.payload))
			}
			subscribers, cursor, _ = redisConnection.SScan(CONSUMERS_PREFIX+incomingMessage.key, cursor, "*", 10).Result()
		}
	}
}

func dispatchMessageToPeers(redisConnection *redis.Client, key string, consumerID string, message string) {
	redisPeersKey := PEERS_PREFIX + consumerID

	// Round robin through different peers with same consumer ID
	peers, cursor, _ := redisConnection.SScan(redisPeersKey, 0, "*", 10).Result()
	for ok := true; ok; ok = (cursor != 0) {
		for _, peer := range peers {
			redisMessagesKey := MESSAGES_PREFIX + key + "_" + consumerID + "_" + peer
			redisConnection.RPush(redisMessagesKey, message)
			redisConnection.Expire(redisMessagesKey, 30*time.Second)
		}
		peers, cursor, _ = redisConnection.SScan(redisPeersKey, cursor, "*", 10).Result()
	}
}

func (s *routeGuideServer) Publish(ctx context.Context, record *pb.PublishRecord) (*pb.Result, error) {
	s.channels.messageToStore <- IncomingMessage{record.Key, record.Payload}
	return &pb.Result{0}, nil
}

func (s *routeGuideServer) Subscribe(ctx context.Context, subscription *pb.Subscription) (*pb.Result, error) {
	peer, _ := peer.FromContext(ctx)
	s.channels.subscription <- Subscription{subscription.Key, subscription.ConsumerID, peer.Addr.String()}
	return &pb.Result{0}, nil
}

func (s *routeGuideServer) Unsubscribe(ctx context.Context, subscription *pb.Subscription) (*pb.Result, error) {
	s.channels.unsubscription <- Unsubscription{subscription.Key, subscription.ConsumerID}
	return &pb.Result{0}, nil
}

func (s *routeGuideServer) Observe(ctx context.Context, identification *pb.Identification) (*pb.RecordSet, error) {
	// Get observed keys
	recordSet := pb.RecordSet{}
	peer, _ := peer.FromContext(ctx)

	redisKey := SUBSCRIPTIONS_PREFIX + identification.ConsumerID
	subscribedKeys, cursor, _ := s.redisConnection.SScan(redisKey, 0, "*", 10).Result()
	for ok := true; ok; ok = (cursor != 0) {
		for _, key := range subscribedKeys {
			dequeuedRecords := Dequeue(s.redisConnection, identification.ConsumerID, key, peer.Addr.String())
			recordSet.Records = append(recordSet.Records, dequeuedRecords...)
		}
		subscribedKeys, cursor, _ = s.redisConnection.SScan(redisKey, cursor, "*", 10).Result()
	}
	return &recordSet, nil
}

func Dequeue(redisConnection *redis.Client, consumerID string, key string, peerInfo string) []*pb.Record {
	dequeuedRecords := make([]*pb.Record, 0, 50)
	redisKey := MESSAGES_PREFIX + key + "_" + consumerID + "_" + peerInfo
	results, _ := redisConnection.LRange(redisKey, 0, 49).Result()
	for _, result := range results {
		dbRecord := new(pb.Message)
		json.Unmarshal([]byte(result), &dbRecord)
		dequeuedRecords = append(dequeuedRecords, &pb.Record{key, dbRecord})
	}
	redisConnection.LTrim(redisKey, 50, -1)
	return dequeuedRecords
}

func LaunchGoRoutines(redisConnection *redis.Client, channels GoMQChannels) {
	//newSubscribedKeyChannel := make(chan string)

	go Subscriptor(redisConnection, channels.subscription)
	go Unsubscriptor(redisConnection, channels.unsubscription)
	go MessageReceiver(redisConnection, channels.messageToStore)
	go MessageDispatcher(redisConnection, channels.messageToDispatch)
	go MessageNotifier(redisConnection, channels.messageToDispatch)
}

func SpawnGoMQServer(redisConnection *redis.Client) {
	lis, err := net.Listen("tcp", ":10001")

	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	// Create channels
	gomqChannels := GoMQChannels{
		make(chan Subscription),
		make(chan Unsubscription),
		make(chan IncomingMessage),
		make(chan IncomingMessage),
	}

	// Launch goroutines
	LaunchGoRoutines(redisConnection, gomqChannels)

	svr := routeGuideServer{
		redisConnection,
		gomqChannels,
	}
	pb.RegisterRouteGuideServer(grpcServer, &svr)
	log.Println("Server listening...")
	grpcServer.Serve(lis)

}

func main() {
	redisConnection := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	redisConnection.FlushDB()

	defer redisConnection.Close()

	SpawnGoMQServer(redisConnection)
}
