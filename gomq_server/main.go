package main

import (
	pb "../routeguide"
	"encoding/json"
	"flag"
	"github.com/go-redis/redis"
	"github.com/rs/xid"
	"github.com/tkanos/gonfig"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"log"
	"net"
	"time"
)

type routeGuideServer struct {
	redisConnection *redis.Client
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

type RedisServerConfiguration struct {
	Addr     string
	Password string
	DB       int
}

type ServerConfiguration struct {
	RedisServer   RedisServerConfiguration
	ListeningHost string
}

const CONSUMERS_PREFIX = "CONSUMERS_"
const SUBSCRIPTIONS_PREFIX = "SUBSCRIPTIONS_"
const PEERS_PREFIX = "PEERS_"
const MESSAGES_PREFIX = "MESSAGES_"
const QUEUE_PREFIX = "QUEUE_"
const SUBSCRIBED_KEYS = "SUBSCRIBED_KEYS"

const TTL_KEY = 30 * time.Second

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

func MessageDispatcher(redisConnection *redis.Client, messageToDispatchChannel chan IncomingMessage) {
	for true {
		incomingMessage := <-messageToDispatchChannel

		var subscribers []string
		var cursor uint64 = 0
		for ok := true; ok; {
			subscribers, cursor, _ = redisConnection.SScan(CONSUMERS_PREFIX+incomingMessage.key, cursor, "*", 10).Result()
			for _, subscriber := range subscribers {
				dispatchMessageToPeers(redisConnection, incomingMessage.key, subscriber, string(incomingMessage.payload))
			}
			ok = (cursor != 0)
		}
	}
}

func dispatchMessageToPeers(redisConnection *redis.Client, key string, consumerID string, message string) {
	redisPeersKey := PEERS_PREFIX + consumerID
	var peers []string
	var cursor uint64 = 0
	for ok := true; ok; {
		peers, cursor, _ = redisConnection.SScan(redisPeersKey, cursor, "*", 10).Result()
		for _, peer := range peers {
			redisMessagesKey := MESSAGES_PREFIX + key + "_" + consumerID + "_" + peer
			redisConnection.RPush(redisMessagesKey, message)
		}
		ok = (cursor != 0)
	}
}

func (s *routeGuideServer) Publish(ctx context.Context, record *pb.PublishRecord) (*pb.Result, error) {
	redisKey := QUEUE_PREFIX + record.Key
	db_record := pb.Message{xid.New().String(), time.Now().Format(time.UnixDate), record.Payload}
	marshalled_value, _ := json.Marshal(db_record)

	err := s.redisConnection.RPush(redisKey, string(marshalled_value)).Err()
	result := pb.Result{0}

	if err != nil {
		result.Code = -1
	}

	return &result, err
}

func (s *routeGuideServer) Subscribe(ctx context.Context, subscription *pb.Subscription) (*pb.Result, error) {
	peer, _ := peer.FromContext(ctx)

	// Add the subscription
	redisKey := CONSUMERS_PREFIX + subscription.Key
	redisValue := subscription.ConsumerID
	isMember, _ := s.redisConnection.SIsMember(redisKey, redisValue).Result()
	if !isMember {
		s.redisConnection.SAdd(redisKey, redisValue)
	}

	redisKey = SUBSCRIPTIONS_PREFIX + subscription.ConsumerID
	redisValue = subscription.Key
	isMember, _ = s.redisConnection.SIsMember(redisKey, redisValue).Result()
	if !isMember {
		s.redisConnection.SAdd(redisKey, redisValue)
	}

	// In case where several peers have the same consumer ID,
	// Save the peer
	redisKey = PEERS_PREFIX + subscription.ConsumerID
	redisValue = peer.Addr.String()
	isMember, _ = s.redisConnection.SIsMember(redisKey, redisValue).Result()
	if !isMember {
		s.redisConnection.SAdd(redisKey, redisValue)
	}
	ReferenceKey(s.redisConnection, subscription.Key)

	return &pb.Result{0}, nil
}

func (s *routeGuideServer) Unsubscribe(ctx context.Context, subscription *pb.Subscription) (*pb.Result, error) {
	// Warning: if there are more than one peer for one consumer ID,
	// Then the other peers will be unsubscribed as well.
	redisKey := CONSUMERS_PREFIX + subscription.Key
	redisValue := subscription.ConsumerID
	isMember, _ := s.redisConnection.SIsMember(redisKey, redisValue).Result()
	if isMember {
		s.redisConnection.SRem(redisKey, redisValue)
	}

	redisKey = SUBSCRIPTIONS_PREFIX + subscription.ConsumerID
	redisValue = subscription.Key
	isMember, _ = s.redisConnection.SIsMember(redisKey, redisValue).Result()
	if isMember {
		s.redisConnection.SRem(redisKey, redisValue)
	}

	return &pb.Result{0}, nil
}

func (s *routeGuideServer) Observe(ctx context.Context, identification *pb.Identification) (*pb.RecordSet, error) {
	// Get observed keys
	recordSet := pb.RecordSet{}
	peer, _ := peer.FromContext(ctx)

	redisKey := SUBSCRIPTIONS_PREFIX + identification.ConsumerID
	var subscribedKeys []string
	var cursor uint64 = 0
	for ok := true; ok; {
		subscribedKeys, cursor, _ = s.redisConnection.SScan(redisKey, cursor, "*", 10).Result()
		for _, key := range subscribedKeys {
			dequeuedRecords := Dequeue(s.redisConnection, identification.ConsumerID, key, peer.Addr.String())
			recordSet.Records = append(recordSet.Records, dequeuedRecords...)
		}
		ok = (cursor != 0)
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

func LaunchGoRoutines(redisConnection *redis.Client, messageToDispatch chan IncomingMessage) {
	go MessageDispatcher(redisConnection, messageToDispatch)
	go MessageNotifier(redisConnection, messageToDispatch)
}

func SpawnGoMQServer(redisConnection *redis.Client, config ServerConfiguration) {
	lis, err := net.Listen("tcp", config.ListeningHost)

	if err != nil {
		panic(err)
	}

	grpcServer := grpc.NewServer()

	// Create channels
	incomingMessage := make(chan IncomingMessage)

	// Launch goroutines
	LaunchGoRoutines(redisConnection, incomingMessage)

	svr := routeGuideServer{redisConnection}

	pb.RegisterRouteGuideServer(grpcServer, &svr)
	log.Printf("Server listening on endpoint '%s'...", config.ListeningHost)
	grpcServer.Serve(lis)

}

func BuildDefaultConfig() ServerConfiguration {
	redisServer := RedisServerConfiguration{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	}
	return ServerConfiguration{
		RedisServer:   redisServer,
		ListeningHost: ":10001",
	}
}

func main() {

	var configFile string

	flag.StringVar(&configFile, "config", "", "Path to JSON configuration file.")
	flag.Parse()

	var config ServerConfiguration
	if configFile == "" {
		config = BuildDefaultConfig()
	} else {
		gonfig.GetConf(configFile, &config)
	}

	redisConnection := redis.NewClient(&redis.Options{
		Addr:     config.RedisServer.Addr,
		Password: config.RedisServer.Password,
		DB:       config.RedisServer.DB,
	})
	_, err := redisConnection.Ping().Result()
	if err != nil {
		log.Printf("Could not connect to redis database at %s, DB %d.\n", config.RedisServer.Addr, config.RedisServer.DB)
		panic(err)
	}

	redisConnection.FlushDB()

	defer redisConnection.Close()

	SpawnGoMQServer(redisConnection, config)
}
