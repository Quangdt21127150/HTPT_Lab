package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/marcelloh/fastdb"
	pb "github.com/marcelloh/fastdb/user"
	"google.golang.org/grpc"
)

const (
	syncTime = 100
	dataDir  = "data"
)

var (
	myIDFlag  = flag.Int("id", 1, "Server ID")
	peersFlag = flag.String("peers", "1, 2, 3", "Comma-separated list of all peer IDs including self")
)

type ServerConfig struct {
	myID          int
	peers         []int
	addressMap    map[int]string
	nextPeerIndex int
	db            *fastdb.DB
}

func loadConfig() ServerConfig {
	flag.Parse()

	myID := *myIDFlag
	peerStrs := strings.Split(*peersFlag, ",")
	peers := make([]int, len(peerStrs))
	addressMap := make(map[int]string)

	for i, p := range peerStrs {
		id, _ := strconv.Atoi(strings.TrimSpace(p))
		peers[i] = id
		port := 3000 + id
		addressMap[id] = fmt.Sprintf("localhost:%d", port)
	}

	sort.Ints(peers)
	nextIndex := 0
	for i, pid := range peers {
		if pid == myID {
			nextIndex = (i + 1) % len(peers)
			break
		}
	}

	dbPath := fmt.Sprintf("%s/users%d.db", dataDir, myID)
	_ = os.MkdirAll(dataDir, 0755)

	db, err := fastdb.Open(dbPath, syncTime)
	if err != nil {
		log.Fatalf("Failed to open db: %v", err)
	}

	return ServerConfig{
		myID:          myID,
		peers:         peers,
		addressMap:    addressMap,
		nextPeerIndex: nextIndex,
		db:            db,
	}
}

func main() {
	config := loadConfig()

	lisPeer, err := net.Listen("tcp", fmt.Sprintf(":%d", 3000+config.myID))
	if err != nil {
		log.Fatalf("[Server %d] Failed to listen on peer port %d: %v", config.myID, 3000+config.myID, err)
	}

	srv := NewUserServer(&config)

	role := "Backup"
	if srv.isLeader {
		role = "Leader"
	}

	grpcServer := grpc.NewServer()
	pb.RegisterUserServiceServer(grpcServer, srv)
	pb.RegisterElectionServiceServer(grpcServer, srv)

	go func() {
		log.Printf("%s [Server %d] [%s] Running on localhost:%d", time.Now().Format("2006-01-02 15:04:05"), config.myID, role, 3000+config.myID)
		if err := grpcServer.Serve(lisPeer); err != nil {
			log.Printf("%s [Server %d] [%s] Peer listener stopped: %v", time.Now().Format("2006-01-02 15:04:05"), config.myID, role, err)
		}
	}()

	if srv.isLeader {
		clientLis, err := net.Listen("tcp", ":3000")
		if err != nil {
			log.Printf("%s [Server %d] [Leader] WARNING: Cannot bind client port 3000: %v", time.Now().Format("2006-01-02 15:04:05"), config.myID, err)
		} else {
			log.Printf("%s [Server %d] [Leader] Listening for Client on port 3000", time.Now().Format("2006-01-02 15:04:05"), config.myID)
			go func() {
				if err := grpcServer.Serve(clientLis); err != nil && err != grpc.ErrServerStopped {
					log.Printf("[Server %d] Client listener stopped: %v", config.myID, err)
				}
			}()
		}
	}

	select {}
}
