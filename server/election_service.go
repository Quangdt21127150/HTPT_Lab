package main

import (
	"context"
	"log"
	"sync"
	"time"

	pb "github.com/marcelloh/fastdb/user"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func (s *UserServer) replicate(req any, operation string) error {
	var wg sync.WaitGroup
	errChan := make(chan error, len(s.config.peers)-1)

	for _, pid := range s.config.peers {
		if pid == s.config.myID {
			continue
		}
		wg.Add(1)
		go func(peerID int) {
			defer wg.Done()
			addr := s.config.addressMap[peerID]
			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("%s [Server %d] [Leader] Replication %s to server %d failed: connect error %v", time.Now().Format("2006-01-02 15:04:05"), s.config.myID, operation, peerID, err)
				errChan <- err
				return
			}
			client := pb.NewUserServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			var rpcErr error
			switch operation {
			case "Insert":
				_, rpcErr = client.ReplicateInsert(ctx, req.(*pb.SetRequest))
			case "Set":
				_, rpcErr = client.ReplicateSet(ctx, req.(*pb.SetRequest))
			case "Delete":
				_, rpcErr = client.ReplicateDelete(ctx, req.(*pb.IDRequest))
			}

			conn.Close()
			if rpcErr != nil {
				log.Printf("%s [Server %d] [Leader] Received response from server %d for %s: FAILED (%v)", time.Now().Format("2006-01-02 15:04:05"), s.config.myID, peerID, operation, rpcErr)
				errChan <- rpcErr
			} else {
				log.Printf("%s [Server %d] [Leader] Received response from server %d for %s: SUCCESS", time.Now().Format("2006-01-02 15:04:05"), s.config.myID, peerID, operation)
			}
		}(pid)
	}

	wg.Wait()
	close(errChan)
	for err := range errChan {
		if err != nil {
			return err
		}
	}
	log.Printf("%s [Server %d] [Leader] All backups responded for %s", time.Now().Format("2006-01-02 15:04:05"), s.config.myID, operation)
	return nil
}

func (s *UserServer) GetLeader(ctx context.Context, req *pb.EmptyRequest) (*pb.ServerID, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return &pb.ServerID{ID: int32(s.currentLeader)}, nil
}

func (s *UserServer) initiateElection() {
	s.mu.Lock()
	currentLeader := s.currentLeader
	if currentLeader == s.config.myID {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	log.Printf("%s [Server %d] [Backup] Leader %d suspected failed, starting Ring election after delay", time.Now().Format("2006-01-02 15:04:05"), s.config.myID, currentLeader)

	nextAddr := s.config.addressMap[s.config.peers[s.config.nextPeerIndex]]
	conn, err := grpc.NewClient(nextAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("%s [Server %d] [Backup] Cannot reach next peer %d for election: %v", time.Now().Format("2006-01-02 15:04:05"), s.config.myID, s.config.peers[s.config.nextPeerIndex], err)
		return
	}
	client := pb.NewElectionServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.SendElection(ctx, &pb.ServerID{ID: int32(s.config.myID)})
	conn.Close()
	if err != nil {
		log.Printf("%s [Server %d] [Backup] Election message failed to %d: %v", time.Now().Format("2006-01-02 15:04:05"), s.config.myID, s.config.peers[s.config.nextPeerIndex], err)
	}
}

func (s *UserServer) SendElection(ctx context.Context, req *pb.ServerID) (*pb.EmptyRequest, error) {
	candidateID := int(req.ID)

	s.mu.Lock()
	myID := s.config.myID
	nextIndex := s.config.nextPeerIndex
	nextAddr := s.config.addressMap[s.config.peers[nextIndex]]
	s.mu.Unlock()

	if candidateID == myID {
		log.Printf("%s [Server %d] Ring election completed, I am the new Leader (ID=%d)", time.Now().Format("2006-01-02 15:04:05"), myID, myID)
		s.setLeader(myID)
		s.broadcastCoordinator()
		return &pb.EmptyRequest{}, nil
	}

	sendID := max(candidateID, myID)

	conn, err := grpc.NewClient(nextAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("%s [Server %d] Cannot forward election to next %d: %v", time.Now().Format("2006-01-02 15:04:05"), myID, s.config.peers[nextIndex], err)
		return &pb.EmptyRequest{}, err
	}
	client := pb.NewElectionServiceClient(conn)
	ctx2, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.SendElection(ctx2, &pb.ServerID{ID: int32(sendID)})
	conn.Close()
	return &pb.EmptyRequest{}, err
}

func (s *UserServer) broadcastCoordinator() {
	for _, pid := range s.config.peers {
		if pid == s.config.myID {
			continue
		}
		addr := s.config.addressMap[pid]
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("%s [Server %d] [Leader] Cannot broadcast Coordinator to %d: %v", time.Now().Format("2006-01-02 15:04:05"), s.config.myID, pid, err)
			continue
		}
		client := pb.NewElectionServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, _ = client.SendCoordinator(ctx, &pb.ServerID{ID: int32(s.config.myID)})
		cancel()
		conn.Close()
	}
}

func (s *UserServer) SendCoordinator(ctx context.Context, req *pb.ServerID) (*pb.EmptyRequest, error) {
	newLeader := int(req.ID)
	s.setLeader(newLeader)
	return &pb.EmptyRequest{}, nil
}

func (s *UserServer) Ping(ctx context.Context, req *pb.EmptyRequest) (*pb.EmptyRequest, error) {
	return &pb.EmptyRequest{}, nil
}
