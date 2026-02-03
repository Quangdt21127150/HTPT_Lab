package main

import (
	"context"
	"log"
	"net"
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

func (s *UserServer) openPortForClient() {
	grpcServer := grpc.NewServer()
	pb.RegisterUserServiceServer(grpcServer, s)
	pb.RegisterElectionServiceServer(grpcServer, s)

	clientLis, err := net.Listen("tcp", ":3000")
	if err != nil {
		log.Printf("%s [Server %d] [Leader] WARNING: Cannot bind client port 3000: %v", time.Now().Format("2006-01-02 15:04:05"), s.config.myID, err)
	} else {
		log.Printf("%s [Server %d] [Leader] Listening for Client on port 3000", time.Now().Format("2006-01-02 15:04:05"), s.config.myID)
		go func() {
			if err := grpcServer.Serve(clientLis); err != nil && err != grpc.ErrServerStopped {
				log.Printf("[Server %d] Client listener stopped: %v", s.config.myID, err)
			}
		}()
	}
}

func (s *UserServer) GetLeader(ctx context.Context, req *pb.EmptyRequest) (*pb.ServerID, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return &pb.ServerID{ID: int32(s.currentLeader)}, nil
}

func (s *UserServer) getNextAlivePeer(startIndex int) (int, string, bool) {
	peers := s.config.peers
	n := len(peers)
	if n <= 1 {
		return -1, "", false
	}

	for i := range n {
		idx := (startIndex + i) % n
		peerID := peers[idx]
		if peerID == s.config.myID {
			continue
		}

		addr := s.config.addressMap[peerID]
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}

		client := pb.NewElectionServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err = client.Ping(ctx, &pb.EmptyRequest{})
		cancel()
		conn.Close()

		if err == nil {
			return peerID, addr, true
		}
	}
	return -1, "", false
}

func (s *UserServer) initiateElection() {
	s.mu.Lock()
	currentLeader := s.currentLeader
	if currentLeader == s.config.myID {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	nextPeerID, nextAddr, found := s.getNextAlivePeer(s.config.nextPeerIndex)
	if !found {
		log.Printf("%s [Server %d] [Leader] No alive peer, self-elect as leader", time.Now().Format("2006-01-02 15:04:05"), s.config.myID)
		s.setLeader(s.config.myID)
		s.broadcastCoordinator()
		s.openPortForClient()
		return
	}

	conn, err := grpc.NewClient(nextAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("%s [Server %d] [Backup] Cannot reach next alive peer %d (%s): %v", time.Now().Format("2006-01-02 15:04:05"), s.config.myID, nextPeerID, nextAddr, err)
		return
	}
	client := pb.NewElectionServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.SendElection(ctx, &pb.ServerID{ID: int32(s.config.myID)})
	conn.Close()
	if err != nil {
		log.Printf("%s [Server %d] [Backup] Election message failed to %d: %v", time.Now().Format("2006-01-02 15:04:05"), s.config.myID, nextPeerID, err)
	}
}

func (s *UserServer) forwardElection(sendID int, currentIndex int) error {
	peers := s.config.peers
	n := len(peers)

	for attempt := range n {
		idx := (currentIndex + attempt) % n
		peerID := peers[idx]
		if peerID == s.config.myID {
			continue
		}

		addr := s.config.addressMap[peerID]
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("%s [Server %d] Skip dead peer %d (%s) during forward", time.Now().Format("2006-01-02 15:04:05"), s.config.myID, peerID, addr)
			continue
		}

		client := pb.NewElectionServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err = client.SendElection(ctx, &pb.ServerID{ID: int32(sendID)})
		cancel()
		conn.Close()

		if err == nil {
			return nil
		}
	}

	return nil
}

func (s *UserServer) SendElection(ctx context.Context, req *pb.ServerID) (*pb.SuccessResponse, error) {
	candidateID := int(req.ID)

	s.mu.Lock()
	myID := s.config.myID
	nextIndex := s.config.nextPeerIndex
	s.mu.Unlock()

	if candidateID == myID {
		s.setLeader(myID)
		s.broadcastCoordinator()
		log.Printf("%s [Server %d] [Leader] Ring election completed, became Leader", time.Now().Format("2006-01-02 15:04:05"), s.config.myID)
		s.openPortForClient()
		return &pb.SuccessResponse{Success: true}, nil
	}

	sendID := max(candidateID, myID)

	err := s.forwardElection(sendID, nextIndex)
	if err != nil {
		return nil, err
	}

	return &pb.SuccessResponse{Success: true}, nil
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

func (s *UserServer) SendCoordinator(ctx context.Context, req *pb.ServerID) (*pb.SuccessResponse, error) {
	newLeader := int(req.ID)
	s.setLeader(newLeader)
	log.Printf("%s [Server %d] [Backup] Ring election completed, follows the Leader %d", time.Now().Format("2006-01-02 15:04:05"), s.config.myID, newLeader)
	return &pb.SuccessResponse{Success: true}, nil
}

func (s *UserServer) Ping(ctx context.Context, req *pb.EmptyRequest) (*pb.SuccessResponse, error) {
	return &pb.SuccessResponse{Success: true}, nil
}
