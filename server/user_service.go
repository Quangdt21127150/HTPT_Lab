package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	pb "github.com/marcelloh/fastdb/user"

	"github.com/marcelloh/fastdb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	bucket   = "user"
	buntPath = "buntData.db"
)

type User struct {
	CreatedAt string `json:"CreatedAt"`
	UUID      string `json:"UUID"`
	Email     string `json:"Email"`
	Password  string `json:"Password"`
	Image     string `json:"Image"`
	ID        int    `json:"ID"`
	IsAdmin   bool   `json:"IsAdmin"`
}

type UserServer struct {
	pb.UnimplementedUserServiceServer
	pb.UnimplementedElectionServiceServer

	config              *ServerConfig
	processedRequests   map[string]bool
	reqMu               sync.RWMutex
	mu                  sync.RWMutex
	isLeader            bool
	currentLeader       int
	electionTriggered   bool
	electionTriggeredMu sync.Mutex
	leaderFailed        bool
	leaderFailTimer     *time.Timer
	healthCheckStop     chan bool
}

func loadLastLeader() int {
	path := filepath.Join("data", "leader.marker")
	data, err := os.ReadFile(path)
	if err != nil {
		log.Printf("Failed to find last leader: %v", err)
		return 0
	}
	idStr := strings.TrimSpace(string(data))
	if idStr == "" {
		log.Printf("Failed to find last leader: %v", err)
		return 0
	}
	id, err := strconv.Atoi(idStr)
	if err != nil || id <= 0 {
		log.Printf("Failed to find last leader: %v", err)
		return 0
	}
	return id
}

func (s *UserServer) saveLeaderToMarker(leaderID int) error {
	path := filepath.Join("data", "leader.marker")
	content := fmt.Sprintf("%d", leaderID)
	return os.WriteFile(path, []byte(content), 0644)
}

func NewUserServer(config *ServerConfig) *UserServer {
	srv := &UserServer{
		config:            config,
		processedRequests: make(map[string]bool),
		currentLeader:     -1,
		isLeader:          false,
		electionTriggered: false,
		healthCheckStop:   make(chan bool, 1),
	}

	all, _ := config.db.GetAll(bucket)
	dbIsEmpty := len(all) == 0

	path := filepath.Join("data", "leader.marker")
	_, err := os.ReadFile(path)

	if err != nil {
		srv.saveLeaderToMarker(config.myID)
	}

	lastLeader := loadLastLeader()

	srv.mu.Lock()
	srv.currentLeader = lastLeader
	srv.isLeader = (lastLeader == config.myID)
	srv.mu.Unlock()

	if srv.isLeader && dbIsEmpty {
		if err := loadFromBunt(config.db, buntPath); err == nil {
			log.Printf("%s [Server %d] [Leader] Initialized database", time.Now().Format("2006-01-02 15:04:05"), config.myID)
		} else {
			log.Printf("%s [Server %d] [Leader] Initialize database failed: %v", time.Now().Format("2006-01-02 15:04:05"), config.myID, err)
		}
	} else if !srv.isLeader {
		if err := srv.copyDataFromLeader(); err == nil {
			log.Printf("%s [Server %d] [Backup] Successfully copied data from Leader %d", time.Now().Format("2006-01-02 15:04:05"), config.myID, lastLeader)
		} else {
			log.Printf("%s [Server %d] [Backup] Failed to copy from leader, starting empty: %v", time.Now().Format("2006-01-02 15:04:05"), config.myID, err)
		}
	}

	// Start health check monitoring for backup servers
	if !srv.isLeader {
		go srv.monitorLeaderHealth()
	}

	return srv
}

func loadFromBunt(db *fastdb.DB, path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		return err
	}

	lines := strings.Split(string(content), "\n")

	for i := 4; i < len(lines); i += 4 {
		key := strings.TrimSpace(lines[i])
		i += 2
		value := strings.TrimSpace(lines[i])
		i++

		if !strings.HasPrefix(key, "user_") {
			continue
		}

		idStr := strings.TrimPrefix(key, "user_")
		id, err := strconv.Atoi(idStr)
		if err != nil {
			continue
		}

		_, ok := db.Get(bucket, id)
		if ok {
			continue
		}

		var u User
		if err := json.Unmarshal([]byte(value), &u); err != nil {
			continue
		}
		if u.ID != id {
			continue
		}

		db.Set(bucket, id, []byte(value))
	}
	return nil
}

func (s *UserServer) copyDataFromLeader() error {
	if s.currentLeader <= 0 {
		return nil
	}

	myDBPath := fmt.Sprintf("data/users%d.db", s.config.myID)

	// Close DB of backup
	if err := s.config.db.Close(); err != nil {
		log.Printf("%s [Server %d] [Backup] Failed to close db before delete: %v", time.Now().Format("2006-01-02 15:04:05"), s.config.myID, err)
		return err
	}

	// Remove DB of backup
	if err := os.Remove(myDBPath); err != nil && !os.IsNotExist(err) {
		log.Printf("%s [Server %d] [Backup] Failed to remove old db file: %v", time.Now().Format("2006-01-02 15:04:05"), s.config.myID, err)
		return err
	}

	// Reopen DB for backup
	newDB, err := fastdb.Open(myDBPath, syncTime)
	if err != nil {
		log.Printf("%s [Server %d] [Backup] Failed to reopen db after delete: %v", time.Now().Format("2006-01-02 15:04:05"), s.config.myID, err)
		return err
	}
	s.config.db = newDB

	// Copy DB from leader
	leaderDBPath := fmt.Sprintf("data/users%d.db", s.currentLeader)
	srcDB, err := fastdb.Open(leaderDBPath, syncTime)
	if err != nil {
		return err
	}
	defer srcDB.Close()

	all, err := srcDB.GetAll(bucket)
	if err != nil {
		return err
	}

	for id, data := range all {
		s.config.db.Set(bucket, id, data)
	}

	return nil
}

func validateUser(u *pb.UserDTO) error {
	if u == nil {
		return status.Error(codes.InvalidArgument, "User data required")
	}
	if u.Email == "" {
		return status.Error(codes.InvalidArgument, "Email required")
	}

	var emailRegex = regexp.MustCompile(`^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`)
	if !emailRegex.MatchString(u.Email) {
		return status.Error(codes.InvalidArgument, "Invalid Email format")
	}

	if u.Password == "" {
		return status.Error(codes.InvalidArgument, "Password required")
	}
	if u.UUID == "" {
		return status.Error(codes.InvalidArgument, "UUID required")
	}

	if u.CreatedAt != "" {
		_, err := time.Parse(time.RFC3339, u.CreatedAt)
		if err != nil {
			return status.Error(codes.InvalidArgument, "Invalid CreatedAt format")
		}
	}

	return nil
}

func (s *UserServer) setLeader(id int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.currentLeader = id
	s.isLeader = (id == s.config.myID)
	if s.isLeader {
		s.saveLeaderToMarker(id)
		if s.healthCheckStop != nil {
			close(s.healthCheckStop)
			s.healthCheckStop = make(chan bool, 1)
			s.electionTriggeredMu.Lock()
			s.electionTriggered = false
			s.electionTriggeredMu.Unlock()
		}
	} else if !s.electionTriggered {
		go s.monitorLeaderHealth()
	}
}

func (s *UserServer) monitorLeaderHealth() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.healthCheckStop:
			return
		case <-ticker.C:
			s.mu.RLock()
			currentLeader := s.currentLeader
			isLeader := s.isLeader
			s.mu.RUnlock()

			if isLeader {
				continue
			}

			if !s.isLeaderAlive(currentLeader) && !s.leaderFailed {

				s.leaderFailed = true
				log.Printf("%s [Server %d] [Backup] Leader %d suspected failed", time.Now().Format("2006-01-02 15:04:05"), s.config.myID, currentLeader)
				s.leaderFailTimer = time.AfterFunc(10*time.Second, func() {
					s.electionTriggeredMu.Lock()
					s.electionTriggered = true
					log.Printf("%s [Server %d] [Backup] Timeout when connecting to Leader %d, starting Ring election after delay", time.Now().Format("2006-01-02 15:04:05"), s.config.myID, currentLeader)
					s.electionTriggeredMu.Unlock()
					s.electionTriggeredMu.Lock()
					s.initiateElection()
					s.electionTriggeredMu.Unlock()
				})

			} else if s.isLeaderAlive(currentLeader) && s.leaderFailed {
				s.electionTriggeredMu.Lock()
				log.Printf("%s [Server %d] [Backup] Leader %d suspected started", time.Now().Format("2006-01-02 15:04:05"), s.config.myID, currentLeader)
				s.leaderFailed = false
				if s.leaderFailTimer != nil {
					s.leaderFailTimer.Stop()
					s.leaderFailTimer = nil
				}
				s.electionTriggered = false
				s.electionTriggeredMu.Unlock()
			}
		}
	}
}

func (s *UserServer) isLeaderAlive(leaderID int) bool {
	if leaderID <= 0 {
		return false
	}

	leaderAddr := s.config.addressMap[leaderID]
	conn, err := grpc.NewClient(leaderAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return false
	}
	defer conn.Close()

	client := pb.NewElectionServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err = client.Ping(ctx, &pb.EmptyRequest{})
	return err == nil
}

func (s *UserServer) hasProcessed(reqID string) bool {
	s.reqMu.RLock()
	_, ok := s.processedRequests[reqID]
	s.reqMu.RUnlock()
	return ok
}

func (s *UserServer) markProcessed(reqID string) {
	s.reqMu.Lock()
	s.processedRequests[reqID] = true
	s.reqMu.Unlock()
}

func (s *UserServer) localInsert(req *pb.SetRequest) (*pb.SuccessResponse, error) {
	if err := validateUser(req.Data); err != nil {
		return nil, err
	}

	id := int(req.ID)
	_, existUser := s.config.db.Get(bucket, id)
	if existUser {
		return nil, status.Error(codes.AlreadyExists, "User already exists")
	}

	u := &User{
		CreatedAt: req.Data.CreatedAt,
		UUID:      req.Data.UUID,
		Email:     req.Data.Email,
		Password:  req.Data.Password,
		Image:     req.Data.Image,
		ID:        id,
		IsAdmin:   req.Data.IsAdmin,
	}
	userJSON, _ := json.Marshal(u)

	err := s.config.db.Set(bucket, id, userJSON)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.SuccessResponse{Success: true}, nil
}

func (s *UserServer) localSet(req *pb.SetRequest) (*pb.SuccessResponse, error) {
	if err := validateUser(req.Data); err != nil {
		return nil, err
	}

	id := int(req.ID)
	_, existUser := s.config.db.Get(bucket, id)
	if !existUser {
		return nil, status.Error(codes.NotFound, "User not found")
	}

	u := &User{
		CreatedAt: req.Data.CreatedAt,
		UUID:      req.Data.UUID,
		Email:     req.Data.Email,
		Password:  req.Data.Password,
		Image:     req.Data.Image,
		ID:        id,
		IsAdmin:   req.Data.IsAdmin,
	}
	userJSON, _ := json.Marshal(u)

	err := s.config.db.Set(bucket, id, userJSON)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.SuccessResponse{Success: true}, nil
}

func (s *UserServer) localDelete(req *pb.IDRequest) (*pb.SuccessResponse, error) {
	id := int(req.ID)
	_, existUser := s.config.db.Get(bucket, id)
	if !existUser {
		return nil, status.Error(codes.NotFound, "User not found")
	}

	_, err := s.config.db.Del(bucket, id)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.SuccessResponse{Success: true}, nil
}

func (s *UserServer) handleInsert(req *pb.SetRequest, isFromClient bool) (*pb.SuccessResponse, error) {
	if isFromClient && !s.isLeader {
		return nil, status.Error(codes.Unavailable, "Cannot connect to this server")
	}

	now := time.Now().Format("2006-01-02 15:04:05")
	role := "Backup"
	source := "Leader (replication)"
	if s.isLeader {
		role = "Leader"
		source = "Client"
	}

	log.Printf("%s [Server %d] [%s] Received Insert request from %s", now, s.config.myID, role, source)

	if req.RequestId == "" {
		req.RequestId = uuid.New().String()
	}
	if s.hasProcessed(req.RequestId) {
		return &pb.SuccessResponse{Success: true}, nil
	}

	resp, err := s.localInsert(req)
	if err != nil {
		return resp, err
	}

	if isFromClient {
		go s.replicate(req, "Insert")
	}

	s.markProcessed(req.RequestId)
	return resp, nil
}

func (s *UserServer) handleSet(req *pb.SetRequest, isFromClient bool) (*pb.SuccessResponse, error) {
	if isFromClient && !s.isLeader {
		return nil, status.Error(codes.Unavailable, "Cannot connect to this server")
	}

	now := time.Now().Format("2006-01-02 15:04:05")
	role := "Backup"
	source := "Leader (replication)"
	if s.isLeader {
		role = "Leader"
		source = "Client"
	}

	log.Printf("%s [Server %d] [%s] Received Set request from %s", now, s.config.myID, role, source)

	if req.RequestId == "" {
		req.RequestId = uuid.New().String()
	}
	if s.hasProcessed(req.RequestId) {
		return &pb.SuccessResponse{Success: true}, nil
	}

	resp, err := s.localSet(req)
	if err != nil {
		return resp, err
	}

	if isFromClient {
		go s.replicate(req, "Set")
	}

	s.markProcessed(req.RequestId)
	return resp, nil
}

func (s *UserServer) handleDelete(req *pb.IDRequest, isFromClient bool) (*pb.SuccessResponse, error) {
	if isFromClient && !s.isLeader {
		return nil, status.Error(codes.Unavailable, "Cannot connect to this server")
	}

	now := time.Now().Format("2006-01-02 15:04:05")
	role := "Backup"
	source := "Leader (replication)"
	if s.isLeader {
		role = "Leader"
		source = "Client"
	}

	log.Printf("%s [Server %d] [%s] Received Delete request from %s", now, s.config.myID, role, source)

	if req.RequestID == "" {
		req.RequestID = uuid.New().String()
	}
	if s.hasProcessed(req.RequestID) {
		return &pb.SuccessResponse{Success: true}, nil
	}

	resp, err := s.localDelete(req)
	if err != nil {
		return resp, err
	}

	if isFromClient {
		go s.replicate(req, "Delete")
	}

	s.markProcessed(req.RequestID)
	return resp, nil
}

func (s *UserServer) Insert(ctx context.Context, req *pb.SetRequest) (*pb.SuccessResponse, error) {
	return s.handleInsert(req, true)
}

func (s *UserServer) ReplicateInsert(ctx context.Context, req *pb.SetRequest) (*pb.SuccessResponse, error) {
	return s.handleInsert(req, false)
}

func (s *UserServer) Set(ctx context.Context, req *pb.SetRequest) (*pb.SuccessResponse, error) {
	return s.handleSet(req, true)
}

func (s *UserServer) ReplicateSet(ctx context.Context, req *pb.SetRequest) (*pb.SuccessResponse, error) {
	return s.handleSet(req, false)
}

func (s *UserServer) Delete(ctx context.Context, req *pb.IDRequest) (*pb.SuccessResponse, error) {
	return s.handleDelete(req, true)
}

func (s *UserServer) ReplicateDelete(ctx context.Context, req *pb.IDRequest) (*pb.SuccessResponse, error) {
	return s.handleDelete(req, false)
}

func (s *UserServer) Get(ctx context.Context, req *pb.IDRequest) (*pb.User, error) {
	if !s.isLeader {
		return nil, status.Error(codes.Unavailable, "Cannot connect to this server")
	}

	log.Printf("%s [Server %d] [Leader] Received Get request from Client", time.Now().Format("2006-01-02 15:04:05"), s.config.myID)

	data, ok := s.config.db.Get(bucket, int(req.ID))
	if !ok {
		return nil, status.Error(codes.NotFound, "User not found")
	}

	var u User
	if err := json.Unmarshal(data, &u); err != nil {
		return nil, status.Error(codes.Internal, "Failed to parse user data")
	}

	return &pb.User{
		CreatedAt: u.CreatedAt,
		UUID:      u.UUID,
		Email:     u.Email,
		Password:  u.Password,
		Image:     u.Image,
		ID:        int32(u.ID),
		IsAdmin:   u.IsAdmin,
	}, nil
}

func (s *UserServer) GetAll(ctx context.Context, req *pb.GetAllRequest) (*pb.GetAllResponse, error) {
	if !s.isLeader {
		return nil, status.Error(codes.Unavailable, "Cannot connect to this server")
	}

	log.Printf("%s [Server %d] [Leader] Received GetAll request from Client", time.Now().Format("2006-01-02 15:04:05"), s.config.myID)

	allData, err := s.config.db.GetAll(bucket)
	if err != nil {
		return nil, status.Error(codes.NotFound, "Bucket not found")
	}

	var ids []int
	var userMap = make(map[int][]byte)

	for id, data := range allData {
		ids = append(ids, id)
		userMap[id] = data
	}

	sort.Ints(ids)

	total := len(ids)

	offset := int(req.Offset)

	if req.Limit < 0 || req.Limit > 50000 {
		return nil, status.Error(codes.InvalidArgument, "Limit must be in range [0, 50000]")
	}
	limit := int(req.Limit)

	if offset >= total {
		return &pb.GetAllResponse{
			Users:  []*pb.User{},
			Total:  int32(total),
			Offset: int32(offset),
			Limit:  int32(limit),
		}, nil
	}

	end := min(offset+limit, total)

	var users []*pb.User
	for i := offset; i < end; i++ {
		id := ids[i]
		data := userMap[id]

		var u User
		if err := json.Unmarshal(data, &u); err != nil {
			continue
		}

		users = append(users, &pb.User{
			CreatedAt: u.CreatedAt,
			UUID:      u.UUID,
			Email:     u.Email,
			Password:  u.Password,
			Image:     u.Image,
			ID:        int32(u.ID),
			IsAdmin:   u.IsAdmin,
		})
	}

	return &pb.GetAllResponse{
		Users:  users,
		Total:  int32(total),
		Offset: int32(offset),
		Limit:  int32(limit),
	}, nil
}

func (s *UserServer) Count(ctx context.Context, req *pb.EmptyRequest) (*pb.CountResponse, error) {
	if !s.isLeader {
		return nil, status.Error(codes.Unavailable, "Cannot connect to this server")
	}

	log.Printf("%s [Server %d] [Leader] Received Count request from Client", time.Now().Format("2006-01-02 15:04:05"), s.config.myID)

	all, err := s.config.db.GetAll(bucket)
	if err != nil {
		return &pb.CountResponse{Count: 0}, nil
	}

	count := len(all)
	return &pb.CountResponse{Count: int32(count)}, nil
}
