package main

import (
	"context"

	pb "github.com/marcelloh/fastdb/user"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

type Client struct {
	conn   *grpc.ClientConn
	client pb.UserServiceClient
}

func NewClient(addr string) (*Client, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Client{
		conn:   conn,
		client: pb.NewUserServiceClient(conn),
	}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) GetUser(id int32) (*User, error) {
	resp, err := c.client.Get(context.Background(), &pb.GetRequest{ID: id})
	if err != nil {
		return nil, err
	}
	return &User{
		CreatedAt: resp.CreatedAt,
		UUID:      resp.UUID,
		Email:     resp.Email,
		Password:  resp.Password,
		Image:     resp.Image,
		ID:        int(id),
		IsAdmin:   resp.IsAdmin,
	}, nil
}

func (c *Client) GetAllUsers() ([]*User, error) {
	resp, err := c.client.GetAll(context.Background(), &pb.GetAllRequest{})
	if err != nil {
		return nil, err
	}
	var users []*User
	for _, pbUser := range resp.Users {
		users = append(users, &User{
			CreatedAt: pbUser.CreatedAt,
			UUID:      pbUser.UUID,
			Email:     pbUser.Email,
			Password:  pbUser.Password,
			Image:     pbUser.Image,
			IsAdmin:   pbUser.IsAdmin,
		})
	}
	return users, nil
}

func (c *Client) CountUsers() (int32, error) {
	resp, err := c.client.Count(context.Background(), &pb.EmptyRequest{})
	if err != nil {
		return 0, err
	}
	return resp.Count, nil
}

func (c *Client) InsertUser(user *User) (bool, error) {
	resp, err := c.client.Insert(context.Background(), &pb.SetRequest{
		ID: int32(user.ID),
		Data: &pb.User{
			CreatedAt: user.CreatedAt,
			UUID:      user.UUID,
			Email:     user.Email,
			Password:  user.Password,
			Image:     user.Image,
			IsAdmin:   user.IsAdmin,
		},
	})
	if err != nil {
		return false, err
	}
	return resp.Success, nil
}

func (c *Client) UpdateUser(user *User) (bool, error) {
	resp, err := c.client.Set(context.Background(), &pb.SetRequest{
		ID: int32(user.ID),
		Data: &pb.User{
			CreatedAt: user.CreatedAt,
			UUID:      user.UUID,
			Email:     user.Email,
			Password:  user.Password,
			Image:     user.Image,
			IsAdmin:   user.IsAdmin,
		},
	})
	if err != nil {
		return false, err
	}
	return resp.Success, nil
}

func (c *Client) DeleteUser(id int32) (bool, error) {
	resp, err := c.client.Delete(context.Background(), &pb.DeleteRequest{ID: id})
	if err != nil {
		return false, err
	}
	return resp.Success, nil
}
