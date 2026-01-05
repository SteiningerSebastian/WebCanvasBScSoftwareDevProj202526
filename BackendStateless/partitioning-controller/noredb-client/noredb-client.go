package noredbclient

import (
	"context"
	"fmt"
	noredb "grpc-general/noreddb"
	"log/slog"

	"google.golang.org/grpc"
)

type ClientError struct {
	Message string
}

func (e *ClientError) Error() string {
	return e.Message
}

type PixelResponse struct {
	Index     uint32
	Key       uint32
	Timestamp []byte
	PixelData []byte
}

// NoreDBClient is a client for interacting with a NoreDB service.
type NoreDBClient struct {
	conn   *grpc.ClientConn
	client noredb.DatabaseClient
}

type NoreDBClientInterface interface {
	// Get retrieves pixel data from the NoreDB service.
	Get(ctx context.Context, index uint32, key uint32) (*PixelResponse, error)

	// Set stores pixel data in the NoreDB service.
	Set(ctx context.Context, index uint32, key uint32, pixelData []byte) error

	// GetAll retrieves all pixel data for a given index from the NoreDB service.
	GetAll(ctx context.Context, index uint32) (chan<- *PixelResponse, chan<- error, error)
}

// NewNoreDBClient creates a new instance of NoreDBClient.
func NewNoreDBClient(endpoint string) *NoreDBClient {
	conn, err := grpc.NewClient(endpoint, grpc.EmptyDialOption{})
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to create gRPC client: %v\n", err))
		return nil
	}

	client := noredb.NewDatabaseClient(conn)

	return &NoreDBClient{conn: conn, client: client}
}

// Close closes the NoreDBClient connection.
func (c *NoreDBClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// Get retrieves pixel data from the NoreDB service.
func (c *NoreDBClient) Get(ctx context.Context, index uint32, key uint32) (*PixelResponse, error) {
	rsp, err := c.client.Get(ctx, &noredb.DataRequest{
		Index: index,
		Key:   key,
	})

	if err != nil {
		slog.Error(fmt.Sprintf("Error during Get request: %v\n", err))
		return nil, err
	}

	return &PixelResponse{
		Index:     rsp.Index,
		Key:       rsp.Key,
		Timestamp: rsp.Timestamp,
		PixelData: rsp.Value,
	}, nil
}

func (c *NoreDBClient) Set(ctx context.Context, index uint32, key uint32, pixelData []byte, timestamp []byte) error {
	rsp, err := c.client.Set(ctx, &noredb.Data{
		Index:     index,
		Key:       key,
		Value:     pixelData,
		Timestamp: timestamp,
	})

	if err != nil {
		slog.Error(fmt.Sprintf("Error during Set request: %v\n", err))
		return err
	}

	if rsp.Status != 1 {
		slog.Error("Set operation failed on server.")
		return &ClientError{Message: "Set operation failed on server."}
	}

	return nil
}

func (c *NoreDBClient) GetAll(ctx context.Context, index uint32) (chan<- *PixelResponse, chan<- error, error) {
	rspChan := make(chan *PixelResponse)
	errChan := make(chan error)

	stream, err := c.client.GetAll(ctx, &noredb.StreamRequest{
		Index: index,
	})
	if err != nil {
		slog.Error(fmt.Sprintf("Error initiating GetAll stream: %v\n", err))
		return nil, nil, err
	}

	go func() {
		defer close(rspChan)
		defer close(errChan)
		for {
			rsp, err := stream.Recv()
			if err != nil {
				slog.Error(fmt.Sprintf("Error receiving from GetAll stream: %v\n", err))
				errChan <- err
				return
			}
			pixelResponse := &PixelResponse{
				Index:     rsp.Index,
				Key:       rsp.Key,
				Timestamp: rsp.Timestamp,
				PixelData: rsp.Value,
			}
			rspChan <- pixelResponse
		}
	}()

	return rspChan, errChan, nil
}
