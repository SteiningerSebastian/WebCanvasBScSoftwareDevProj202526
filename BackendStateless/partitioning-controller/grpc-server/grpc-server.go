// Implemented by Claude Sonet 4.5

package grpcserver

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	partitioningcontroller "grpc-general/partitioning-controller"
	pc "partitioning-controller/partitioning-controller"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GrpcServer implements the DatabaseServer interface from the generated protobuf code
type GrpcServer struct {
	partitioningcontroller.UnimplementedDatabaseServer
	controller *pc.PartitioningController
}

// NewGrpcServer creates a new gRPC server instance
func NewGrpcServer(controller *pc.PartitioningController) *GrpcServer {
	return &GrpcServer{
		controller: controller,
	}
}

// Set implements the Set RPC method
func (s *GrpcServer) Set(ctx context.Context, req *partitioningcontroller.Data) (*partitioningcontroller.Commit, error) {
	slog.Debug(fmt.Sprintf("Received Set request for key: %d", req.Key))

	// Convert key to pixel coordinates
	x := uint16(req.Key >> 16)
	y := uint16(req.Key & 0xFFFF)

	// Parse color from value bytes
	if len(req.Value) != 3 {
		return &partitioningcontroller.Commit{
			Index:  0,
			Status: 0b0010, // failure
		}, status.Error(codes.InvalidArgument, "invalid color value, expected 3 bytes")
	}

	color := pc.Color{
		R: req.Value[0],
		G: req.Value[1],
		B: req.Value[2],
	}

	// Perform the set operation
	doneChan, errorChan, err := s.controller.Set(ctx, x, y, color)

	// Wait for either an error or the done channel to be closed. (Write achived.)
	select {
	case <-doneChan:
	case e := <-errorChan:
		err = e
	}

	if err != nil {
		slog.Error("Failed to set pixel", "error", err)

		// Check error type and return appropriate status
		if _, ok := err.(*pc.QuorumNotReached); ok {
			return &partitioningcontroller.Commit{
				Index:  0,
				Status: 0b0010, // failure
			}, status.Error(codes.Unavailable, "quorum not reached")
		}

		return &partitioningcontroller.Commit{
			Index:  0,
			Status: 0b0010, // failure
		}, status.Error(codes.Internal, err.Error())
	}

	return &partitioningcontroller.Commit{
		Index:  0,
		Status: 0b0001, // success
	}, nil
}

// Get implements the Get RPC method
func (s *GrpcServer) Get(ctx context.Context, req *partitioningcontroller.DataRequest) (*partitioningcontroller.DataResponse, error) {
	slog.Debug(fmt.Sprintf("Received Get request for key: %d", req.Key))

	// Convert key to pixel coordinates
	x := uint16(req.Key >> 16)
	y := uint16(req.Key & 0xFFFF)

	// Perform the get operation
	colorChan, errChan, err := s.controller.Get(ctx, x, y)
	if err != nil {
		slog.Error("Failed to get pixel", "error", err)

		// Check error type and return appropriate status
		if _, ok := err.(*pc.QuorumNotReached); ok {
			return &partitioningcontroller.DataResponse{
				Key:    req.Key,
				Value:  nil,
				Status: 2, // error
			}, nil
		}

		return &partitioningcontroller.DataResponse{
			Key:    req.Key,
			Value:  nil,
			Status: 2, // error
		}, nil
	}

	var color *pc.Color

	// Wait for either a resposne / color or an error
	select {
	case col := <-colorChan:
		color = col
	case err := <-errChan:
		slog.Error(err.Error())
		return &partitioningcontroller.DataResponse{
			Key:    req.Key,
			Value:  nil,
			Status: 2, // error
		}, nil
	}

	if color == nil {
		return &partitioningcontroller.DataResponse{
			Key:    req.Key,
			Value:  nil,
			Status: 1, // not found
		}, nil
	}

	return &partitioningcontroller.DataResponse{
		Key:    req.Key,
		Value:  color.ToBytes(),
		Status: 0, // success
	}, nil
}

// GetAll implements the GetAll RPC method (server streaming)
func (s *GrpcServer) GetAll(req *partitioningcontroller.StreamRequest, stream grpc.ServerStreamingServer[partitioningcontroller.DataResponse]) error {
	slog.Debug("Received GetAll request")

	ctx := stream.Context()

	// Get all data from the controller
	dataChan, errorChan, err := s.controller.GetAll(ctx)
	if err != nil {
		slog.Error("Failed to start GetAll operation", "error", err)
		return status.Error(codes.Internal, err.Error())
	}

	// Stream data back to client
	for {
		select {
		case <-ctx.Done():
			slog.Info("GetAll stream context cancelled")
			return ctx.Err()
		case err := <-errorChan:
			if err != nil {
				slog.Error("Error during GetAll streaming", "error", err)
				return status.Error(codes.Internal, err.Error())
			}
		case data, ok := <-dataChan:
			if !ok {
				// Channel closed, streaming complete
				slog.Debug("GetAll streaming completed")
				return nil
			}

			// Convert pixel coordinates to key
			key := (uint32(data.X) << 16) | uint32(data.Y)

			// Send data to client
			err := stream.Send(&partitioningcontroller.DataResponse{
				Key:    key,
				Value:  data.Value.ToBytes(),
				Status: 0, // success
			})
			if err != nil {
				slog.Error("Failed to send data to client", "error", err)
				return status.Error(codes.Internal, err.Error())
			}
		}
	}
}

// StartServer starts the gRPC server on the specified port
func StartServer(port int, controller *pc.PartitioningController) error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %w", port, err)
	}

	grpcServer := grpc.NewServer()
	server := NewGrpcServer(controller)

	partitioningcontroller.RegisterDatabaseServer(grpcServer, server)

	slog.Info(fmt.Sprintf("Starting gRPC server on port %d", port))

	if err := grpcServer.Serve(listener); err != nil {
		return fmt.Errorf("failed to serve: %w", err)
	}

	return nil
}
