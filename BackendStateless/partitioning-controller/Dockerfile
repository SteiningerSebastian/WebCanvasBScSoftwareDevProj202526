FROM golang:1.25.5-alpine AS builder

WORKDIR /app

# Copy the entire workspace structure
COPY go.work ./
COPY general ./general
COPY grpc-general ./grpc-general
COPY veritas-client ./veritas-client
COPY partitioning-controller ./partitioning-controller

# Set working directory to the partitioning-controller
WORKDIR /app/partitioning-controller

# Download dependencies using the workspace
RUN go mod download

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o main .

# Production stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy built application from builder stage
COPY --from=builder /app/partitioning-controller/main .

# Expose the port the app runs on
EXPOSE 3000

# Run the application
CMD ["./main"]