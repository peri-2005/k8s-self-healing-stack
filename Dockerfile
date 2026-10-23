# Stage 1: Build source code safely
FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o healer main.go

# Stage 2: Deploy execution binary inside lightweight production image
FROM alpine:3.19
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /app/healer .
ENTRYPOINT ["./healer"]
