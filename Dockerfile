FROM golang:1.26-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /relayd ./cmd/relayd

FROM alpine:3.22

RUN apk add --no-cache ca-certificates \
	&& addgroup -S relayd \
	&& adduser -S -G relayd relayd
COPY --from=builder /relayd /usr/local/bin/relayd
USER relayd
EXPOSE 8080
ENTRYPOINT ["relayd"]
CMD ["serve"]
