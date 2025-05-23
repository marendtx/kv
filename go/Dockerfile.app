FROM golang:1.24 AS builder

WORKDIR /app

ENV CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64

COPY ./go.mod ./
RUN go mod download

COPY ./*.go ./
RUN go build -o myapp

FROM alpine:3.20

WORKDIR /app

COPY --from=builder /app/myapp .

CMD ["./myapp"]