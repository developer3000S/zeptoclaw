FROM golang:1.26

WORKDIR /app

COPY . .

RUN go mod download

RUN go build -o ui-server ./cmd/ui

EXPOSE 28090

CMD ["./ui-server"]