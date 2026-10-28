FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/http-ingest ./examples/http-ingest
FROM alpine:3.21
COPY --from=build /out/http-ingest /http-ingest
EXPOSE 8080
ENTRYPOINT ["/http-ingest"]
