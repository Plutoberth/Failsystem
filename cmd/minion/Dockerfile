FROM golang:alpine as build-env

WORKDIR /go/src/Failsystem
COPY . .

RUN go mod download
RUN go install -v ./cmd/minion/

# Distroless would be better, but there are some bugs with it currently
FROM alpine

COPY --from=build-env /go/bin/minion /
EXPOSE 1337
CMD ["/minion"]