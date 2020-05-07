FROM golang:alpine as build-env

WORKDIR /go/src/Failsystem
COPY . .

RUN go mod download
RUN go install ./cmd/minion/

# TODO: Move to distroless
FROM alpine

COPY --from=build-env /go/bin/minion /
EXPOSE 1337
CMD ["/minion"]