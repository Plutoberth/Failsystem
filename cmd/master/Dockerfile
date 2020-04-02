FROM golang:alpine as build-env

WORKDIR /go/src/Failsystem
COPY . .

RUN go mod download
RUN go install -v ./cmd/master/

FROM alpine

COPY --from=build-env /go/bin/master /
EXPOSE 1337
CMD ["/master"]