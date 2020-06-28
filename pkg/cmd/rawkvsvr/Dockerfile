FROM golang:1.13
WORKDIR /rawkvsvr
COPY . .
RUN go install ./pkg/cmd/rawkvsvr
CMD $GOPATH/bin/rawkvsvr -storageaddr :5640 -raftaddr :5641 -rootdir . -loglevel 1
