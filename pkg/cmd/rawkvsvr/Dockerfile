# docker build -t rawkvsvr:latest -f ./pkg/cmd/rawkvsvr/Dockerfile .
# docker container run --publish 5640:5640 --detach --name rawkvsvr rawkvsvr:latest
FROM golang:1.13
WORKDIR /rawkvsvr
COPY . .
RUN go install ./pkg/cmd/rawkvsvr
CMD $GOPATH/bin/rawkvsvr -storageaddr :5640 -raftaddr :5641 -loglevel 1
