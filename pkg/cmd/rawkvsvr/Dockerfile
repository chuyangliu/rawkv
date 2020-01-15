# Build: docker image build -t rawkvsvr:0.1.0 -f ./pkg/cmd/rawkvsvr/Dockerfile .
# Run: docker container run --publish 8000:8000 --detach --name rawkvsvr rawkvsvr:0.1.0
FROM golang:1.13.2
WORKDIR /rawkvsvr
COPY . .
RUN go install ./pkg/cmd/rawkvsvr
CMD $GOPATH/bin/rawkvsvr -storageaddr :8000
