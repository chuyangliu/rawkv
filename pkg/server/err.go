package server

import (
	"errors"
)

// ErrNotReady indicates the server is still initializing and not ready to server requests.
var ErrNotReady error = errors.New("Serve not ready")

// ErrCanceled indicates the request has been canceled by the client.
var ErrCanceled error = errors.New("Request canceled by client")

// ErrInternal indicates there is something wrong during server's internal processing.
var ErrInternal error = errors.New("Internal server error (please see server's logs for more details)")

// ErrLeaderNotFound indicates the node that receives the request is not the leader and it does not know where the
// leader is.
var ErrLeaderNotFound error = errors.New("Leader not found (please try again)")
