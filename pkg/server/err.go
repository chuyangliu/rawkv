package server

import (
	"errors"
)

// Errors returned by grpc servers.
var (
	ErrNotReady       error = errors.New("Serve not ready")
	ErrCanceled       error = errors.New("Request canceled by client")
	ErrInternal       error = errors.New("Internal server error (please see server's logs for more details)")
	ErrLeaderNotFound error = errors.New("Leader not found (please try again)")
)
