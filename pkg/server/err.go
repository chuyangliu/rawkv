package server

import (
	"errors"
)

// ErrNotReady indicates the server is still initializing and not ready to server requests.
var ErrNotReady error = errors.New("Serve not ready")

// ErrInternal indicates there is something wrong during server's internal processing.
var ErrInternal error = errors.New("Internal server error (please see server's logs for more details)")
