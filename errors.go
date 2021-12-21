package streammux

import "errors"

var (
	// ErrInvalidMessage is returned when an unknown message type was detected
	ErrInvalidMessage = errors.New("invalid message")
	// ErrInvalidResponse indicates that the client responded with malformed or unknown message
	ErrInvalidResponse error = errors.New("invalid response")
	// ErrMessageReadTimeout is returned when read timed out
	ErrMessageReadTimeout = errors.New("incoming message read tiemd out")
	// ErrMessageWriteTimeout is returned when write timed out
	ErrMessageWriteTimeout = errors.New("incoming message write tiemd out")
)

// ErrorMessage represents an error sent by the other end
type ErrorMessage struct {
	error
}
