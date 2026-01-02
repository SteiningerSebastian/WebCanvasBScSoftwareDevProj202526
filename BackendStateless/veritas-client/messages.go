package veritasclient

import (
	"errors"
	"fmt"
)

// ParseError represents errors that can occur during message parsing
type ParseError struct {
	Type    string
	Message string
}

func (e *ParseError) Error() string {
	switch e.Type {
	case "ParameterNotFound":
		return fmt.Sprintf("Error: Missing argument: %s", e.Message)
	case "InvalidCommandType":
		return fmt.Sprintf("Error: Wrong command type: %s", e.Message)
	default:
		return e.Message
	}
}

func NewParameterNotFoundError(msg string) *ParseError {
	return &ParseError{Type: "ParameterNotFound", Message: msg}
}

func NewInvalidCommandTypeError(msg string) *ParseError {
	return &ParseError{Type: "InvalidCommandType", Message: msg}
}

// WebsocketCommand represents a command sent via websocket
type WebsocketCommand struct {
	Command    string     `json:"command"`
	Parameters [][]string `json:"parameters"`
}

// WatchCommand represents a watch command with a key
type WatchCommand struct {
	Key string
}

// ToWatchCommand converts a WebsocketCommand into a WatchCommand if possible.
// Returns an error if the command type is incorrect or if the "key" parameter is missing.
func (wc *WebsocketCommand) ToWatchCommand() (*WatchCommand, error) {
	if wc.Command != "WatchCommand" {
		return nil, NewInvalidCommandTypeError(wc.Command)
	}

	// Look for the "key" parameter
	for _, param := range wc.Parameters {
		if len(param) == 2 && param[0] == "key" {
			return &WatchCommand{Key: param[1]}, nil
		}
	}

	return nil, NewParameterNotFoundError("key")
}

// FromWatchCommand creates a WebsocketCommand from a WatchCommand.
// Returns a WebsocketCommand with the appropriate command type and parameters.
func FromWatchCommand(watchCmd *WatchCommand) *WebsocketCommand {
	return &WebsocketCommand{
		Command:    "WatchCommand",
		Parameters: [][]string{{"key", watchCmd.Key}},
	}
}

// WebsocketResponse represents a response sent via websocket
type WebsocketResponse struct {
	Command    string     `json:"command"`
	Parameters [][]string `json:"parameters"`
}

// UpdateNotification represents a notification about a value update
type UpdateNotification struct {
	Key      string
	NewValue string
	// Be aware that the OldValue is NOT linearizable or guaranteed to be temporally ordered.
	// Only for the atomic operations get_and_add and compare_set old_value is guaranteed to be the value before the operation.
	// This is because we do not want to load the value of the quorum before setting it, as this would introduce additional latency and complexity.
	// For the mentioned operations we must query the quorum anyway to ensure atomicity. (So we can get the old value at that point.)
	OldValue string
}

// ToUpdateNotification converts a WebsocketResponse into an UpdateNotification if possible.
// Returns an error if the command type is incorrect or if any required parameters are missing.
func (wr *WebsocketResponse) ToUpdateNotification() (*UpdateNotification, error) {
	if wr.Command != "UpdateNotification" {
		return nil, NewInvalidCommandTypeError(wr.Command)
	}

	var key, newValue, oldValue *string

	for _, param := range wr.Parameters {
		if len(param) == 2 {
			switch param[0] {
			case "key":
				key = &param[1]
			case "new_value":
				newValue = &param[1]
			case "old_value":
				oldValue = &param[1]
			}
		}
	}

	if key != nil && newValue != nil && oldValue != nil {
		return &UpdateNotification{
			Key:      *key,
			NewValue: *newValue,
			OldValue: *oldValue,
		}, nil
	}

	return nil, NewParameterNotFoundError("One or more parameters missing")
}

// FromUpdateNotification creates a WebsocketResponse from an UpdateNotification.
// Returns a WebsocketResponse with the appropriate command type and parameters.
func FromUpdateNotification(update *UpdateNotification) *WebsocketResponse {
	return &WebsocketResponse{
		Command: "UpdateNotification",
		Parameters: [][]string{
			{"key", update.Key},
			{"new_value", update.NewValue},
			{"old_value", update.OldValue},
		},
	}
}

// Helper function to get parameter value by name
func getParameter(parameters [][]string, name string) (string, error) {
	for _, param := range parameters {
		if len(param) == 2 && param[0] == name {
			return param[1], nil
		}
	}
	return "", errors.New("parameter not found: " + name)
}
