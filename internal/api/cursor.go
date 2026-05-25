package api

import (
	"encoding/base64"
	"encoding/json"
	"errors"
)

// cursorPayload is the wire format inside an opaque cursor.
// Versioned via the struct shape so future fields stay backward-compatible.
type cursorPayload struct {
	ID int64 `json:"id"`
}

// encodeCursor builds an opaque base64url cursor from a row ID.
// Used for pagination "next page" pointer.
func encodeCursor(id int64) string {
	payload, _ := json.Marshal(cursorPayload{ID: id})
	return base64.RawURLEncoding.EncodeToString(payload)
}

// decodeCursor unwraps a cursor back to its row ID.
// Returns error for empty, non-base64, or non-JSON inputs.
func decodeCursor(cursor string) (int64, error) {
	if cursor == "" {
		return 0, errors.New("cursor is empty")
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, err
	}
	var p cursorPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return 0, err
	}
	return p.ID, nil
}
