package lsm

import "errors"

var ErrKeyTombstoned = errors.New("key has been tombstoned (deleted)")
