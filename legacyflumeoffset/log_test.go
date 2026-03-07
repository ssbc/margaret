// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

package legacyflumeoffset

import (
	"path/filepath"
	"testing"

	mtest "github.com/ssbc/margaret/v2/test"
)

func newLFOLog(dir string) (mtest.Log, error) {
	return Open[*mtest.Entry](filepath.Join(dir, "log.offset"))
}

func TestLogGet(t *testing.T) {
	t.Run("Get", mtest.LogTestGet(newLFOLog))
}

func TestLogSimple(t *testing.T) {
	t.Run("Simple", mtest.LogTestSimple(newLFOLog))
}
