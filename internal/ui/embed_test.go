package ui_test

import (
	"testing"
	"github.com/developer3000S/zeptoclaw/internal/ui"
)

func TestEmbeddedFiles(t *testing.T) {
	files := []string{
		"static/index.html",
		"static/css/style.css",
		"static/js/main.js",
	}

	for _, file := range files {
		data, err := ui.StaticFS.ReadFile(file)
		if err != nil {
			t.Errorf("failed to read embedded file %s: %v", file, err)
		}
		if len(data) == 0 {
			t.Errorf("embedded file %s is empty", file)
		}
	}
}
