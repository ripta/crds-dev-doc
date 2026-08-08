package main

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestMain initializes the package-level logger, which is otherwise only set
// in main(). Helpers that log would nil-pointer panic without it.
func TestMain(m *testing.M) {
	logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	os.Exit(m.Run())
}

func TestParseLogLevel(t *testing.T) {
	tests := []struct {
		name  string
		value string
		set   bool
		want  slog.Level
	}{
		{name: "unset defaults to info", set: false, want: slog.LevelInfo},
		{name: "empty defaults to info", value: "", set: true, want: slog.LevelInfo},
		{name: "debug", value: "debug", set: true, want: slog.LevelDebug},
		{name: "info", value: "info", set: true, want: slog.LevelInfo},
		{name: "warn", value: "warn", set: true, want: slog.LevelWarn},
		{name: "warning alias", value: "warning", set: true, want: slog.LevelWarn},
		{name: "error", value: "error", set: true, want: slog.LevelError},
		{name: "uppercase is folded", value: "DEBUG", set: true, want: slog.LevelDebug},
		{name: "mixed case is folded", value: "WaRnInG", set: true, want: slog.LevelWarn},
		{name: "unrecognized defaults to info", value: "trace", set: true, want: slog.LevelInfo},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.set {
				t.Setenv("LOG_LEVEL", tt.value)
			} else {
				os.Unsetenv("LOG_LEVEL")
			}

			if got := parseLogLevel(); got != tt.want {
				t.Errorf("parseLogLevel() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetPersistPath(t *testing.T) {
	t.Run("uses env value when set", func(t *testing.T) {
		t.Setenv(persistPathEnv, "/var/lib/gitter")
		if got := getPersistPath(); got != "/var/lib/gitter" {
			t.Errorf("getPersistPath() = %q, want %q", got, "/var/lib/gitter")
		}
	})

	t.Run("falls back to default when empty", func(t *testing.T) {
		t.Setenv(persistPathEnv, "")
		if got := getPersistPath(); got != defaultPersistPath {
			t.Errorf("getPersistPath() = %q, want %q", got, defaultPersistPath)
		}
	})

	t.Run("falls back to default when unset", func(t *testing.T) {
		os.Unsetenv(persistPathEnv)
		if got := getPersistPath(); got != defaultPersistPath {
			t.Errorf("getPersistPath() = %q, want %q", got, defaultPersistPath)
		}
	})
}

func TestRepoExists(t *testing.T) {
	t.Run("true when .git is a directory", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.Mkdir(filepath.Join(dir, ".git"), 0o755); err != nil {
			t.Fatalf("failed to create .git dir: %v", err)
		}
		if !repoExists(dir) {
			t.Error("repoExists() = false, want true")
		}
	})

	// Worktree and submodule checkouts have a .git file, not a directory.
	t.Run("false when .git is a file", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, ".git"), []byte("gitdir: ../elsewhere\n"), 0o644); err != nil {
			t.Fatalf("failed to create .git file: %v", err)
		}
		if repoExists(dir) {
			t.Error("repoExists() = true, want false")
		}
	})

	t.Run("false when .git is absent", func(t *testing.T) {
		if repoExists(t.TempDir()) {
			t.Error("repoExists() = true, want false")
		}
	})

	t.Run("false when dir does not exist", func(t *testing.T) {
		if repoExists(filepath.Join(t.TempDir(), "nope")) {
			t.Error("repoExists() = true, want false")
		}
	})
}

func TestTruncateError(t *testing.T) {
	t.Run("short message is unchanged", func(t *testing.T) {
		msg := "git clone error: repository not found"
		if got := truncateError(msg); got != msg {
			t.Errorf("truncateError() = %q, want %q", got, msg)
		}
	})

	t.Run("message at the limit is unchanged", func(t *testing.T) {
		msg := strings.Repeat("x", maxErrorLength)
		got := truncateError(msg)
		if got != msg {
			t.Errorf("truncateError() changed a message of exactly maxErrorLength (len %d)", len(got))
		}
	})

	t.Run("message over the limit is truncated and marked", func(t *testing.T) {
		msg := strings.Repeat("x", maxErrorLength+1)
		got := truncateError(msg)

		if !strings.HasPrefix(got, strings.Repeat("x", maxErrorLength)) {
			t.Error("truncateError() did not preserve the first maxErrorLength bytes")
		}
		if !strings.HasSuffix(got, "[truncated at 950 bytes]") {
			t.Errorf("truncateError() = %q, want a truncation suffix", got[len(got)-40:])
		}
	})

	// The marker is appended, not substituted, so attempts.error must fit
	// more than maxErrorLength bytes.
	t.Run("result exceeds the limit by the marker length", func(t *testing.T) {
		got := truncateError(strings.Repeat("x", 10_000))
		if len(got) <= maxErrorLength {
			t.Errorf("len(truncateError()) = %d, want > %d", len(got), maxErrorLength)
		}
	})
}

func TestBuildInsert(t *testing.T) {
	const prefix = "INSERT INTO crds VALUES "

	tests := []struct {
		name          string
		argsPerInsert int
		numInsert     int
		want          string
	}{
		{
			name:          "single row",
			argsPerInsert: 6,
			numInsert:     1,
			want:          prefix + "($1,$2,$3,$4,$5,$6)",
		},
		{
			name:          "two rows continue numbering",
			argsPerInsert: 6,
			numInsert:     2,
			want:          prefix + "($1,$2,$3,$4,$5,$6),($7,$8,$9,$10,$11,$12)",
		},
		{
			name:          "single arg per row",
			argsPerInsert: 1,
			numInsert:     3,
			want:          prefix + "($1),($2),($3)",
		},
		{
			name:          "zero rows emits no placeholders",
			argsPerInsert: 6,
			numInsert:     0,
			want:          prefix,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := buildInsert(prefix, tt.argsPerInsert, tt.numInsert); got != tt.want {
				t.Errorf("buildInsert() = %q, want %q", got, tt.want)
			}
		})
	}
}

// The placeholder count must equal the arg count passed to Exec, or pgx
// rejects the statement at runtime.
func TestBuildInsertPlaceholderCount(t *testing.T) {
	for _, numInsert := range []int{1, 2, 5, 50, 300} {
		got := buildInsert("VALUES ", crdArgCount, numInsert)

		want := crdArgCount * numInsert
		if n := strings.Count(got, "$"); n != want {
			t.Errorf("buildInsert(%d rows) produced %d placeholders, want %d", numInsert, n, want)
		}
		if !strings.Contains(got, "$"+strconv.Itoa(want)+")") {
			t.Errorf("buildInsert(%d rows) does not end its last row at $%d", numInsert, want)
		}
		if strings.Contains(got, "$"+strconv.Itoa(want+1)) {
			t.Errorf("buildInsert(%d rows) emitted a placeholder past $%d", numInsert, want)
		}
	}
}
