package main

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

// TestMain initializes the package-level logger, which is otherwise only set
// in main(). loggingMiddleware would nil-pointer panic without it.
func TestMain(m *testing.M) {
	logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	os.Exit(m.Run())
}

func TestGetClientIP(t *testing.T) {
	tests := []struct {
		name       string
		headers    map[string]string
		remoteAddr string
		want       string
	}{
		{
			name:       "falls back to RemoteAddr host",
			remoteAddr: "203.0.113.5:54321",
			want:       "203.0.113.5",
		},
		{
			name:       "CF-Connecting-IP wins",
			headers:    map[string]string{"CF-Connecting-IP": "198.51.100.1"},
			remoteAddr: "203.0.113.5:54321",
			want:       "198.51.100.1",
		},
		{
			name: "True-Client-IP beats X-Real-IP and XFF",
			headers: map[string]string{
				"True-Client-IP":  "198.51.100.2",
				"X-Real-IP":       "198.51.100.3",
				"X-Forwarded-For": "198.51.100.4",
			},
			remoteAddr: "203.0.113.5:54321",
			want:       "198.51.100.2",
		},
		{
			name: "CF-Connecting-IP beats True-Client-IP",
			headers: map[string]string{
				"CF-Connecting-IP": "198.51.100.1",
				"True-Client-IP":   "198.51.100.2",
			},
			remoteAddr: "203.0.113.5:54321",
			want:       "198.51.100.1",
		},
		{
			name:       "X-Real-IP beats XFF",
			headers:    map[string]string{"X-Real-IP": "198.51.100.3", "X-Forwarded-For": "198.51.100.4"},
			remoteAddr: "203.0.113.5:54321",
			want:       "198.51.100.3",
		},
		{
			name:       "XFF uses the leftmost entry",
			headers:    map[string]string{"X-Forwarded-For": "198.51.100.4, 10.0.0.1, 10.0.0.2"},
			remoteAddr: "203.0.113.5:54321",
			want:       "198.51.100.4",
		},
		{
			name:       "XFF entries are trimmed",
			headers:    map[string]string{"X-Forwarded-For": "  198.51.100.4  ,10.0.0.1"},
			remoteAddr: "203.0.113.5:54321",
			want:       "198.51.100.4",
		},
		{
			name:       "single-entry XFF",
			headers:    map[string]string{"X-Forwarded-For": "198.51.100.4"},
			remoteAddr: "203.0.113.5:54321",
			want:       "198.51.100.4",
		},
		{
			name:       "blank XFF falls through to RemoteAddr",
			headers:    map[string]string{"X-Forwarded-For": "   "},
			remoteAddr: "203.0.113.5:54321",
			want:       "203.0.113.5",
		},
		{
			name:       "empty headers are ignored",
			headers:    map[string]string{"CF-Connecting-IP": "", "X-Real-IP": ""},
			remoteAddr: "203.0.113.5:54321",
			want:       "203.0.113.5",
		},
		// Searching for the last colon left the brackets attached.
		{
			name:       "bracketed IPv6 loses its brackets and port",
			remoteAddr: "[2001:db8::1]:54321",
			want:       "2001:db8::1",
		},
		{
			name:       "IPv6 loopback with port",
			remoteAddr: "[::1]:54321",
			want:       "::1",
		},
		// Searching for the last colon turned "::1" into ":".
		{
			name:       "bare IPv6 without a port is returned intact",
			remoteAddr: "::1",
			want:       "::1",
		},
		{
			name:       "bare IPv4 without a port is returned intact",
			remoteAddr: "203.0.113.5",
			want:       "203.0.113.5",
		},
		{
			name:       "empty RemoteAddr",
			remoteAddr: "",
			want:       "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			r.RemoteAddr = tt.remoteAddr
			for k, v := range tt.headers {
				r.Header.Set(k, v)
			}

			if got := getClientIP(r); got != tt.want {
				t.Errorf("getClientIP() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestResponseRecorder(t *testing.T) {
	t.Run("records an explicit status and byte count", func(t *testing.T) {
		rr := &responseRecorder{ResponseWriter: httptest.NewRecorder(), statusCode: http.StatusOK}

		rr.WriteHeader(http.StatusNotFound)
		n, err := rr.Write([]byte("not found"))
		if err != nil {
			t.Fatalf("Write() error = %v", err)
		}

		if n != len("not found") {
			t.Errorf("Write() = %d, want %d", n, len("not found"))
		}
		if rr.statusCode != http.StatusNotFound {
			t.Errorf("statusCode = %d, want %d", rr.statusCode, http.StatusNotFound)
		}
		if rr.bytes != len("not found") {
			t.Errorf("bytes = %d, want %d", rr.bytes, len("not found"))
		}
	})

	t.Run("accumulates bytes across writes", func(t *testing.T) {
		rr := &responseRecorder{ResponseWriter: httptest.NewRecorder(), statusCode: http.StatusOK}

		rr.Write([]byte("abc"))
		rr.Write([]byte("de"))

		if rr.bytes != 5 {
			t.Errorf("bytes = %d, want 5", rr.bytes)
		}
	})

	t.Run("passes the status through to the underlying writer", func(t *testing.T) {
		under := httptest.NewRecorder()
		rr := &responseRecorder{ResponseWriter: under, statusCode: http.StatusOK}

		rr.WriteHeader(http.StatusTeapot)

		if under.Code != http.StatusTeapot {
			t.Errorf("underlying status = %d, want %d", under.Code, http.StatusTeapot)
		}
	})
}

func TestLoggingMiddleware(t *testing.T) {
	t.Run("passes the request through", func(t *testing.T) {
		called := false
		h := loggingMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			called = true
			w.WriteHeader(http.StatusCreated)
			w.Write([]byte("hello"))
		}))

		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

		if !called {
			t.Error("middleware did not call the next handler")
		}
		if rec.Code != http.StatusCreated {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusCreated)
		}
		if rec.Body.String() != "hello" {
			t.Errorf("body = %q, want %q", rec.Body.String(), "hello")
		}
	})

	// A handler that never calls WriteHeader still returns 200, so the
	// recorder must not log a zero status.
	t.Run("defaults to 200 when the handler does not set a status", func(t *testing.T) {
		h := loggingMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte("implicit"))
		}))

		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
		}
	})
}
