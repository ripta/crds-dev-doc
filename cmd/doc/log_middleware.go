package main

import (
	"net"
	"net/http"
	"strings"
	"time"
)

// responseRecorder wraps http.ResponseWriter to capture status code and bytes written
type responseRecorder struct {
	http.ResponseWriter
	statusCode int
	bytes      int
}

func (rr *responseRecorder) WriteHeader(statusCode int) {
	rr.statusCode = statusCode
	rr.ResponseWriter.WriteHeader(statusCode)
}

func (rr *responseRecorder) Write(b []byte) (int, error) {
	n, err := rr.ResponseWriter.Write(b)
	rr.bytes += n
	return n, err
}

// getClientIP extracts the real client IP from request headers. The headers
// come from the proxy in front of this process and are spoofable, so the
// result is only used for logging.
func getClientIP(r *http.Request) string {
	for _, header := range []string{"CF-Connecting-IP", "True-Client-IP", "X-Real-IP"} {
		if ip := r.Header.Get(header); ip != "" {
			return ip
		}
	}

	// X-Forwarded-For accumulates left to right.
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		first, _, _ := strings.Cut(xff, ",")
		if first = strings.TrimSpace(first); first != "" {
			return first
		}
	}

	// SplitHostPort handles the bracketed IPv6 form, and fails when RemoteAddr
	// carries no port at all.
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		recorder := &responseRecorder{
			ResponseWriter: w,
			statusCode:     http.StatusOK, // default if WriteHeader is not called
			bytes:          0,
		}

		next.ServeHTTP(recorder, r)

		logger.Debug("request handled",
			"method", r.Method,
			"path", r.URL.Path,
			"status", recorder.statusCode,
			"duration", time.Since(start),
			"bytes", recorder.bytes,
			"ip", getClientIP(r),
		)
	})
}
