// Producer main.go (fixed to remove Idempotent field)
// Changes from previous:
// - Removed Idempotent field from Config and newWriter to fix the "Unknown field" error.
// - Added comment about enabling idempotence at the Kafka broker level.
// - Retained metrics, async completion handling, and improved topic creation from previous fixes.
// - Note: For true idempotence, ensure Kafka broker has enable.idempotence=true and use Acks=-1 with retries.

package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/alimwb/kafka-go-examle/src/internal/events"
)

// -------------------- metrics --------------------

var (
	metricSent atomic.Int64
	metricErr  atomic.Int64

	lastTraceID atomic.Value
	lastStatus  atomic.Value
	lastError   atomic.Value
)

func valOrDash(v any) string {
	if v == nil {
		return "-"
	}
	if s, ok := v.(string); ok && s != "" {
		return s
	}
	return "-"
}

// -------------------- config --------------------

type Config struct {
	Broker       string
	Topic        string
	Acks         int
	Async        bool
	Compression  string
	BatchBytes   int
	BatchTimeout time.Duration
	Timeout      time.Duration
	RetryMax     int
	RetryBackoff time.Duration
	HTTPAddr     string
	Oneshot      bool
}

func getenv(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func getenvDur(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func getenvBool(key string, def bool) bool {
	v := strings.ToLower(os.Getenv(key))
	if v == "true" {
		return true
	} else if v == "false" {
		return false
	}
	return def
}

func loadConfig() Config {
	return Config{
		Broker:       getenv("KAFKA_BROKER", "localhost:9094"),
		Topic:        getenv("KAFKA_TOPIC", "users.signin"),
		Acks:         getenvInt("KAFKA_ACKS", -1), // 0,1,-1
		Async:        getenvBool("KAFKA_ASYNC", false),
		Compression:  strings.ToLower(getenv("KAFKA_COMPRESSION", "snappy")), // snappy|gzip|lz4|zstd|none
		BatchBytes:   getenvInt("KAFKA_BATCH_BYTES", 64<<10),
		BatchTimeout: getenvDur("KAFKA_BATCH_TIMEOUT", 50*time.Millisecond),
		Timeout:      getenvDur("KAFKA_TIMEOUT", 5*time.Second),
		RetryMax:     getenvInt("KAFKA_RETRY_MAX", 5),
		RetryBackoff: getenvDur("KAFKA_RETRY_BACKOFF", 200*time.Millisecond),
		HTTPAddr:     getenv("HTTP_ADDR", ":8080"),
		Oneshot:      getenvBool("PRODUCER_ONESHOT", false),
	}
}

// -------------------- kafka helpers --------------------

func compressionCodec(name string) kafka.Compression {
	switch strings.ToLower(name) {
	case "gzip":
		return kafka.Gzip
	case "lz4":
		return kafka.Lz4
	case "zstd":
		return kafka.Zstd
	case "none":
		return 0
	default:
		return kafka.Snappy
	}
}

func ensureTopic(ctx context.Context, broker, topic string, partitions int) error {
	conn, err := kafka.DialContext(ctx, "tcp", broker)
	if err != nil {
		return fmt.Errorf("dial broker: %w", err)
	}
	defer conn.Close()

	parts, err := conn.ReadPartitions(topic)
	if err != nil {
		return fmt.Errorf("read partitions: %w", err)
	}
	if len(parts) > 0 {
		return nil // Topic exists if partitions are returned
	}
	return conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: 1, // single-node friendly
	})
}

func newWriter(cfg Config) *kafka.Writer {
	// Note: For idempotent producer, ensure Kafka broker has enable.idempotence=true
	// and use Acks=-1 with retries for exactly-once semantics.
	w := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.Broker),
		Topic:                  cfg.Topic,
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequiredAcks(cfg.Acks),
		Async:                  cfg.Async,
		Compression:            compressionCodec(cfg.Compression),
		BatchBytes:             int64(cfg.BatchBytes),
		BatchTimeout:           cfg.BatchTimeout,
		AllowAutoTopicCreation: true,
	}
	if cfg.Async {
		w.Completion = func(messages []kafka.Message, err error) {
			if err != nil {
				slog.Error("async write failed", "err", err)
				metricErr.Add(1)
				lastError.Store(err.Error())
			} else {
				metricSent.Add(int64(len(messages)))
			}
		}
	}
	return w
}

func newTraceID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// -------------------- http --------------------

func startHTTP(l *slog.Logger, cfg Config, send func(ctx context.Context, msg kafka.Message) error) *http.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintf(w, "producer_messages_sent %d\n", metricSent.Load())
		fmt.Fprintf(w, "producer_messages_err %d\n", metricErr.Load())
	})

	// Simple HTML dashboard (auto-refresh)
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprintf(w, `
<!doctype html>
<title>Producer Dashboard</title>
<meta http-equiv="refresh" content="2">
<style>body{font-family:system-ui;margin:24px} .err{color:#b00020}</style>
<h2>Producer Dashboard</h2>
<ul>
  <li><b>Broker:</b> %s</li>
  <li><b>Topic:</b> %s</li>
  <li><b>Last trace_id:</b> %s</li>
  <li><b>Last status:</b> %s</li>
  <li><b>Last error:</b> <span class="err">%s</span></li>
</ul>
<h3>Metrics</h3>
<ul>
  <li>messages_sent: %d</li>
  <li>messages_err: %d</li>
</ul>
<p><a href="/healthz">/healthz</a> • <a href="/metrics">/metrics</a> • <a href="/debug/pprof">/pprof</a></p>
<form method="post" action="/produce" style="margin-top:16px">
  <h3>Send a message</h3>
  <input name="user_id" placeholder="user id" required>
  <input name="email" placeholder="email" required>
  <button type="submit">Produce</button>
</form>
`, cfg.Broker, cfg.Topic,
			valOrDash(lastTraceID.Load()),
			valOrDash(lastStatus.Load()),
			valOrDash(lastError.Load()),
			metricSent.Load(), metricErr.Load())
	})

	// POST /produce : from form or JSON
	mux.HandleFunc("/produce", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			_, _ = w.Write([]byte("use POST"))
			return
		}
		var in struct {
			UserID string `json:"user_id"`
			Email  string `json:"email"`
		}
		ct := r.Header.Get("Content-Type")
		if strings.Contains(ct, "application/json") {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte("bad body"))
				return
			}
			defer r.Body.Close()
			if err := json.Unmarshal(body, &in); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`expected JSON: {"user_id":"...","email":"..."}`))
				return
			}
		} else {
			_ = r.ParseForm()
			in.UserID = r.FormValue("user_id")
			in.Email = r.FormValue("email")
		}

		if in.UserID == "" || in.Email == "" {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte("user_id and email are required"))
			return
		}

		ev := events.UserSignin{
			Event:   "user_signin",
			Version: 1,
			UserID:  in.UserID,
			Email:   in.Email,
			TS:      time.Now().UTC(),
		}
		payload, err := json.Marshal(ev)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("marshal failed"))
			return
		}

		trace := newTraceID()
		msg := kafka.Message{
			Key:   []byte(ev.UserID),
			Value: payload,
			Time:  time.Now(),
			Headers: []kafka.Header{
				{Key: "type", Value: []byte("UserSignin")},
				{Key: "version", Value: []byte("1")},
				{Key: "trace_id", Value: []byte(trace)},
			},
		}

		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		if err := send(ctx, msg); err != nil {
			lastStatus.Store("failed")
			lastError.Store(err.Error())
			metricErr.Add(1)
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte("send failed"))
			return
		}
		lastTraceID.Store(trace)
		lastStatus.Store("sent")
		lastError.Store("")
		if !cfg.Async {
			metricSent.Add(1) // For sync, increment here; async handled in completion
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	srv := &http.Server{
		Addr:         cfg.HTTPAddr,
		Handler:      mux,
		ReadTimeout:  4 * time.Second,
		WriteTimeout: 4 * time.Second,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			l.Error("http server failed", "err", err)
		}
	}()
	return srv
}

// -------------------- main --------------------

func main() {
	cfg := loadConfig()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	ctx, cancel := signalContext()
	defer cancel()

	if err := tcpCheck(cfg.Broker, 2*time.Second); err != nil {
		logger.Warn("broker tcp check failed", "broker", cfg.Broker, "err", err)
	}

	// Ensure topic
	{
		c, ccancel := context.WithTimeout(ctx, 5*time.Second)
		if err := ensureTopic(c, cfg.Broker, cfg.Topic, 3); err != nil {
			logger.Error("ensure topic failed", "topic", cfg.Topic, "err", err)
			os.Exit(1)
		}
		ccancel()
	}

	w := newWriter(cfg)
	defer func() {
		if err := w.Close(); err != nil {
			logger.Warn("writer close error", "err", err)
		}
	}()

	// send with retry/backoff
	send := func(ctx context.Context, msg kafka.Message) error {
		var lastErr error
		for attempt := 0; attempt <= cfg.RetryMax; attempt++ {
			c, cancel := context.WithTimeout(ctx, cfg.Timeout)
			err := w.WriteMessages(c, msg)
			cancel()
			if err == nil {
				return nil
			}
			lastErr = err
			time.Sleep(time.Duration(attempt+1) * cfg.RetryBackoff)
		}
		return lastErr
	}

	httpSrv := startHTTP(logger, cfg, send)
	defer func() { _ = httpSrv.Shutdown(context.Background()) }()

	// oneshot vs service
	if cfg.Oneshot {
		ev := events.UserSignin{
			Event:   "user_signin",
			Version: 1,
			UserID:  "u-111",
			Email:   "alice@example.com",
			TS:      time.Now().UTC(),
		}
		payload, _ := json.Marshal(ev)
		trace := newTraceID()
		msg := kafka.Message{
			Key:   []byte(ev.UserID),
			Value: payload,
			Time:  time.Now(),
			Headers: []kafka.Header{
				{Key: "type", Value: []byte("UserSignin")},
				{Key: "version", Value: []byte("1")},
				{Key: "trace_id", Value: []byte(trace)},
			},
		}
		c, cancel := context.WithTimeout(ctx, cfg.Timeout)
		err := send(c, msg)
		cancel()
		if err != nil {
			lastStatus.Store("failed")
			lastError.Store(err.Error())
			metricErr.Add(1)
			logger.Error("oneshot send failed", "err", err)
			os.Exit(2)
		}
		lastTraceID.Store(trace)
		lastStatus.Store("sent")
		lastError.Store("")
		if !cfg.Async {
			metricSent.Add(1)
		}
		logger.Info("message sent", "topic", cfg.Topic, "key", ev.UserID, "trace_id", trace)
		return
	}

	logger.Info("producer is running as a service", "broker", cfg.Broker, "topic", cfg.Topic, "http", cfg.HTTPAddr)
	<-ctx.Done()
}

func tcpCheck(addr string, d time.Duration) error {
	dialer := net.Dialer{Timeout: d}
	conn, err := dialer.Dial("tcp", addr)
	if err != nil {
		return err
	}
	_ = conn.Close()
	return nil
}

func signalContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		<-ch
		cancel()
	}()
	return ctx, cancel
}
