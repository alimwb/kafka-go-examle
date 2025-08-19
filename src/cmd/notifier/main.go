// Notifier main.go (fixed and enhanced)
// Changes:
// - Added support for logging trace_id from message headers (new feature using Kafka headers and Go's string processing).
// - Minor fixes: Improved topic existence check in ensureTopic to query specific topic partitions.
// - Added random failure simulation in processMessage for better testing (occasional timeout to trigger retries/DLQ).
// - Ensured DLQ write includes trace_id if present.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
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

// -------------------- config --------------------

type Config struct {
	Broker       string
	Topic        string
	Group        string
	DLQTopic     string
	MinBytes     int
	MaxBytes     int
	MaxWait      time.Duration
	SessionTO    time.Duration
	CommitEvery  time.Duration
	ProcessTO    time.Duration
	RetryMax     int
	RetryBackoff time.Duration
	HTTPAddr     string
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

func loadConfig() Config {
	return Config{
		Broker:       getenv("KAFKA_BROKER", "localhost:9094"),
		Topic:        getenv("KAFKA_TOPIC", "users.signin"),
		Group:        getenv("KAFKA_GROUP", "notifier"),
		DLQTopic:     getenv("KAFKA_DLQ_TOPIC", "users.signin.dlq"),
		MinBytes:     getenvInt("KAFKA_MIN_BYTES", 1),
		MaxBytes:     getenvInt("KAFKA_MAX_BYTES", 10<<20), // 10MB
		MaxWait:      getenvDur("KAFKA_MAX_WAIT", 250*time.Millisecond),
		SessionTO:    getenvDur("KAFKA_SESSION_TIMEOUT", 10*time.Second),
		CommitEvery:  getenvDur("KAFKA_COMMIT_INTERVAL", 0),
		ProcessTO:    getenvDur("PROCESS_TIMEOUT", 5*time.Second),
		RetryMax:     getenvInt("PROCESS_RETRY_MAX", 2),
		RetryBackoff: getenvDur("PROCESS_RETRY_BACKOFF", 300*time.Millisecond),
		HTTPAddr:     getenv("HTTP_ADDR", ":8081"),
	}
}

// -------------------- kafka helpers --------------------

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
		return nil // Topic exists
	}
	return conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	})
}

func newDLQWriter(broker, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:                   kafka.TCP(broker),
		Topic:                  topic,
		Balancer:               &kafka.LeastBytes{},
		RequiredAcks:           -1,
		Async:                  false,
		Compression:            kafka.Snappy,
		AllowAutoTopicCreation: true,
	}
}

// -------------------- metrics & dashboard state --------------------

var (
	metricIn     atomic.Int64
	metricOK     atomic.Int64
	metricErr    atomic.Int64
	metricDLQ    atomic.Int64
	metricCommit atomic.Int64

	lastType    atomic.Value
	lastVersion atomic.Value
	lastUser    atomic.Value
	lastEmail   atomic.Value
	lastStatus  atomic.Value
	lastErr     atomic.Value
	lastTraceID atomic.Value // New: track trace_id from headers
)

func setLastKV(k *atomic.Value, v string) { k.Store(v) }
func getLastKV(k *atomic.Value) string {
	if v := k.Load(); v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return "-"
}

// -------------------- processing --------------------

func getHeaderValue(headers []kafka.Header, key string) string {
	for _, h := range headers {
		if strings.EqualFold(h.Key, key) {
			return string(h.Value)
		}
	}
	return ""
}

func validateHeaders(m kafka.Message) (typ string, ver int, err error) {
	var t, v string
	for _, h := range m.Headers {
		switch strings.ToLower(h.Key) {
		case "type":
			t = string(h.Value)
		case "version":
			v = string(h.Value)
		}
	}
	if t == "" {
		return "", 0, errors.New("missing header: type")
	}
	if v == "" {
		return "", 0, errors.New("missing header: version")
	}
	vi, err := strconv.Atoi(v)
	if err != nil {
		return "", 0, fmt.Errorf("invalid version header: %w", err)
	}
	return t, vi, nil
}

func processMessage(ctx context.Context, m kafka.Message) error {
	metricIn.Add(1)

	traceID := getHeaderValue(m.Headers, "trace_id") // New: extract trace_id
	setLastKV(&lastTraceID, traceID)

	typ, ver, err := validateHeaders(m)
	setLastKV(&lastType, typ)
	setLastKV(&lastVersion, strconv.Itoa(ver))
	if err != nil {
		return err
	}
	if typ != "UserSignin" || ver != 1 {
		return fmt.Errorf("unsupported message: type=%s version=%d", typ, ver)
	}

	var ev events.UserSignin
	if err := json.Unmarshal(m.Value, &ev); err != nil {
		return fmt.Errorf("unmarshal failed: %w", err)
	}
	setLastKV(&lastUser, ev.UserID)
	setLastKV(&lastEmail, ev.Email)

	// simulate a side effect (e.g., send an email) with a timeout
	// New: random failure simulation (10% chance to timeout)
	pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	sleepDuration := 120 * time.Millisecond
	if rand.Intn(10) == 0 { // 10% chance of failure
		sleepDuration = 3 * time.Second // Cause timeout
	}

	select {
	case <-time.After(sleepDuration):
		metricOK.Add(1)
		setLastKV(&lastStatus, "processed")
		setLastKV(&lastErr, "")
		return nil
	case <-pctx.Done():
		return pctx.Err()
	}
}

// -------------------- http --------------------

func startHTTP(cfg Config, l *slog.Logger) *http.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintf(w, "notifier_messages_in %d\n", metricIn.Load())
		fmt.Fprintf(w, "notifier_messages_ok %d\n", metricOK.Load())
		fmt.Fprintf(w, "notifier_messages_err %d\n", metricErr.Load())
		fmt.Fprintf(w, "notifier_messages_dlq %d\n", metricDLQ.Load())
		fmt.Fprintf(w, "notifier_commits %d\n", metricCommit.Load())
	})

	// Simple HTML dashboard (auto-refresh)
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprintf(w, `
<!doctype html>
<title>Notifier Dashboard</title>
<meta http-equiv="refresh" content="2">
<style>body{font-family:system-ui;margin:24px} .ok{color:green} .err{color:#b00020}</style>
<h2>Notifier Dashboard</h2>
<ul>
  <li><b>Broker:</b> %s</li>
  <li><b>Topic:</b> %s</li>
  <li><b>Group:</b> %s</li>
  <li><b>Last trace_id:</b> %s</li>
  <li><b>Last type:</b> %s</li>
  <li><b>Last version:</b> %s</li>
  <li><b>Last user:</b> %s</li>
  <li><b>Last email:</b> %s</li>
  <li><b>Last status:</b> %s</li>
  <li><b>Last error:</b> <span class="err">%s</span></li>
</ul>
<h3>Metrics</h3>
<ul>
  <li>messages_in: %d</li>
  <li>messages_ok: %d</li>
  <li>messages_err: %d</li>
  <li>messages_dlq: %d</li>
  <li>commits: %d</li>
</ul>
<p><a href="/healthz">/healthz</a> • <a href="/metrics">/metrics</a> • <a href="/debug/pprof">/pprof</a></p>
`,
			cfg.Broker, cfg.Topic, cfg.Group,
			getLastKV(&lastTraceID), getLastKV(&lastType), getLastKV(&lastVersion), getLastKV(&lastUser),
			getLastKV(&lastEmail), getLastKV(&lastStatus), getLastKV(&lastErr),
			metricIn.Load(), metricOK.Load(), metricErr.Load(), metricDLQ.Load(), metricCommit.Load())
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
	rand.Seed(time.Now().UnixNano()) // For random failure simulation
	cfg := loadConfig()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	ctx, cancel := signalContext()
	defer cancel()

	// Ensure topics
	{
		c, ccancel := context.WithTimeout(ctx, 5*time.Second)
		if err := ensureTopic(c, cfg.Broker, cfg.Topic, 3); err != nil {
			logger.Error("ensure topic failed", "topic", cfg.Topic, "err", err)
			os.Exit(1)
		}
		if err := ensureTopic(c, cfg.Broker, cfg.DLQTopic, 1); err != nil {
			logger.Error("ensure DLQ topic failed", "topic", cfg.DLQTopic, "err", err)
			os.Exit(1)
		}
		ccancel()
	}

	// HTTP dashboard
	httpSrv := startHTTP(cfg, logger)
	defer func() { _ = httpSrv.Shutdown(context.Background()) }()

	// Reader
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{cfg.Broker},
		Topic:          cfg.Topic,
		GroupID:        cfg.Group,
		MinBytes:       cfg.MinBytes,
		MaxBytes:       cfg.MaxBytes,
		MaxWait:        cfg.MaxWait,
		SessionTimeout: cfg.SessionTO,
		CommitInterval: cfg.CommitEvery, // 0 => synchronous commit each message
		StartOffset:    kafka.FirstOffset,
	})
	defer r.Close()

	// DLQ
	dlq := newDLQWriter(cfg.Broker, cfg.DLQTopic)
	defer dlq.Close()

	logger.Info("notifier is running...",
		"broker", cfg.Broker, "topic", cfg.Topic, "group", cfg.Group, "dlq", cfg.DLQTopic)

	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				logger.Info("context canceled, exiting")
				return
			}
			logger.Warn("fetch error", "err", err)
			continue
		}

		var perr error
		for attempt := 0; attempt <= cfg.RetryMax; attempt++ {
			pctx, pcancel := context.WithTimeout(ctx, cfg.ProcessTO)
			perr = processMessage(pctx, m)
			pcancel()
			if perr == nil {
				break
			}
			time.Sleep(time.Duration(attempt+1) * cfg.RetryBackoff)
		}

		if perr != nil {
			metricErr.Add(1)
			setLastKV(&lastStatus, "failed")
			setLastKV(&lastErr, perr.Error())

			headers := append(m.Headers, kafka.Header{Key: "error", Value: []byte(perr.Error())})
			dlqMsg := kafka.Message{
				Key:     m.Key,
				Value:   m.Value,
				Time:    time.Now(),
				Headers: headers,
			}
			if err := dlq.WriteMessages(ctx, dlqMsg); err != nil {
				logger.Error("DLQ write failed", "err", err)
			} else {
				metricDLQ.Add(1)
			}
		}

		if err := r.CommitMessages(ctx, m); err != nil {
			logger.Warn("commit error", "err", err)
		} else {
			metricCommit.Add(1)
		}
	}
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
