package server

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
)

func TLSConfig(certFile, keyFile, caFile string, logger *slog.Logger) (*tls.Config, error) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	r := &certReloader{certFile: certFile, keyFile: keyFile, log: logger}
	if err := r.reload(); err != nil {
		return nil, err
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, GetCertificate: r.get}
	if caFile == "" {
		return cfg, nil
	}
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("server: TLS CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("server: TLS CA %s holds no PEM certificates", caFile)
	}
	cfg.ClientCAs = pool
	cfg.ClientAuth = tls.RequireAndVerifyClientCert
	return cfg, nil
}

type certReloader struct {
	certFile, keyFile string
	log               *slog.Logger

	mu       sync.Mutex
	cert     *tls.Certificate
	modified [2]time.Time
}

func (r *certReloader) get(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.reload(); err != nil {
		r.log.Warn("TLS certificate not reloaded, serving the previous one", "err", err)
	}
	return r.cert, nil
}

func (r *certReloader) reload() error {
	var modified [2]time.Time
	for i, path := range []string{r.certFile, r.keyFile} {
		st, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("server: TLS certificate: %w", err)
		}
		modified[i] = st.ModTime()
	}
	if r.cert != nil && modified == r.modified {
		return nil
	}
	cert, err := tls.LoadX509KeyPair(r.certFile, r.keyFile)
	if err != nil {
		return fmt.Errorf("server: TLS certificate: %w", err)
	}
	r.cert, r.modified = &cert, modified
	r.log.Info("TLS certificate loaded", "cert", r.certFile)
	return nil
}
