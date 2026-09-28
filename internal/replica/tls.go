package replica

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"slices"

	"github.com/acneism/raft"
	"github.com/acneism/raft/transport"
)

func TLSConfig(id, certFile, keyFile, caFile string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("replica: raft TLS certificate: %w", err)
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return nil, fmt.Errorf("replica: raft TLS certificate: %w", err)
	}
	if got, err := transport.Identity(tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf}}); err != nil || got != raft.NodeID(id) {
		return nil, fmt.Errorf("replica: raft TLS certificate %s identifies node %q, this node is %q", certFile, got, id)
	}
	if err := leaf.VerifyHostname(id); err != nil {
		return nil, fmt.Errorf("replica: raft TLS certificate %s needs the DNS name %q: %w", certFile, id, err)
	}
	if !usableBothWays(leaf.ExtKeyUsage) {
		return nil, fmt.Errorf("replica: raft TLS certificate %s must allow both server and client authentication", certFile)
	}
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("replica: raft TLS CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("replica: raft TLS CA %s holds no PEM certificates", caFile)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ClientCAs:    pool,
		MinVersion:   tls.VersionTLS13,
	}, nil
}

func usableBothWays(usages []x509.ExtKeyUsage) bool {
	if len(usages) == 0 || slices.Contains(usages, x509.ExtKeyUsageAny) {
		return true
	}
	return slices.Contains(usages, x509.ExtKeyUsageServerAuth) && slices.Contains(usages, x509.ExtKeyUsageClientAuth)
}
