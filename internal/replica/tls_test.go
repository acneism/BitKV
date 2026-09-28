package replica

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	file string
	dir  string
	next int64
}

func writePEM(t *testing.T, path, kind string, der []byte) {
	t.Helper()
	must(t, os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: der}), 0o600))
}

func newTestCA(t *testing.T, name string) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	must(t, err)
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             now.Add(-24 * time.Hour),
		NotAfter:              now.Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	must(t, err)
	cert, err := x509.ParseCertificate(der)
	must(t, err)
	dir := t.TempDir()
	ca := &testCA{cert: cert, key: key, file: filepath.Join(dir, "ca.crt"), dir: dir, next: 2}
	writePEM(t, ca.file, "CERTIFICATE", der)
	return ca
}

func (ca *testCA) issue(t *testing.T, cn string, dns []string, usage []x509.ExtKeyUsage) (string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	must(t, err)
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(ca.next),
		Subject:      pkix.Name{CommonName: cn},
		DNSNames:     dns,
		NotBefore:    now.Add(-24 * time.Hour),
		NotAfter:     now.Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  usage,
	}
	ca.next++
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	must(t, err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	must(t, err)
	name := strings.Join(append([]string{cn}, dns...), "-")
	certFile, keyFile := filepath.Join(ca.dir, name+".crt"), filepath.Join(ca.dir, name+".key")
	writePEM(t, certFile, "CERTIFICATE", der)
	writePEM(t, keyFile, "EC PRIVATE KEY", keyDER)
	return certFile, keyFile
}

var bothWays = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}

func (ca *testCA) nodeTLS(t *testing.T, id string) *tls.Config {
	t.Helper()
	cert, key := ca.issue(t, id, []string{id}, bothWays)
	cfg, err := TLSConfig(id, cert, key, ca.file)
	must(t, err)
	return cfg
}

func TestMutualTLSCluster(t *testing.T) {
	ca := newTestCA(t, "casketdb test ca")
	nodes := newCluster(t, 3, false, func(tn *testNode) { tn.tls = ca.nodeTLS(t, tn.id) })
	l := leader(t, nodes)
	for range 20 {
		must(t, incr(l, "counter"))
	}
	eventually(t, "replication over mTLS", converged(nodes, "counter", "20", 1))
}

func TestMutualTLSRejectsStrangers(t *testing.T) {
	ca := newTestCA(t, "casketdb test ca")
	other := newTestCA(t, "someone else's ca")
	for name, setup := range map[string]func(*testNode){
		"foreign CA":  func(tn *testNode) { tn.tls = other.nodeTLS(t, tn.id) },
		"without TLS": func(tn *testNode) { tn.tls = nil },
	} {
		t.Run(name, func(t *testing.T) {
			nodes := newCluster(t, 3, false, func(tn *testNode) {
				tn.tls = ca.nodeTLS(t, tn.id)
				if tn.id == "n2" {
					setup(tn)
				}
			})
			stranger := nodes[2]
			l := leader(t, nodes[:2])
			must(t, put(l, "k", "v"))
			eventually(t, "the trusted nodes to replicate", converged(nodes[:2], "k", "v", 1))
			time.Sleep(300 * time.Millisecond)
			if got := get(stranger, "k"); got != "" {
				t.Fatalf("the node outside the PKI received %q", got)
			}
			if st := stranger.node.Status(); st.LeaderID != "" {
				t.Fatalf("the node outside the PKI follows %s", st.LeaderID)
			}
		})
	}
}

func TestTLSConfigChecksTheCertificate(t *testing.T) {
	ca := newTestCA(t, "casketdb test ca")
	good, goodKey := ca.issue(t, "n0", []string{"n0"}, bothWays)
	if _, err := TLSConfig("n0", good, goodKey, ca.file); err != nil {
		t.Fatal(err)
	}
	cnOnly, cnOnlyKey := ca.issue(t, "n1", nil, bothWays)
	serverOnly, serverOnlyKey := ca.issue(t, "n2", []string{"n2"}, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	notPEM := filepath.Join(t.TempDir(), "empty.crt")
	must(t, os.WriteFile(notPEM, []byte("not a certificate"), 0o600))
	for name, tc := range map[string]struct {
		id, cert, key, ca, want string
	}{
		"another node's certificate": {"n1", good, goodKey, ca.file, `identifies node "n0"`},
		"no DNS name":                {"n1", cnOnly, cnOnlyKey, ca.file, `needs the DNS name "n1"`},
		"server-only usage":          {"n2", serverOnly, serverOnlyKey, ca.file, "both server and client"},
		"missing key":                {"n0", good, good + ".missing", ca.file, "raft TLS certificate"},
		"CA without certificates":    {"n0", good, goodKey, notPEM, "holds no PEM certificates"},
	} {
		if _, err := TLSConfig(tc.id, tc.cert, tc.key, tc.ca); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s: %v, want an error containing %q", name, err, tc.want)
		}
	}
}
