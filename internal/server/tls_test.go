package server

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/acneism/casketdb/internal/testcert"
)

var (
	serverAuth = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	clientAuth = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
)

func serveTLS(t *testing.T, cfg *tls.Config) string {
	t.Helper()
	srv, db, _ := startServer(t, t.TempDir())
	t.Cleanup(func() { stopServer(t, srv, db) })
	ln, err := tls.Listen("tcp", "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(ln) }()
	return ln.Addr().String()
}

func clientTLS(t *testing.T, ca *testcert.CA) *tls.Config {
	t.Helper()
	pem, err := os.ReadFile(ca.File)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(pem)
	return &tls.Config{RootCAs: pool, ServerName: "localhost"}
}

func ping(conn net.Conn) (string, error) {
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.WriteString(conn, "*1\r\n$4\r\nPING\r\n"); err != nil {
		return "", err
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	return strings.TrimSpace(line), err
}

func pingTLS(addr string, cfg *tls.Config) (string, error) {
	conn, err := tls.Dial("tcp", addr, cfg)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	return ping(conn)
}

func TestTLS(t *testing.T) {
	ca := testcert.New(t, "casketdb test ca")
	cert, key := ca.Issue(t, "server", []string{"localhost"}, serverAuth)
	cfg, err := TLSConfig(cert, key, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	addr := serveTLS(t, cfg)
	if got, err := pingTLS(addr, clientTLS(t, ca)); err != nil || got != "+PONG" {
		t.Fatalf("PING over TLS = %q, %v", got, err)
	}
	plain, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	if got, _ := ping(plain); got == "+PONG" {
		t.Fatal("the TLS port answered a plain-text client")
	}
	if _, err := TLSConfig(cert, key+".missing", "", nil); err == nil {
		t.Fatal("TLSConfig accepted a missing key")
	}
	if _, err := TLSConfig(cert, key, cert+".missing", nil); err == nil {
		t.Fatal("TLSConfig accepted a missing CA")
	}
}

func TestTLSClientCertificates(t *testing.T) {
	ca := testcert.New(t, "casketdb test ca")
	cert, key := ca.Issue(t, "server", []string{"localhost"}, serverAuth)
	cfg, err := TLSConfig(cert, key, ca.File, nil)
	if err != nil {
		t.Fatal(err)
	}
	addr := serveTLS(t, cfg)
	if got, err := pingTLS(addr, clientTLS(t, ca)); err == nil {
		t.Fatalf("a client without a certificate got %q", got)
	}
	stranger := testcert.New(t, "someone else's ca")
	for name, issuer := range map[string]*testcert.CA{"trusted": ca, "foreign": stranger} {
		ccert, ckey := issuer.Issue(t, "app", nil, clientAuth)
		pair, err := tls.LoadX509KeyPair(ccert, ckey)
		if err != nil {
			t.Fatal(err)
		}
		withCert := clientTLS(t, ca)
		withCert.Certificates = []tls.Certificate{pair}
		got, err := pingTLS(addr, withCert)
		if name == "trusted" && (err != nil || got != "+PONG") {
			t.Fatalf("a client with a trusted certificate got %q, %v", got, err)
		}
		if name == "foreign" && err == nil {
			t.Fatalf("a client with a foreign certificate got %q", got)
		}
	}
}

func TestTLSReloadsTheCertificate(t *testing.T) {
	ca := testcert.New(t, "casketdb test ca")
	cert, key := ca.Issue(t, "first", []string{"localhost"}, serverAuth)
	cfg, err := TLSConfig(cert, key, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	addr := serveTLS(t, cfg)
	served := func() string {
		t.Helper()
		conn, err := tls.Dial("tcp", addr, clientTLS(t, ca))
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		return conn.ConnectionState().PeerCertificates[0].Subject.CommonName
	}
	if cn := served(); cn != "first" {
		t.Fatalf("served %q, want first", cn)
	}
	next, nextKey := ca.Issue(t, "second", []string{"localhost"}, serverAuth)
	later := time.Now().Add(time.Minute)
	for from, to := range map[string]string{next: cert, nextKey: key} {
		b, err := os.ReadFile(from)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(to, b, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(to, later, later); err != nil {
			t.Fatal(err)
		}
	}
	if cn := served(); cn != "second" {
		t.Fatalf("served %q after the certificate changed, want second", cn)
	}
}
