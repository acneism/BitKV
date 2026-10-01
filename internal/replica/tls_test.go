package replica

import (
	"crypto/tls"
	"crypto/x509"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/acneism/casketdb/internal/testcert"
)

var bothWays = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}

func nodeTLS(t *testing.T, ca *testcert.CA, id string) *tls.Config {
	t.Helper()
	cert, key := ca.Issue(t, id, []string{id}, bothWays)
	cfg, err := TLSConfig(id, cert, key, ca.File)
	must(t, err)
	return cfg
}

func TestMutualTLSCluster(t *testing.T) {
	ca := testcert.New(t, "casketdb test ca")
	nodes := newCluster(t, 3, false, func(tn *testNode) { tn.tls = nodeTLS(t, ca, tn.id) })
	l := leader(t, nodes)
	for range 20 {
		must(t, incr(l, "counter"))
	}
	eventually(t, "replication over mTLS", converged(nodes, "counter", "20", 1))
}

func TestMutualTLSRejectsStrangers(t *testing.T) {
	ca := testcert.New(t, "casketdb test ca")
	other := testcert.New(t, "someone else's ca")
	for name, setup := range map[string]func(*testNode){
		"foreign CA":  func(tn *testNode) { tn.tls = nodeTLS(t, other, tn.id) },
		"without TLS": func(tn *testNode) { tn.tls = nil },
	} {
		t.Run(name, func(t *testing.T) {
			nodes := newCluster(t, 3, false, func(tn *testNode) {
				tn.tls = nodeTLS(t, ca, tn.id)
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
	ca := testcert.New(t, "casketdb test ca")
	good, goodKey := ca.Issue(t, "n0", []string{"n0"}, bothWays)
	if _, err := TLSConfig("n0", good, goodKey, ca.File); err != nil {
		t.Fatal(err)
	}
	cnOnly, cnOnlyKey := ca.Issue(t, "n1", nil, bothWays)
	serverOnly, serverOnlyKey := ca.Issue(t, "n2", []string{"n2"}, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	notPEM := filepath.Join(t.TempDir(), "empty.crt")
	must(t, os.WriteFile(notPEM, []byte("not a certificate"), 0o600))
	for name, tc := range map[string]struct {
		id, cert, key, ca, want string
	}{
		"another node's certificate": {"n1", good, goodKey, ca.File, `identifies node "n0"`},
		"no DNS name":                {"n1", cnOnly, cnOnlyKey, ca.File, `needs the DNS name "n1"`},
		"server-only usage":          {"n2", serverOnly, serverOnlyKey, ca.File, "both server and client"},
		"missing key":                {"n0", good, good + ".missing", ca.File, "raft TLS certificate"},
		"CA without certificates":    {"n0", good, goodKey, notPEM, "holds no PEM certificates"},
	} {
		if _, err := TLSConfig(tc.id, tc.cert, tc.key, tc.ca); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s: %v, want an error containing %q", name, err, tc.want)
		}
	}
}
