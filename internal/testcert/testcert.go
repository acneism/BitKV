package testcert

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
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

type CA struct {
	File string
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	dir  string
	next int64
}

func New(t testing.TB, name string) *CA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	check(t, err)
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
	check(t, err)
	cert, err := x509.ParseCertificate(der)
	check(t, err)
	dir := t.TempDir()
	ca := &CA{File: filepath.Join(dir, "ca.crt"), cert: cert, key: key, dir: dir, next: 2}
	writePEM(t, ca.File, "CERTIFICATE", der)
	return ca
}

func (ca *CA) Issue(t testing.TB, cn string, dns []string, usage []x509.ExtKeyUsage) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	check(t, err)
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
	check(t, err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	check(t, err)
	name := strings.Join(append([]string{cn}, dns...), "-")
	certFile, keyFile = filepath.Join(ca.dir, name+".crt"), filepath.Join(ca.dir, name+".key")
	writePEM(t, certFile, "CERTIFICATE", der)
	writePEM(t, keyFile, "EC PRIVATE KEY", keyDER)
	return certFile, keyFile
}

func writePEM(t testing.TB, path, kind string, der []byte) {
	t.Helper()
	check(t, os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: der}), 0o600))
}

func check(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
