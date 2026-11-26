// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package ros_test

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds"
	ddssecurity "github.com/podomy/weld/internal/transports/dds/security"
	"github.com/podomy/weld/internal/transports/dds/security/auth"
	"github.com/podomy/weld/internal/transports/dds/security/crypto"
	"github.com/podomy/weld/testbed/live/helpers"
)

// secLivePaths holds generated live fixture paths.
type secLivePaths struct {
	// dir holds all fixture files.
	dir string
	// ca is the permissions CA certificate.
	ca string
	// cert is the Weld identity certificate.
	cert string
	// key is the Weld identity private key.
	key string
	// governance is the signed governance document.
	governance string
	// permissions is the signed permissions document.
	permissions string
	// governanceXML is the unsigned governance XML for content reuse.
	governanceXML string
	// permissionsXML is the unsigned permissions XML for content reuse.
	permissionsXML string
	// cycloneXML is the CycloneDDS secure configuration.
	cycloneXML string
}

// Secure handshake live gate constants.
const (
	// secLiveContainer is the CycloneDDS peer container.
	secLiveContainer = "ros-humble"
	// secLiveDomain is the secured fixtures domain.
	secLiveDomain = 95
	// secLiveTopic is the ROS name of the protected topic.
	secLiveTopic = "/secure_chatter"
	// secLiveType is the IDL name used on the protected topic.
	secLiveType = "std_msgs__msg__String"
	// secLiveCASubject names the fixture permissions CA.
	secLiveCASubject = "CN=sec5-perms-ca"
	// secLiveContainerDir stages fixtures inside the peer container.
	secLiveContainerDir = "/tmp/sec5weld"
	// secLiveRosBin is the peer ros2 binary.
	secLiveRosBin = "/opt/ros/humble/bin/ros2"
)

// TestGateSecurityHandshakeLive proves Weld-to-Cyclone protected DATA.
//
// It generates fresh EC identity material, re-signs the Phase 5
// governance plus permissions XML with a fresh CA, brings up a secure
// CycloneDDS peer on domain 95, and completes the PKI-DH handshake on
// the wire: Weld publishes protected DATA the secure echo receives,
// and the secure talker publishes protected DATA Weld takes. Both
// directions fail closed without session keys, so received plaintext
// proves negotiated crypto. It needs the ros-humble peer plus an
// openssl binary and skips cleanly without them. ROS_DOMAIN_ID stays
// 90 through 101: the proof runs on domain 95 only.
func TestGateSecurityHandshakeLive(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	helpers.SkipIfNoPeer(t, ctx, secLiveContainer)

	if _, err := exec.LookPath("openssl"); err != nil {
		t.Skipf("openssl absent, skipping: %v", err)
	}

	fixtures := mustSecLiveFixtures(t)

	mustSecLiveContainerFixtures(t, ctx, fixtures)

	backend := mustSecLiveBackend(t, ctx, fixtures)

	payload := "weld-secure-" + strconv.FormatInt(time.Now().UnixNano(), 10)

	mustSecLiveWeldToCyclone(t, ctx, backend, payload)
	mustSecLiveCycloneToWeld(t, ctx, backend, payload)
}

// mustSecLiveFixtures generates fresh live fixture files.
func mustSecLiveFixtures(t *testing.T) secLivePaths {
	t.Helper()

	dir := t.TempDir()

	srcDir := filepath.Join("testdata", "secphase5")

	govXML, err := os.ReadFile(filepath.Join(srcDir, "governance.xml")) //nolint:gosec // live gate reads test-controlled fixture content
	if err != nil {
		t.Fatalf("read governance xml: %v", err)
	}

	permsXML, err := os.ReadFile(filepath.Join(srcDir, "permissions.xml")) //nolint:gosec // live gate reads test-controlled fixture content
	if err != nil {
		t.Fatalf("read permissions xml: %v", err)
	}

	out := secLivePaths{
		dir:            dir,
		ca:             filepath.Join(dir, "ca.pem"),
		cert:           filepath.Join(dir, "weld.pem"),
		key:            filepath.Join(dir, "weld.key"),
		governance:     filepath.Join(dir, "governance.smime"),
		permissions:    filepath.Join(dir, "permissions.smime"),
		governanceXML:  filepath.Join(dir, "governance.xml"),
		permissionsXML: filepath.Join(dir, "permissions.xml"),
		cycloneXML:     filepath.Join(dir, "cyclonedds_secure.xml"),
	}

	writeSecFile(t, out.governanceXML, govXML)
	writeSecFile(t, out.permissionsXML, permsXML)

	caKey := filepath.Join(dir, "ca.key")

	secOpenSSL(t, "ecparam", "-name", "prime256v1", "-genkey", "-noout", "-out", caKey)
	secOpenSSL(t, "req", "-new", "-x509", "-key", caKey, "-out", out.ca, "-days", "2", "-subj", "/CN=sec5-perms-ca")

	secOpenSSL(t, "ecparam", "-name", "prime256v1", "-genkey", "-noout", "-out", out.key)
	secOpenSSL(t, "req", "-new", "-key", out.key, "-out", filepath.Join(dir, "weld.csr"), "-subj", "/CN=weld")
	secOpenSSL(t, "x509", "-req", "-in", filepath.Join(dir, "weld.csr"), "-CA", out.ca, "-CAkey", caKey, "-CAcreateserial", "-out", out.cert, "-days", "2", "-sha256")

	cycloneKey := filepath.Join(dir, "cyclone.key")

	secOpenSSL(t, "ecparam", "-name", "prime256v1", "-genkey", "-noout", "-out", cycloneKey)
	secOpenSSL(t, "req", "-new", "-key", cycloneKey, "-out", filepath.Join(dir, "cyclone.csr"), "-subj", "/CN=cyclone_talker")
	secOpenSSL(t, "x509", "-req", "-in", filepath.Join(dir, "cyclone.csr"), "-CA", out.ca, "-CAkey", caKey, "-CAcreateserial", "-out", filepath.Join(dir, "cyclone.pem"), "-days", "2", "-sha256")

	cycloneXML := `<CycloneDDS>
  <Domain>
    <Security>
      <Authentication>
        <IdentityCA>file:/tmp/sec5weld/cyclone/ca.pem</IdentityCA>
        <IdentityCertificate>file:/tmp/sec5weld/cyclone/cyclone.pem</IdentityCertificate>
        <PrivateKey>file:/tmp/sec5weld/cyclone/cyclone.key</PrivateKey>
      </Authentication>
      <AccessControl>
        <PermissionsCA>file:/tmp/sec5weld/cyclone/ca.pem</PermissionsCA>
        <Governance>file:/tmp/sec5weld/cyclone/governance.smime</Governance>
        <Permissions>file:/tmp/sec5weld/cyclone/permissions.smime</Permissions>
      </AccessControl>
    </Security>
  </Domain>
</CycloneDDS>
`
	writeSecFile(t, out.cycloneXML, []byte(cycloneXML))

	secOpenSSL(t, "smime", "-sign", "-text", "-in", out.governanceXML, "-signer", out.ca, "-inkey", caKey, "-outform", "SMIME", "-out", out.governance)
	secOpenSSL(t, "smime", "-sign", "-text", "-in", out.permissionsXML, "-signer", out.ca, "-inkey", caKey, "-outform", "SMIME", "-out", out.permissions)

	return out
}

// writeSecFile writes one fixture file, failing loudly.
func writeSecFile(t *testing.T, path string, data []byte) {
	t.Helper()

	err := os.WriteFile(path, data, 0o600) //nolint:gosec // G703: live harness writes test-controlled fixture path
	if err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// secOpenSSL runs one openssl argv, failing loudly on error output.
func secOpenSSL(t *testing.T, args ...string) {
	t.Helper()

	cmd := exec.CommandContext(context.Background(), "openssl", args...) //nolint:gosec // G204: live gate runs fixed openssl binary with test-controlled args
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("openssl %v: %v output %s", args, err, string(out))
	}
}

// mustSecLiveContainerFixtures copies fixtures into the peer container.
func mustSecLiveContainerFixtures(t *testing.T, ctx context.Context, fixtures secLivePaths) {
	t.Helper()

	cycloneDir := t.TempDir()

	copySecFile(t, fixtures.ca, filepath.Join(cycloneDir, "ca.pem"))
	copySecFile(t, filepath.Join(fixtures.dir, "cyclone.pem"), filepath.Join(cycloneDir, "cyclone.pem"))
	copySecFile(t, filepath.Join(fixtures.dir, "cyclone.key"), filepath.Join(cycloneDir, "cyclone.key"))
	copySecFile(t, fixtures.governance, filepath.Join(cycloneDir, "governance.smime"))
	copySecFile(t, fixtures.permissions, filepath.Join(cycloneDir, "permissions.smime"))
	copySecFile(t, fixtures.cycloneXML, filepath.Join(cycloneDir, "cyclonedds_secure.xml"))

	mkdir := exec.CommandContext(ctx, "docker", "exec", secLiveContainer, "mkdir", "-p", secLiveContainerDir+"/cyclone")
	if out, err := mkdir.CombinedOutput(); err != nil {
		t.Fatalf("docker mkdir: %v output %s", err, string(out))
	}

	cmd := exec.CommandContext(ctx, "docker", "cp", cycloneDir+"/.", secLiveContainer+":"+secLiveContainerDir+"/cyclone/") //nolint:gosec // live harness copies test-controlled fixture dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("docker cp fixtures: %v output %s", err, string(out))
	}
}

// copySecFile copies one file, failing loudly.
func copySecFile(t *testing.T, src, dst string) {
	t.Helper()

	raw, err := os.ReadFile(src) //nolint:gosec // live harness reads test-controlled fixture path
	if err != nil {
		t.Fatalf("read %s: %v", src, err)
	}

	writeSecFile(t, dst, raw)
}

// mustSecLiveBackend opens a Weld backend with security enabled.
func mustSecLiveBackend(t *testing.T, ctx context.Context, fixtures secLivePaths) *dds.Backend {
	t.Helper()

	backend, err := helpers.OpenLiveBackend(ctx, secLiveDomain, "weld-secure", "secure-handshake")
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}

	t.Cleanup(func() { //nolint:contextcheck // backend Close is cleanup without caller ctx
		cerr := backend.Close()
		if cerr != nil {
			t.Logf("backend close: %v", cerr)
		}
	})

	err = backend.Subscribe(ctx, secLiveTopic, secLiveType, dds.StringTypeHash)
	if err != nil {
		t.Fatalf("subscribe %s: %v", secLiveTopic, err)
	}

	err = backend.PublishBytes(ctx, transports.OutgoingMessage{TopicName: secLiveTopic, TypeName: secLiveType, Payload: []byte("secure-declare")})
	if err != nil && !errors.Is(err, dds.ErrNoRoute) {
		t.Fatalf("declare publish %s: %v", secLiveTopic, err)
	}

	suite := mustSecLiveSuite(t, fixtures)

	managerAuth, err := auth.NewAuthenticator(auth.KAgreeP256)
	if err != nil {
		t.Fatalf("new auth: %v", err)
	}

	manager, err := auth.NewManager("weld-secure", managerAuth)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	store := crypto.NewKeyStore()
	backend.SetSecuritySuite(suite, manager, store)

	certDER := mustSecLiveCertDER(t, fixtures.cert)
	permDoc := mustSecLiveFile(t, fixtures.permissions)
	privPEM := mustSecLiveFile(t, fixtures.key)

	if err := backend.InstallSecureIdentity(certDER, permDoc, privPEM, secLiveCASubject); err != nil {
		t.Fatalf("install identity: %v", err)
	}

	return backend
}

// mustSecLiveSuite builds the decision suite from live fixtures.
func mustSecLiveSuite(t *testing.T, fixtures secLivePaths) *ddssecurity.Suite {
	t.Helper()

	cfg := dds.SecurityConfig{
		Enabled:                 true,
		IdentityCertificatePath: fixtures.cert,
		IdentityPrivateKeyPath:  fixtures.key,
		PermissionsCAPath:       fixtures.ca,
		GovernancePath:          fixtures.governance,
		PermissionsDocumentPath: fixtures.permissions,
	}

	suite, err := cfg.BuildSuite()
	if err != nil {
		t.Fatalf("build suite: %v", err)
	}

	return suite
}

// mustSecLiveCertDER parses one PEM certificate to DER.
func mustSecLiveCertDER(t *testing.T, path string) []byte {
	t.Helper()

	raw := mustSecLiveFile(t, path)

	block, _ := pem.Decode(raw)
	if block == nil {
		t.Fatalf("decode pem %s", path)
	}

	if _, err := x509.ParseCertificate(block.Bytes); err != nil {
		t.Fatalf("parse certificate %s: %v", path, err)
	}

	return block.Bytes
}

// mustSecLiveFile reads one fixture file.
func mustSecLiveFile(t *testing.T, path string) []byte {
	t.Helper()

	raw, err := os.ReadFile(path) //nolint:gosec // live harness reads test-controlled fixture path
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}

	return raw
}

// mustSecLiveWeldToCyclone proves protected Weld DATA reaches secure echo.
func mustSecLiveWeldToCyclone(t *testing.T, ctx context.Context, backend *dds.Backend, payload string) {
	t.Helper()

	runID := "sec5live-echo"

	echoArgs := append([]string{"env", "CYCLONEDDS_URI=file://" + secLiveContainerDir + "/cyclone/cyclonedds_secure.xml"}, secLiveRosArgs("topic", "echo", secLiveTopic, "std_msgs/msg/String")...)
	if err := helpers.StartPeerProc(ctx, secLiveContainer, secLiveDomain, runID, echoArgs...); err != nil {
		t.Fatalf("start secure echo: %v", err)
	}

	t.Cleanup(func() {
		helpers.StopPeerProcAndWait(ctx, secLiveContainer, secLiveDomain, helpers.ContainerPidPath(runID), "topic echo "+secLiveTopic)
	})

	outPath := helpers.HostOutPath(t, runID)

	deadline := time.Now().Add(150 * time.Second)

	sent := false

	for time.Now().Before(deadline) {
		err := backend.PublishBytes(ctx, transports.OutgoingMessage{TopicName: secLiveTopic, TypeName: secLiveType, Payload: []byte(payload)})
		if err != nil && !errors.Is(err, dds.ErrNoRoute) {
			t.Fatalf("publish protected: %v", err)
		}

		if err == nil {
			sent = true
		}

		if helpers.WaitForFileContains(ctx, outPath, payload, 3*time.Second) {
			return
		}

		if ctx.Err() != nil {
			t.Fatalf("context ended waiting for echo")
		}
	}

	t.Fatalf("secure echo never saw protected payload, published=%v", sent)
}

// mustSecLiveCycloneToWeld proves protected Cyclone DATA reaches Weld takes.
func mustSecLiveCycloneToWeld(t *testing.T, ctx context.Context, backend *dds.Backend, payload string) {
	t.Helper()

	runID := "sec5live-pub"

	pubArgs := append([]string{"env", "CYCLONEDDS_URI=file://" + secLiveContainerDir + "/cyclone/cyclonedds_secure.xml"}, secLiveRosArgs("topic", "pub", secLiveTopic, "std_msgs/msg/String", "{data: '"+payload+"'}")...)
	if err := helpers.StartPeerProc(ctx, secLiveContainer, secLiveDomain, runID, pubArgs...); err != nil {
		t.Fatalf("start secure talker: %v", err)
	}

	t.Cleanup(func() {
		helpers.StopPeerProcAndWait(ctx, secLiveContainer, secLiveDomain, helpers.ContainerPidPath(runID), "topic pub "+secLiveTopic)
	})

	deadline := time.Now().Add(150 * time.Second)

	for time.Now().Before(deadline) {
		takeCtx, takeCancel := context.WithTimeout(ctx, 5*time.Second)

		msg, err := backend.TakeBytes(takeCtx)
		takeCancel()

		if err != nil {
			if ctx.Err() != nil {
				t.Fatalf("context ended waiting for take")
			}

			continue
		}

		if string(msg.Payload) == payload {
			return
		}
	}

	t.Fatalf("weld never took protected payload")
}

// secLiveRosArgs builds the peer ros2 argv with the humble binary.
func secLiveRosArgs(args ...string) []string {
	return append([]string{secLiveRosBin}, args...)
}
