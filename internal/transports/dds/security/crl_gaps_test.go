// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package security

import (
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports/dds/security/access"
)

// TestSecGapsCRLReloadacc9 pins CRL rotation flipping authentication.
func TestSecGapsCRLReloadacc9(t *testing.T) {
	t.Parallel()

	ca, caKey, caPEM := auditCA(t, "gaps-crl-reload-ca")
	_, _, localPEM := auditLeaf(t, ca, caKey, "local", 230)
	_, _, peerPEM := auditLeaf(t, ca, caKey, "peer", 231)

	dir := t.TempDir()
	caPath := writeGapsFile(t, dir, "ca.pem", caPEM)
	certPath := writeGapsFile(t, dir, "cert.pem", localPEM)
	keyPath := writeGapsFile(t, dir, "key.pem", localPEM)

	govSigned, err := access.SignEnvelope(auditGovernance(), ca, caKey)
	if err != nil {
		t.Fatalf("sign gov: %v", err)
	}

	govPath := writeGapsFile(t, dir, "governance.xml", govSigned)

	before, after := auditValidity()
	allowPerms := []byte(`<permissions><grant name="peer"><subject_name>CN=peer</subject_name>` +
		`<validity><not_before>` + before + `</not_before><not_after>` + after + `</not_after></validity>` +
		`<allow_rule><domains><id>0</id></domains><topics><topic>rt/chatter</topic></topics></allow_rule>` +
		`</grant></permissions>`)

	permsSigned, err := access.SignEnvelope(allowPerms, ca, caKey)
	if err != nil {
		t.Fatalf("sign perms: %v", err)
	}

	permsPath := writeGapsFile(t, dir, "permissions.xml", permsSigned)

	emptyCRL, err := x509.CreateRevocationList(rand.Reader, &x509.RevocationList{
		Number:     big.NewInt(1),
		ThisUpdate: time.Now().Add(-time.Hour),
		NextUpdate: time.Now().Add(24 * time.Hour),
	}, ca, caKey)
	if err != nil {
		t.Fatalf("create CRL: %v", err)
	}

	crlPath := writeGapsFile(t, dir, "crl.pem", pem.EncodeToMemory(&pem.Block{Type: "X509 CRL", Bytes: emptyCRL}))

	suite, err := NewSuite(SuiteConfig{
		Enabled:                 true,
		IdentityCertificatePath: certPath,
		IdentityPrivateKeyPath:  keyPath,
		PermissionsCAPath:       caPath,
		GovernancePath:          govPath,
		PermissionsDocumentPath: permsPath,
		CRLPath:                 crlPath,
	})
	if err != nil {
		t.Fatalf("new suite: %v", err)
	}

	if err := suite.AuthenticatePeer("audit-peer", peerPEM); err != nil {
		t.Fatalf("clean CRL must authenticate: %v", err)
	}

	revokedDER, err := x509.CreateRevocationList(rand.Reader, &x509.RevocationList{
		Number:     big.NewInt(2),
		ThisUpdate: time.Now().Add(-time.Hour),
		NextUpdate: time.Now().Add(24 * time.Hour),
		RevokedCertificateEntries: []x509.RevocationListEntry{
			{SerialNumber: big.NewInt(231), RevocationTime: time.Now()},
		},
	}, ca, caKey)
	if err != nil {
		t.Fatalf("create revoked CRL: %v", err)
	}

	future := time.Now().Add(2 * time.Second)
	if err := os.Chtimes(crlPath, future, future); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	if err := os.WriteFile(crlPath, pem.EncodeToMemory(&pem.Block{Type: "X509 CRL", Bytes: revokedDER}), 0o600); err != nil {
		t.Fatalf("rotate CRL: %v", err)
	}

	if err := os.Chtimes(crlPath, future.Add(time.Second), future.Add(time.Second)); err != nil {
		t.Fatalf("chtimes2: %v", err)
	}

	// New identity under a different label must now refuse via the
	// reloaded CRL; the existing binding stays pinned, so use a fresh
	// label to exercise the revocation path.
	err = suite.AuthenticatePeer("audit-peer-2", peerPEM)
	if !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("rotated CRL must refuse revoked peer, got %v", err)
	}
}
