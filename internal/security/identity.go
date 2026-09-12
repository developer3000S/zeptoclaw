// Package security implements node identity, message signing, trust policy,
// rate limiting and the security audit journal.
package security

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

// KeyFileMagic prefixes the on-disk key so an operator can tell the format at
// a glance and so a truncated file is rejected rather than misread.
const KeyFileMagic = "zeptomesh-ed25519-v1\n"

// Identity is the node's cryptographic identity.
type Identity struct {
	priv   ic.PrivKey
	pub    ic.PubKey
	peerID peer.ID
}

// LoadOrGenerate reads the Ed25519 key from path, creating it when absent.
func LoadOrGenerate(path string) (*Identity, error) {
	if path == "" {
		return nil, errors.New("security: empty key path")
	}
	raw, err := os.ReadFile(path)
	switch {
	case err == nil:
		return decodeKey(raw)
	case errors.Is(err, os.ErrNotExist):
	default:
		return nil, fmt.Errorf("security: read key %s: %w", path, err)
	}

	priv, _, err := ic.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("security: generate key: %w", err)
	}
	blob, err := ic.MarshalPrivateKey(priv)
	if err != nil {
		return nil, fmt.Errorf("security: marshal key: %w", err)
	}
	if err := writeKeyFile(path, []byte(KeyFileMagic+hex.EncodeToString(blob))); err != nil {
		return nil, err
	}
	return newIdentity(priv)
}

// NewEphemeral builds an in-memory identity, used by tests.
func NewEphemeral() (*Identity, error) {
	priv, _, err := ic.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("security: generate ephemeral key: %w", err)
	}
	return newIdentity(priv)
}

func decodeKey(raw []byte) (*Identity, error) {
	text := strings.TrimSpace(string(raw))
	if !strings.HasPrefix(text, strings.TrimSpace(KeyFileMagic)) {
		return nil, fmt.Errorf("security: %s has an unknown key format", KeyFileMagic)
	}
	body := strings.TrimSpace(strings.TrimPrefix(text, strings.TrimSpace(KeyFileMagic)))
	blob, err := hex.DecodeString(body)
	if err != nil {
		return nil, fmt.Errorf("security: key is not hex: %w", err)
	}
	priv, err := ic.UnmarshalPrivateKey(blob)
	if err != nil {
		return nil, fmt.Errorf("security: unmarshal key: %w", err)
	}
	if priv.Type() != ic.Ed25519 {
		return nil, fmt.Errorf("security: key type %s is not ed25519", priv.Type())
	}
	return newIdentity(priv)
}

func newIdentity(priv ic.PrivKey) (*Identity, error) {
	pid, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		return nil, fmt.Errorf("security: peer id: %w", err)
	}
	return &Identity{priv: priv, pub: priv.GetPublic(), peerID: pid}, nil
}

// PeerID returns the node identity.
func (i *Identity) PeerID() peer.ID { return i.peerID }

// PrivKey exposes the libp2p private key (used to build the host).
func (i *Identity) PrivKey() ic.PrivKey { return i.priv }

// PubKey exposes the libp2p public key.
func (i *Identity) PubKey() ic.PubKey { return i.pub }

// Sign signs an arbitrary byte slice with the node's Ed25519 key.
func (i *Identity) Sign(msg []byte) ([]byte, error) {
	sig, err := i.priv.Sign(msg)
	if err != nil {
		return nil, fmt.Errorf("security: sign: %w", err)
	}
	return sig, nil
}

// PublicKeyOf resolves a peer's public key, preferring the key embedded in the
// peer ID (self-certifying for Ed25519) and falling back to the lookup.
func PublicKeyOf(pid peer.ID, lookup func(peer.ID) (ic.PubKey, error)) (ic.PubKey, error) {
	if pk, err := pid.ExtractPublicKey(); err == nil && pk != nil {
		return pk, nil
	}
	if lookup == nil {
		return nil, fmt.Errorf("security: no public key for %s", pid)
	}
	pk, err := lookup(pid)
	if err != nil {
		return nil, fmt.Errorf("security: lookup key %s: %w", pid, err)
	}
	if pk == nil {
		return nil, fmt.Errorf("security: no public key for %s", pid)
	}
	return pk, nil
}

// Verify checks a signature against a peer's public key.
func Verify(pid peer.ID, msg, sig []byte, lookup func(peer.ID) (ic.PubKey, error)) error {
	pk, err := PublicKeyOf(pid, lookup)
	if err != nil {
		return err
	}
	ok, err := pk.Verify(msg, sig)
	if err != nil {
		return fmt.Errorf("security: verify: %w", err)
	}
	if !ok {
		return ErrBadSignature
	}
	return nil
}

// ErrBadSignature marks a failed signature check.
var ErrBadSignature = errors.New("security: bad signature")

func writeKeyFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("security: mkdir key dir: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("security: write key: %w", err)
	}
	if err := os.Chmod(tmp, 0o600); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("security: chmod key: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("security: rename key: %w", err)
	}
	return nil
}
