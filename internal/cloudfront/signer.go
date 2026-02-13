package cloudfront

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"os"
	"strings"
	"time"
)

// SignedCookies contains the three CloudFront signed cookies
type SignedCookies struct {
	Policy    string
	Signature string
	KeyPairID string
}

// Signer handles CloudFront signed cookie generation
type Signer struct {
	privateKey *rsa.PrivateKey
	keyPairID  string
}

// Policy represents a CloudFront signed URL policy
type Policy struct {
	Statement []Statement `json:"Statement"`
}

// Statement represents a policy statement
type Statement struct {
	Resource  string    `json:"Resource"`
	Condition Condition `json:"Condition"`
}

// Condition represents policy conditions
type Condition struct {
	DateLessThan DateCondition `json:"DateLessThan"`
}

// DateCondition represents a date condition
type DateCondition struct {
	AWSEpochTime int64 `json:"AWS:EpochTime"`
}

// NewSigner creates a new CloudFront signer from a PEM private key file
func NewSigner(privateKeyPath, keyPairID string) (*Signer, error) {
	keyData, err := os.ReadFile(privateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read private key: %w", err)
	}

	privateKey, err := parsePrivateKey(keyData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key: %w", err)
	}

	return &Signer{
		privateKey: privateKey,
		keyPairID:  keyPairID,
	}, nil
}

// parsePrivateKey parses an RSA private key from PEM data
func parsePrivateKey(pemData []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(pemData)
	if block == nil {
		return nil, fmt.Errorf("failed to decode PEM block")
	}

	// Try PKCS#1 format first
	if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return key, nil
	}

	// Try PKCS#8 format
	keyInterface, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key: %w", err)
	}

	rsaKey, ok := keyInterface.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("key is not an RSA private key")
	}

	return rsaKey, nil
}

// GenerateSignedCookies generates CloudFront signed cookies for the given resource pattern
func (s *Signer) GenerateSignedCookies(resourcePattern string, expiry time.Time) (*SignedCookies, error) {
	// Create policy
	policy := Policy{
		Statement: []Statement{
			{
				Resource: resourcePattern,
				Condition: Condition{
					DateLessThan: DateCondition{
						AWSEpochTime: expiry.Unix(),
					},
				},
			},
		},
	}

	// Marshal policy to JSON
	policyJSON, err := json.Marshal(policy)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal policy: %w", err)
	}

	// Base64 encode policy with CloudFront-safe encoding
	encodedPolicy := base64Encode(policyJSON)

	// Sign the policy
	signature, err := s.signPolicy(policyJSON)
	if err != nil {
		return nil, fmt.Errorf("failed to sign policy: %w", err)
	}

	return &SignedCookies{
		Policy:    encodedPolicy,
		Signature: signature,
		KeyPairID: s.keyPairID,
	}, nil
}

// signPolicy creates an RSA-SHA1 signature of the policy
func (s *Signer) signPolicy(policy []byte) (string, error) {
	hash := sha1.Sum(policy)

	signature, err := rsa.SignPKCS1v15(rand.Reader, s.privateKey, crypto.SHA1, hash[:])
	if err != nil {
		return "", fmt.Errorf("failed to sign: %w", err)
	}

	return base64Encode(signature), nil
}

// base64Encode encodes data using CloudFront-safe base64
// CloudFront uses a modified base64 encoding:
// + -> -
// = -> _
// / -> ~
func base64Encode(data []byte) string {
	encoded := base64.StdEncoding.EncodeToString(data)
	encoded = strings.ReplaceAll(encoded, "+", "-")
	encoded = strings.ReplaceAll(encoded, "=", "_")
	encoded = strings.ReplaceAll(encoded, "/", "~")
	return encoded
}
