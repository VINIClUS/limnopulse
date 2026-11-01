package workerproc

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

// Credential names, matching the LoadCredential= names in ops/workers/roles.yaml.
const (
	CredentialAWSAccessKeyID     = "aws-access-key-id"
	CredentialAWSSecretAccessKey = "aws-secret-access-key"
	CredentialInfluxDBToken      = "influxdb-token"
	CredentialRedisPassword      = "redis-password"
)

var credentialName = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)

// Credentials reads the secrets systemd places in CREDENTIALS_DIRECTORY. Without that
// variable (local runs) nothing is available and callers keep their usual sources.
type Credentials struct {
	dir string
}

// CredentialsFromEnv refuses to fall back to the default chain under systemd: a unit
// without CREDENTIALS_DIRECTORY lost its LoadCredential= lines and is misconfigured.
func CredentialsFromEnv(lookup LookupEnv) (Credentials, error) {
	get := lookupOrOS(lookup)
	dir, _ := get("CREDENTIALS_DIRECTORY")
	if dir == "" {
		if supervised(get) {
			return Credentials{}, Configuration(errors.New("CREDENTIALS_DIRECTORY is not set under systemd"))
		}
		return Credentials{}, nil
	}
	if !filepath.IsAbs(dir) {
		return Credentials{}, Configuration(errors.New("CREDENTIALS_DIRECTORY must be absolute"))
	}
	return Credentials{dir: dir}, nil
}

// SupervisorEnv is set to "systemd" by every worker unit (go_unit_environment in
// ops/workers/roles.yaml). INVOCATION_ID is no marker: desktop terminals and CI runners
// inherit it from their own units.
const SupervisorEnv = "LIMNOPULSE_SUPERVISOR"

func supervised(get LookupEnv) bool {
	value, _ := get(SupervisorEnv)
	return value == "systemd"
}

func (creds Credentials) Available() bool {
	return creds.dir != ""
}

// Lookup returns the named credential. It reports false without a credentials directory.
// Inside one, a missing or empty credential is a configuration error.
func (creds Credentials) Lookup(name string) (string, bool, error) {
	if !credentialName.MatchString(name) {
		return "", false, Configuration(fmt.Errorf("invalid credential name %q", name))
	}
	if !creds.Available() {
		return "", false, nil
	}
	raw, err := os.ReadFile(filepath.Join(creds.dir, name))
	if err != nil {
		return "", false, Configuration(fmt.Errorf("credential %s is not readable", name))
	}
	value := strings.TrimSpace(string(raw))
	if value == "" {
		return "", false, Configuration(fmt.Errorf("credential %s is empty", name))
	}
	return value, true, nil
}

// AWSProvider returns static keys from the aws-access-key-id and aws-secret-access-key
// credentials, or false when no credentials directory exists. Both keys are required.
func (creds Credentials) AWSProvider() (aws.CredentialsProvider, bool, error) {
	if !creds.Available() {
		return nil, false, nil
	}
	keyID, _, err := creds.Lookup(CredentialAWSAccessKeyID)
	if err != nil {
		return nil, false, err
	}
	secret, _, err := creds.Lookup(CredentialAWSSecretAccessKey)
	if err != nil {
		return nil, false, err
	}
	return credentials.NewStaticCredentialsProvider(keyID, secret, ""), true, nil
}

// AWSLoadOptions adds the LoadCredential= keys to config.LoadDefaultConfig when systemd
// supplied them; otherwise it adds nothing and the default chain applies.
func (creds Credentials) AWSLoadOptions() ([]func(*awsconfig.LoadOptions) error, error) {
	provider, ok, err := creds.AWSProvider()
	if err != nil || !ok {
		return nil, err
	}
	return []func(*awsconfig.LoadOptions) error{awsconfig.WithCredentialsProvider(provider)}, nil
}
