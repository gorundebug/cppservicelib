// Test-only identities produced by official NATS JWT/nkeys libraries. Never
// imported by the C++ SDK; writes ephemeral credentials, not stdout.
package main

import (
	"bytes"
	"fmt"
	jwt "github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
	"os"
	"path/filepath"
	"time"
)

func must[T any](value T, err error) T {
	if err != nil {
		panic("test identity generation failed")
	}
	return value
}
func main() {
	root := os.Args[1]
	write := func(name string, data []byte) {
		if os.WriteFile(filepath.Join(root, name), data, 0600) != nil {
			panic("write test identity failed")
		}
	}
	operator := must(nkeys.CreateOperator())
	account := must(nkeys.CreateAccount())
	system := must(nkeys.CreateAccount())
	systemClaim := jwt.NewAccountClaims(must(system.PublicKey()))
	systemJWT := must(systemClaim.Encode(operator))
	operatorClaim := jwt.NewOperatorClaims(must(operator.PublicKey()))
	operatorClaim.SystemAccount = must(system.PublicKey())
	operatorJWT := must(operatorClaim.Encode(operator))
	claim := jwt.NewAccountClaims(must(account.PublicKey()))
	claim.Limits.MemoryStorage = jwt.NoLimit
	claim.Limits.DiskStorage = jwt.NoLimit
	claim.Limits.Streams = jwt.NoLimit
	claim.Limits.Consumer = jwt.NoLimit
	accountJWT := must(claim.Encode(operator))
	write("operator.jwt", []byte(operatorJWT))
	write("jwt.conf", []byte(fmt.Sprintf("operator: \"/fixture/operator.jwt\"\nresolver: MEMORY\nresolver_preload: { %s: %q, %s: %q }\n", must(account.PublicKey()), accountJWT, must(system.PublicKey()), systemJWT)))
	for _, name := range []string{"valid", "untrusted-issuer", "expired", "wrong-seed"} {
		user := must(nkeys.CreateUser())
		signer := account
		if name == "untrusted-issuer" {
			signer = must(nkeys.CreateAccount())
		}
		userClaim := jwt.NewUserClaims(must(user.PublicKey()))
		if name == "expired" {
			userClaim.Expires = time.Now().Add(-time.Minute).Unix()
		}
		userJWT := must(userClaim.Encode(signer))
		seed := must(user.Seed())
		credentials := must(jwt.FormatUserConfig(userJWT, seed))
		if name == "wrong-seed" {
			// Deliberately corrupt one otherwise valid credential fixture.
			credentials = bytes.Replace(credentials, seed, must(must(nkeys.CreateUser()).Seed()), 1)
		}
		write(name+".creds", credentials)
	}
}
