package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/hex"
	"fmt"
)

var commonIV = []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f}

var salt = []byte("zYwyXq5ngYtK,gq1my0n2Tq3lOfJzj[i")

func initCiper() (cipher.Block, error) {
	c, err := aes.NewCipher(salt)
	if err != nil {
		fmt.Printf("Error: NewCipher(%d bytes) = %s", len(salt), err)
	}
	return c, err
}

// Encrypt src
func Encrypt(src string) (string, error) {
	c, err := initCiper()
	if err != nil {
		return "", err
	}
	cfb := cipher.NewCFBEncrypter(c, commonIV)
	dst := make([]byte, len(src))
	cfb.XORKeyStream(dst, []byte(src))
	return hex.EncodeToString(dst), err
}

// Decrypt src
func Decrypt(s string) (string, error) {
	c, err := initCiper()
	if err != nil {
		return "", err
	}
	src, err := hex.DecodeString(s)
	if err != nil {
		return "", err
	}
	cfb := cipher.NewCFBDecrypter(c, commonIV)
	dst := make([]byte, len(src))
	cfb.XORKeyStream(dst, src)
	return string(dst), err
}
