package dialer

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/dolphindb/api-go/v3/dialer/protocol"
	"github.com/dolphindb/api-go/v3/model"
	"golang.org/x/crypto/pbkdf2"
)

const (
	IGNORE ErrorType = iota
	UNKNOWN
	NEW_LEADER
	NODE_NOT_AVAIL
	NO_INITIALIZED
	UNEXPECT
)

type ErrorType int

func generateScriptCommand(cmdStr string) []byte {
	bs := bytes.Buffer{}
	bs.WriteString(scriptCmd)
	bs.WriteByte(protocol.NewLine)
	bs.WriteString(cmdStr)
	return bs.Bytes()
}

func generateFunctionCommand(cmdStr string, bo byte, args []model.DataForm) []byte {
	bs := bytes.Buffer{}
	bs.WriteString(functionCmd)
	bs.WriteByte(protocol.NewLine)
	bs.WriteString(cmdStr)
	bs.WriteByte(protocol.NewLine)
	bs.WriteString(strconv.Itoa(len(args)))
	bs.WriteByte(protocol.NewLine)
	bs.WriteByte(bo)
	bs.WriteByte(protocol.NewLine)
	return bs.Bytes()
}

func generateConnectionCommand() []byte {
	bs := bytes.Buffer{}
	bs.WriteString(connectCmd)
	bs.WriteByte(protocol.NewLine)
	return bs.Bytes()
}

func generateVariableCommand(names string, bo byte, count int) []byte {
	bs := bytes.Buffer{}
	bs.WriteString(variableCmd)
	bs.WriteByte(protocol.NewLine)
	bs.WriteString(names)
	bs.WriteByte(protocol.NewLine)
	bs.WriteString(strconv.Itoa(count))
	bs.WriteByte(protocol.NewLine)
	bs.WriteByte(bo)
	return bs.Bytes()
}

func generatorRequestFlag(opt *BehaviorOptions) int {
	flag := 0
	if opt.IsClearSessionMemory {
		flag += 16
	}

	if opt.UsePython {
		flag += 2048
	}

	if opt.IsReverseStreaming {
		flag += 131072
	}
	return flag
}

func readFile(path string) (string, error) {
	var err error
	if !filepath.IsAbs(path) {
		path, err = filepath.Abs(path)
		if err != nil {
			return "", err
		}
	}

	fl, err := os.Open(path)
	if err != nil {
		return "", err
	}

	defer fl.Close()

	byt, err := ioutil.ReadAll(fl)
	if err != nil {
		return "", err
	}

	return string(byt), err
}

func parseAddr(raw string) string {
	strs := strings.Split(raw, ":")
	if len(strs) < 2 {
		return ""
	}

	return strings.Join(strs[:2], ":")
}

func Login(conn Conn, userID, password string) error {
	if conn.enableScram() {
		return scramLogin(conn, userID, password)
	} else {
		err := scramLogin(conn, userID, password)
		if err == nil {
			return nil
		}
	}
	args := make([]model.DataForm, 2)
	user, err := model.NewDataType(model.DtString, userID)
	if err != nil {
		return err
	}
	pwd, err := model.NewDataType(model.DtString, password)
	if err != nil {
		return err
	}

	args[0] = model.NewScalar(user)
	args[1] = model.NewScalar(pwd)
	_, err = conn.RunFunc("login", args)
	if err != nil {
		return err
	}
	return nil
}

func generateNonce(length int) (string, error) {
	buffer := make([]byte, length)
	_, err := rand.Read(buffer)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(buffer), nil
}

func xorBytes(a, b []byte) []byte {
	result := make([]byte, len(a))
	for i := range a {
		result[i] = a[i] ^ b[i]
	}
	return result
}

func scramLogin(conn Conn, userID, password string) error {
	args := make([]model.DataForm, 2)
	user, err := model.NewDataType(model.DtString, userID)
	if err != nil {
		return fmt.Errorf("SCRAM login failed, %w", err)
	}
	clientNonce, err := generateNonce(16)
	if err != nil {
		return fmt.Errorf("SCRAM login failed, %w", err)
	}
	nonce, err := model.NewDataType(model.DtString, clientNonce)
	if err != nil {
		return fmt.Errorf("SCRAM login failed, %w", err)
	}
	args[0] = model.NewScalar(user)
	args[1] = model.NewScalar(nonce)

	result, err := conn.RunFunc("scramClientFirst", args)
	if err != nil {
		if strings.Contains(err.Error(), "Can't recognize function name scramClientFirst") {
			return fmt.Errorf("SCRAM login is unavailable on current server")
		}
		if strings.Contains(err.Error(), "sha256 authMode doesn't support scram authMode") {
			return fmt.Errorf("user '%s' doesn't support scram authMode", userID)
		}
		return fmt.Errorf("scramClientFirst failed: %w", err)
	}

	retVec := result.(*model.Vector)

	if retVec.Rows() != 3 {
		return fmt.Errorf("SCRAM login failed, server error: get server nonce failed")
	}
	saltStr := retVec.Get(0).Value().(*model.Scalar).Value().(string)
	iterCount := int(retVec.Get(1).Value().(*model.Scalar).Value().(int32))
	combinedNonce := retVec.Get(2).Value().(*model.Scalar).Value().(string)

	salt, err := base64.StdEncoding.DecodeString(saltStr)
	if err != nil {
		return fmt.Errorf("SCRAM login failed, base64 decode failed: %w", err)
	}

	saltedPassword := pbkdf2.Key([]byte(password), salt, iterCount, 32, sha256.New)

	mac := hmac.New(sha256.New, saltedPassword)
	_, err = mac.Write([]byte("Client Key"))
	if err != nil {
		return fmt.Errorf("SCRAM login failed, HMAC calculation failed: %w", err)
	}
	clientKey := mac.Sum(nil)

	storedKey := sha256.Sum256(clientKey)

	authMessage := fmt.Sprintf(`n=%s,r=%s,r=%s,s=%s,i=%d,c=biws,r=%s`,
		userID, clientNonce, combinedNonce, saltStr, iterCount, combinedNonce)

	mac = hmac.New(sha256.New, storedKey[:])
	_, err = mac.Write([]byte(authMessage))
	if err != nil {
		return fmt.Errorf("SCRAM login failed, HMAC calculation failed: %w", err)
	}
	clientSig := mac.Sum(nil)

	proof := xorBytes(clientKey, clientSig)

	finalArgs := make([]model.DataForm, 3)
	combinedNonceScalar, err := model.NewDataType(model.DtString, combinedNonce)
	if err != nil {
		return fmt.Errorf("SCRAM login failed, %w", err)
	}
	proofScalar, err := model.NewDataType(model.DtString, base64.StdEncoding.EncodeToString(proof))
	if err != nil {
		return fmt.Errorf("SCRAM login failed, %w", err)
	}

	finalArgs[0] = model.NewScalar(user)
	finalArgs[1] = model.NewScalar(combinedNonceScalar)
	finalArgs[2] = model.NewScalar(proofScalar)

	finalResult, err := conn.RunFunc("scramClientFinal", finalArgs)
	if err != nil {
		return fmt.Errorf("scramClientFinal failed: %w", err)
	}
	serverSigBase64 := finalResult.(*model.Scalar).Value().(string)

	mac = hmac.New(sha256.New, saltedPassword)
	_, err = mac.Write([]byte("Server Key"))
	if err != nil {
		return fmt.Errorf("SCRAM login failed, HMAC calculation failed: %w", err)
	}
	serverKey := mac.Sum(nil)

	mac = hmac.New(sha256.New, serverKey)
	_, err = mac.Write([]byte(authMessage))
	if err != nil {
		return fmt.Errorf("SCRAM login failed, HMAC calculation failed: %w", err)
	}
	serverSig := mac.Sum(nil)

	expectedSig := base64.StdEncoding.EncodeToString(serverSig)

	if serverSigBase64 != "" && expectedSig != serverSigBase64 {
		conn.Close()
		return fmt.Errorf("invalid SCRAM server signature")
	}

	fmt.Println("SCRAM login succeeded")
	return nil
}
