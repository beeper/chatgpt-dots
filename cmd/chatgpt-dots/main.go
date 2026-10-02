package main

import (
	"syscall"

	"github.com/beeper/chatgpt-dots/pkg/connector"
	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix/bridgev2/matrix/mxmain"
)

var tag, commit, buildTime string

func main() {
	syscall.Umask(0077)
	dbutil.GlobalSafeQueryLog = true
	m := mxmain.BridgeMain{Name: "chatgpt-dots", Description: "Text, image, file and video messaging with your existing ChatGPT Dot", Version: "0.1.0", Connector: &connector.Connector{}}
	m.InitVersion(tag, commit, buildTime)
	m.Run()
}
