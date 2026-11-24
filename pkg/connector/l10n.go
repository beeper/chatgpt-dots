package connector

import (
	"go.mau.fi/util/l10n"
)

var L = l10n.NewNamespace("chatgpt-dots")

var (
	msgFlowCookiesName          = L.Define("login.flow.cookies.name", "ChatGPT Dots")
	msgFlowCookiesDescription   = L.Define("login.flow.cookies.description", "Sign in with ChatGPT to message your existing Dot. Regular ChatGPT conversations are not imported.")
	msgStepCookiesInstructions  = L.Define("login.step.cookies.instructions", "Connect ChatGPT Dots: sign in with ChatGPT to message your existing Dot. Regular ChatGPT conversations are not imported. ChatGPT credentials grant broader account access, not a provider-enforced Dots-only scope. This bridge uses them only for authentication, Dot discovery, verified Dot room messaging and status reads for tasks attached to those rooms. Credentials are stored by the bridge runtime. Logout removes this bridge's copy, not your ChatGPT browser session. Provider session renewal is automatic while the session remains valid; reconnect if ChatGPT revokes it.")
	msgStepCompleteInstructions = L.Define("login.step.complete.instructions", "Connected the existing primary Dot. Only messages after linking are bridged.")
)
