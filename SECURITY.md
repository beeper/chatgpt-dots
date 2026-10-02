# Security

## Credentials

ChatGPT credentials grant broader account access, not Dots-only authorization.
The connector limits requests to authentication, identity, Dot discovery,
verified Dot rooms and metadata for tasks attached to those rooms. It does not
fetch unrelated conversations or task transcripts.

Tokens are stored in the bridge database without separate connector-level
encryption. Restrict access to runtime files and backups. Disconnect retains
credentials; bridge logout deletes them locally but does not revoke the
ChatGPT browser session.

## Deployment and diagnostics

Keep credentials, databases, private messages and signed URLs out of source
control and published artifacts. Protect provisioning with encrypted transport.
Diagnostic logs can contain account or message metadata; SQL-argument
redaction does not make all logs safe to share. Avoid HTTP payload tracing.

Media downloads use allowlisted origins, reject redirects and send no ChatGPT
credentials to storage hosts.

## Reporting a vulnerability

Contact the repository maintainers privately. Include reproduction steps, but
omit credentials, private messages and unredacted captures.
