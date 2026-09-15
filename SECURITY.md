# Security

## Reporting a vulnerability

Report suspected vulnerabilities through GitHub's private advisory form:
[**Report a vulnerability**](https://github.com/hurdad/payload-manager/security/advisories/new).
That channel is private until an advisory is published. Please do not open a
public issue for anything you believe is exploitable.

Include the version or commit, the configuration involved (particularly which
storage tiers and which database backend), and the smallest reproduction you
have. You should get an acknowledgement within a week.

## What this project assumes about its environment

Payload Manager's threat model is **a trusted host and a trusted network**.
That is a real design position, not an oversight, and it comes directly from
what the system is: a control plane whose entire purpose is to let unrelated
processes address the *same physical bytes* without copying them. Read this
section before exposing a deployment to anything wider than the node it runs on.

### Transport security and authentication are off by default

Both exist and both are opt-in. A configuration that sets neither binds with
`InsecureServerCredentials()` and accepts every caller — the historic
behaviour, and still what you get from a config written before these existed.
Anyone who can reach that port can allocate, read, spill and delete any
payload, and create and delete streams.

To turn them on:

```yaml
server:
  bind_address: "0.0.0.0:50051"
  tls:
    cert_file: /etc/payload-manager/tls/server.pem
    key_file:  /etc/payload-manager/tls/server-key.pem
    # client_ca_file: /etc/payload-manager/tls/ca.pem   # also require client certs
  auth:
    jwt_hs256_key_file: /etc/payload-manager/auth/hs256.key
    # or jwt_public_key_file, for tokens from an external issuer
    issuer: "your-issuer"
    audience: "payload-manager"
```

Both blocks take an explicit `enabled` for templated configuration: an absent
block is off, a present block without `enabled` is on, and an explicit value
wins. A present-but-disabled block warns at startup, because that is the state
you reach by switching it off to debug something and forgetting.

`scripts/gen-dev-certs.sh` generates a CA, a server certificate, a signing key
and a token for trying this locally. They are development credentials — the CA
key sits unencrypted beside the certificate it signed — and are not for
deployment.

Clients configure the matching side through the environment, identically in
C++ and Python: `PAYLOAD_MANAGER_TLS_CA` (its presence enables TLS),
`PAYLOAD_MANAGER_TOKEN` or `PAYLOAD_MANAGER_TOKEN_FILE`,
`PAYLOAD_MANAGER_TLS_CERT`/`_KEY` for mutual TLS, and
`PAYLOAD_MANAGER_TLS_SERVER_NAME` when the address dialled is not a name the
certificate carries. Two combinations are refused rather than connected: a
token without TLS, which would put the credential on the wire in cleartext,
and a client certificate without a CA, which authenticates this end while
leaving the peer unverified.

**What authentication does and does not give you.** A valid token grants every
RPC — there is no per-caller or per-route authorization, so every authenticated
caller can do everything. Tokens are stateless bearer credentials with no
revocation list, so a leaked token is valid until its `exp`; keep expiries
short. `exp` is required, and a token without one is rejected rather than
treated as valid forever.

The HTTP gateway serves HTTPS when `-tls-cert`/`-tls-key` are set and forwards
the caller's token to payload-manager; it never substitutes one of its own, so
it cannot act with more authority than the browser behind it. CORS is **off**
unless `-cors-origins` (or `CORS_ORIGINS`) names the origins that need it;
turning it on widens reachability from "anything that can route to the port" to
"any page loaded in a browser that can route to the port", so name specific
origins rather than `*`.

Even with both enabled, a TLS-terminating authenticating proxy in front is
reasonable for anything internet-facing — it adds rate limiting and request
logging, neither of which this service has.

### Prefer a Unix socket where every client is local

`bind_address` accepts a gRPC target, so `unix:///run/payload-manager/pm.sock`
works, and `extra_bind_addresses` lets one process serve a socket and a TCP
port at once. This is worth doing whenever the clients are node-local, and for
the RAM and ring tiers they always are — their data plane is `/dev/shm`, so a
client on another host cannot use those tiers at all. In that deployment a TCP
port is reachable surface that buys nothing, and the socket's file mode becomes
the access control.

Note the URI form: `unix:///absolute/path` with three slashes, or
`unix:relative/path`. `unix://host/path` is an authority form the scheme
rejects.

### Shared memory is readable and writable by any local user

The RAM tier and the ring tier create POSIX shared-memory segments with mode
`0666` (`internal/storage/ram/ram_arrow_store.cpp`,
`internal/ring/ring.cpp`). This is what makes the zero-copy data plane work —
producers and consumers map the segments directly, and payload bytes never pass
through the service — but it means:

- Any local user, and any container sharing the host's `/dev/shm`, can **read**
  every payload resident in the RAM or ring tiers.
- The same parties can **write** to those segments. Nothing detects it. A ring
  slot's generation counter guards against a *stale consumer*, not against a
  hostile writer.

Treat `/dev/shm` as being inside the trust boundary. Do not co-locate untrusted
workloads with `payload-manager` on the same host or in containers sharing its
IPC namespace, and size the deployment so that the set of local users is the set
of users allowed to see the data.

### Credentials live in the runtime config

`database.postgres.connection_uri` and `filesystem_options.s3.*` hold secrets in
plaintext. `chmod 600` the config file, inject it from a secrets manager where
possible, and see `docs/ARCHITECTURE.md` §6 for the longer discussion.

### The published images

Images at `ghcr.io/hurdad/payload-manager*` are built by
`.github/workflows/ci.yml` from this repository and publish only after the test
jobs pass. They pin package versions but track base-image tags
(`ubuntu:26.04`, `nvidia/cuda:…`) rather than digests, so a rebuild picks up
upstream base updates. Pin by digest yourself if you need a reproducible
supply chain.

## Supported versions

This project is pre-1.0 and has no release branches. Fixes land on `main`.
