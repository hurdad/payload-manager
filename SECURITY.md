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

### There is no authentication, authorization, or transport security

The gRPC server binds with `InsecureServerCredentials()`
(`internal/runtime/server.cpp`). There is no TLS, no caller identity, no
per-RPC authorization, and no rate limiting. Anyone who can reach the port can
allocate, read, spill, and delete any payload, and can create and delete
streams.

The HTTP gateway inherits all of that, since it is a translating proxy in front
of the same API. CORS is **off** unless `-cors-origins` (or `CORS_ORIGINS`)
names the origins that need it; turning it on widens reachability from "anything
that can route to the port" to "any page loaded in a browser that can route to
the port", so set it to specific origins and not `*`.

Consequently: bind to loopback or an internal address, and put a
TLS-terminating, authenticating proxy in front of any deployment that is not
confined to a single trusted node.

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
