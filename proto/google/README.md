# Vendored third-party protos

`google/api/annotations.proto` and `google/api/http.proto` are vendored from
[googleapis/googleapis](https://github.com/googleapis/googleapis) at commit
`208f19890d8e0a4a5bc772584246c973ff57f6c1`.

They are the only two files the service protos need (for `google.api.http`
method annotations, consumed by grpc-gateway).  They are vendored rather than
carried as a git submodule because the upstream repository is very large and
these two files are small, stable, and Apache-2.0 licensed — the same licence
as this project.  Each file retains its original copyright header.

To refresh:

    curl -sL -o google/api/annotations.proto \
      https://raw.githubusercontent.com/googleapis/googleapis/<commit>/google/api/annotations.proto
    curl -sL -o google/api/http.proto \
      https://raw.githubusercontent.com/googleapis/googleapis/<commit>/google/api/http.proto
