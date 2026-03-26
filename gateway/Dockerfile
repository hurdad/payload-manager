FROM golang:1.22-bookworm AS build
WORKDIR /src
COPY gateway/go.mod gateway/go.sum* ./gateway/
WORKDIR /src/gateway
RUN go mod download
WORKDIR /src
COPY gateway ./gateway
RUN cd gateway && go build -o /out/gateway .

FROM gcr.io/distroless/base-debian12
COPY --from=build /out/gateway /gateway
EXPOSE 8080
ENTRYPOINT ["/gateway"]
