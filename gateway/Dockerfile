FROM node:22-alpine AS ui-builder
WORKDIR /src/ui
COPY ui/package.json ui/package-lock.json ./
RUN npm ci
COPY ui ./
RUN npm run build


FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY gateway/go.mod gateway/go.sum* ./gateway/
WORKDIR /src/gateway
RUN go mod download
WORKDIR /src
COPY gateway ./gateway
COPY --from=ui-builder /src/ui/dist ./gateway/static
RUN cd gateway && go build -o /out/gateway .


FROM gcr.io/distroless/base-debian12
COPY --from=build /out/gateway /gateway
EXPOSE 8080
ENTRYPOINT ["/gateway"]
