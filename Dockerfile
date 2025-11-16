##
# Builder image
FROM golang:1.24-bookworm AS build

WORKDIR /usr/src/app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
ENV CGO_ENABLED=0
RUN go test ./...
RUN go install -v ./cmd/...


##
# Deployment image
FROM gcr.io/distroless/static:nonroot

COPY --from=build /go/bin/doc /usr/local/bin/doc
COPY --from=build /go/bin/gitter /usr/local/bin/gitter
