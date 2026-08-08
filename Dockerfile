##
# Builder image
FROM golang:1.26-bookworm AS build

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

WORKDIR /app

COPY --from=build /go/bin/doc /usr/local/bin/doc
COPY --from=build /go/bin/gitter /usr/local/bin/gitter
COPY ./template ./template
COPY ./static ./static
