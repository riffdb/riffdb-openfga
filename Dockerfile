# syntax=docker/dockerfile:1
FROM golang:1.25.8-alpine AS builder

RUN apk add --no-cache ca-certificates curl git make

WORKDIR /src
COPY go.mod go.sum Makefile ./
RUN make bootstrap

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/openfga-riffdb ./cmd/openfga-riffdb

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/openfga-riffdb /openfga-riffdb
COPY --from=builder /out/openfga-riffdb /openfga

EXPOSE 8080 8081 2112 3000 3001

ENTRYPOINT ["/openfga-riffdb"]
CMD ["run"]
