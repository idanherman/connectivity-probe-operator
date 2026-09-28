FROM golang:1.24 AS builder
WORKDIR /build
COPY agent/ ./agent/
COPY go.mod go.sum ./
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o /mesh-agent ./agent/cmd/

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /mesh-agent /mesh-agent
USER 65532:65532
ENTRYPOINT ["/mesh-agent"]
