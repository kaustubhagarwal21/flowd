# Build stage: compile a static binary (no cgo), so the runtime image needs
# no C library.
FROM golang:1.27 AS build
WORKDIR /src
# Download modules in their own layer, so code changes do not re-download them.
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/flowd ./cmd/flowd

# Runtime stage: distroless has no shell or package manager, which leaves
# little to attack, and the nonroot variant runs as an unprivileged user.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/flowd /flowd
EXPOSE 8080
USER nonroot
ENTRYPOINT ["/flowd"]
