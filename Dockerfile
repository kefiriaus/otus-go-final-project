FROM golang:1.27.3-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /out/stats-daemon ./cmd/stats-daemon

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends tcpdump libcap2-bin \
    && setcap cap_net_raw=eip /usr/bin/tcpdump \
    && rm -rf /var/lib/apt/lists/*
RUN groupadd --system --gid 10001 stats \
    && useradd --system --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin stats
COPY --from=build /out/stats-daemon /usr/local/bin/stats-daemon
USER 10001:10001
EXPOSE 50051
ENTRYPOINT ["/usr/local/bin/stats-daemon"]
