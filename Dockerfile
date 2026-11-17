FROM golang:1.26.10-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=secret,id=build_ca,target=/tmp/build-ca.pem,required=false \
    if [ -f /tmp/build-ca.pem ]; then export SSL_CERT_FILE=/tmp/build-ca.pem; fi; \
    go mod download
COPY cmd/ cmd/
COPY internal/ internal/
RUN CGO_ENABLED=0 go build -trimpath -buildvcs=false -o /out/recovery ./cmd/recovery

FROM alpine:3.23
RUN addgroup -g 10001 recovery && adduser -D -u 10001 -G recovery recovery \
    && mkdir /data && chown recovery:recovery /data
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/recovery /usr/local/bin/recovery
USER 10001:10001
WORKDIR /data
ENTRYPOINT ["/usr/local/bin/recovery"]
CMD ["demo"]
