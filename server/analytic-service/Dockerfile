FROM golang:alpine AS build_container
WORKDIR /app
COPY go.mod .
COPY go.sum .
RUN go mod download
COPY . .
RUN go build -o server

FROM alpine:3.18

RUN apk add --no-cache ca-certificates

COPY --from=build_container /app/server /usr/bin
COPY --from=build_container /app/cert.crt /app/cert.crt
COPY --from=build_container /app/privat.key /app/privat.key

RUN cp /app/cert.crt /usr/local/share/ca-certificates/cert.crt && update-ca-certificates

EXPOSE 8082

ENTRYPOINT ["server"]
