FROM golang:alpine AS build_container
WORKDIR /app
COPY go.mod .
COPY go.sum .
RUN go mod download
COPY . .
RUN go build -o server

FROM alpine:3.18
COPY --from=build_container /app/server /usr/bin
COPY --from=build_container /app/10k-worst-passwords.txt /app/10k-worst-passwords.txt
COPY --from=build_container /app/cert.crt /app/cert.crt
COPY --from=build_container /app/privat.key /app/privat.key

EXPOSE 8090
ENTRYPOINT ["/usr/bin/server"]