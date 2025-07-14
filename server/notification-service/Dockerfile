FROM golang:alpine AS build_container
RUN apk update && apk add --no-cache tzdata
ENV TZ=Europe/Budapest

WORKDIR /app
COPY go.mod .
COPY go.sum .
RUN go mod download
COPY . .
RUN go build -o server

FROM alpine:3.18

COPY --from=build_container /app/server /usr/bin
COPY --from=build_container /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=build_container /app/cert.crt /app/cert.crt
COPY --from=build_container /app/privat.key /app/privat.key

ENV TZ=Europe/Budapest

EXPOSE 8085
ENTRYPOINT ["server"]
