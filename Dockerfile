FROM golang:1.20-alpine as build

# Set necessary environment variables needed for our image
ENV GO111MODULE=on \
    CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=${TARGETARCH}

# Move to working directory /build
WORKDIR /build

# Copy and download dependency using go mod
COPY go.mod .
COPY go.sum .
RUN go mod download

# Copy the code into the container
ADD . .

# Build the application
RUN go build -o main .

FROM build as dist

# Move to /dist directory as the place for resulting binary folder
WORKDIR /dist

# Copy binary from build to main folder
RUN cp /build/main .
RUN rm -r /build

FROM alpine as final
COPY --from=dist /dist/main /dist/main

RUN apk add --no-cache tzdata

EXPOSE 3003
ENTRYPOINT ["/dist/main"]
