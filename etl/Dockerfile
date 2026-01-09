# Use the official Golang base image with Alpine
FROM golang:1.25-alpine

# Set the working directory inside the container
WORKDIR /workspace

# Copy the current directory (context) to /workspace in the container
# This will include all your Go source files
COPY . .

RUN cd cmd/etl && go build -o /usr/local/bin/etl

# Run the tests with verbose output, coverage, and race detection
ENTRYPOINT ["etl"]

