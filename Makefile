GOPATH:=$(PWD):${GOPATH}
export GOPATH

all: build

build:
	go clean; rm -rf pkg; go build

install:
	go install

clean:
	go clean; rm -rf pkg

test:
	cd test; go test

