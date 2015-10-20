GOPATH:=$(PWD):${GOPATH}
export GOPATH

all: build

build:
	go clean; rm -rf pkg; go build

install:
	go install

clean:
	go clean; rm -rf pkg

test : test1

test1:
	cd test; go test
