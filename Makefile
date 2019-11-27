#GOPATH:=$(PWD):${GOPATH}
#xport GOPATH
flags=-ldflags="-s -w"
# flags=-ldflags="-s -w -extldflags -static"
TAG := $(shell git tag | sed -e "s,v,,g" | sort -r | head -n 1)

all: build

build:
	sed -i -e "s,{{VERSION}},$(TAG),g" main.go
	go clean; rm -rf pkg; go build ${flags}
	sed -i -e "s,$(TAG),{{VERSION}},g" main.go

build_debug:
	sed -i -e "s,{{VERSION}},$(TAG),g" main.go
	go clean; rm -rf pkg; go build ${flags} -gcflags="-m -m"
	sed -i -e "s,$(TAG),{{VERSION}},g" main.go

build_all: build_osx build_linux build

build_osx:
	sed -i -e "s,{{VERSION}},$(TAG),g" main.go
	go clean; rm -rf pkg das2go_osx; GOOS=darwin go build ${flags}
	mv das2go das2go_osx
	sed -i -e "s,$(TAG),{{VERSION}},g" main.go

build_linux:
	sed -i -e "s,{{VERSION}},$(TAG),g" main.go
	go clean; rm -rf pkg das2go_linux; GOOS=linux go build ${flags}
	mv das2go das2go_linux
	sed -i -e "s,$(TAG),{{VERSION}},g" main.go

build_power8:
	sed -i -e "s,{{VERSION}},$(TAG),g" main.go
	go clean; rm -rf pkg das2go_power8; GOARCH=ppc64le GOOS=linux go build ${flags}
	sed -i -e "s,$(TAG),{{VERSION}},g" main.go
	mv das2go das2go_power8

build_arm64:
	sed -i -e "s,{{VERSION}},$(TAG),g" main.go
	go clean; rm -rf pkg das2go_arm64; GOARCH=arm64 GOOS=linux go build ${flags}
	sed -i -e "s,$(TAG),{{VERSION}},g" main.go
	mv das2go das2go_arm64

build_windows:
	sed -i -e "s,{{VERSION}},$(TAG),g" main.go
	go clean; rm -rf pkg das2go.exe; GOARCH=amd64 GOOS=windows go build ${flags}
	sed -i -e "s,$(TAG),{{VERSION}},g" main.go

install:
	go install

clean:
	go clean; rm -rf pkg

test : test1

test1:
	cd test; go test
