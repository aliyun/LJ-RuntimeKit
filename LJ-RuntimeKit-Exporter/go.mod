module github.com/lj-runtimekit/exporter

// Minimum Go version is dictated by dependencies that declare `go 1.21`
// (github.com/NVIDIA/go-dcgm and google.golang.org/protobuf). Keep the builder
// image in Makefile (GOLANG_VERSION) at or above this version.
go 1.25.0

replace (
	k8s.io/api => k8s.io/api v0.20.2
	k8s.io/apimachinery => k8s.io/apimachinery v0.20.2
	k8s.io/client-go => k8s.io/client-go v0.20.2
	k8s.io/kubelet => k8s.io/kubelet v0.20.2
)

require (
	github.com/NVIDIA/go-dcgm v0.0.0-20240315184911-5e46e1bcb05a
	github.com/gorilla/mux v1.8.1
	github.com/sirupsen/logrus v1.8.1
	github.com/urfave/cli/v2 v2.3.0
	google.golang.org/grpc v1.83.1
	google.golang.org/protobuf v1.36.11
	k8s.io/api v0.20.2
	k8s.io/apimachinery v0.27.3
	k8s.io/client-go v0.20.2
	k8s.io/kubelet v0.20.2
	sigs.k8s.io/yaml v1.2.0
)

require (
	github.com/Masterminds/semver v1.5.0 // indirect
	github.com/bits-and-blooms/bitset v1.13.0 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/google/gofuzz v1.1.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/rogpeppe/go-internal v1.10.0 // indirect
	github.com/russross/blackfriday/v2 v2.0.1 // indirect
	github.com/shurcooL/sanitized_anchor_name v1.0.0 // indirect
	github.com/stretchr/testify v1.10.0 // indirect
	golang.org/x/crypto v0.51.0 // indirect
	golang.org/x/net v0.55.0 // indirect
	golang.org/x/oauth2 v0.36.0 // indirect
	golang.org/x/sys v0.45.0 // indirect
	golang.org/x/term v0.43.0 // indirect
	golang.org/x/text v0.37.0 // indirect
	golang.org/x/time v0.0.0-20211116232009-f0f3c7e86c11 // indirect
	google.golang.org/genproto v0.0.0-20210917145530-b395a37504d4 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	k8s.io/klog/v2 v2.90.1 // indirect
	k8s.io/utils v0.0.0-20230209194617-a36077c30491 // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.0.2 // indirect
)
