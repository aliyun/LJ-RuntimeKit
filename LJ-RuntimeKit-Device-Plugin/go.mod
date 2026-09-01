module github.com/lj-runtimekit/device-plugin

go 1.21

require (
	github.com/NVIDIA/go-nvml v0.13.3-1
	github.com/fsnotify/fsnotify v1.4.9
	github.com/urfave/cli v1.22.17
	google.golang.org/grpc v1.32.0
	k8s.io/client-go v0.21.2
	k8s.io/code-generator v0.0.0-00010101000000-000000000000
	k8s.io/component-base v0.21.2
	k8s.io/kubelet v0.0.0
)

require (
	github.com/PuerkitoBio/purell v1.1.1 // indirect
	github.com/PuerkitoBio/urlesc v0.0.0-20170810143723-de5bf2ad4578 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.7 // indirect
	github.com/emicklei/go-restful v2.9.5+incompatible // indirect
	github.com/go-openapi/jsonpointer v0.19.3 // indirect
	github.com/go-openapi/jsonreference v0.19.3 // indirect
	github.com/go-openapi/spec v0.19.5 // indirect
	github.com/go-openapi/swag v0.19.5 // indirect
	github.com/mailru/easyjson v0.7.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	golang.org/x/mod v0.3.1-0.20200828183125-ce943fd02449 // indirect
	golang.org/x/tools v0.1.0 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	k8s.io/gengo v0.0.0-20201214224949-b6c5ce23f027 // indirect
	k8s.io/kube-openapi v0.0.0-20210305001622-591a79e4bda7 // indirect
)

require (
	github.com/go-logr/logr v0.4.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/go-cmp v0.5.8 // indirect
	github.com/json-iterator/go v1.1.10 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.1 // indirect
	github.com/stretchr/testify v1.11.1
	golang.org/x/net v0.0.0-20220722155237-a158d28d115b // indirect
	golang.org/x/sys v0.0.0-20220722155257-8c9f86f7a55f // indirect
	golang.org/x/text v0.3.7 // indirect
	google.golang.org/genproto v0.0.0-20201110150050-8816d57aaa9a // indirect
	google.golang.org/protobuf v1.28.0
	gopkg.in/yaml.v2 v2.4.0 // indirect
	k8s.io/klog/v2 v2.8.0
	sigs.k8s.io/yaml v1.2.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/google/gofuzz v1.1.0 // indirect
	github.com/googleapis/gnostic v0.4.1 // indirect
	github.com/imdario/mergo v0.3.5 // indirect
	golang.org/x/oauth2 v0.0.0-20200107190931-bf48bf16ab8d // indirect
	golang.org/x/term v0.0.0-20210927222741-03fcf44c2211 // indirect
	golang.org/x/time v0.0.0-20210220033141-f8bda1e9f3ba // indirect
	google.golang.org/appengine v1.6.5 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	k8s.io/api v0.21.2
	k8s.io/apimachinery v0.21.2
	k8s.io/utils v0.0.0-20201110183641-67b214c5f920 // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.1.0 // indirect
)

replace k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.21.2

replace k8s.io/apimachinery => k8s.io/apimachinery v0.21.3-rc.0

replace k8s.io/apiserver => k8s.io/apiserver v0.21.2

replace k8s.io/cli-runtime => k8s.io/cli-runtime v0.21.2

replace k8s.io/cloud-provider => k8s.io/cloud-provider v0.21.2

replace k8s.io/cluster-bootstrap => k8s.io/cluster-bootstrap v0.21.2

replace k8s.io/code-generator => k8s.io/code-generator v0.21.3-rc.0

replace k8s.io/component-base => k8s.io/component-base v0.21.2

replace k8s.io/component-helpers => k8s.io/component-helpers v0.21.2

replace k8s.io/controller-manager => k8s.io/controller-manager v0.21.2

replace k8s.io/cri-api => k8s.io/cri-api v0.21.5-rc.0

replace k8s.io/csi-translation-lib => k8s.io/csi-translation-lib v0.21.2

replace k8s.io/kube-aggregator => k8s.io/kube-aggregator v0.21.2

replace k8s.io/kube-controller-manager => k8s.io/kube-controller-manager v0.21.2

replace k8s.io/kube-proxy => k8s.io/kube-proxy v0.21.2

replace k8s.io/kube-scheduler => k8s.io/kube-scheduler v0.21.2

replace k8s.io/kubectl => k8s.io/kubectl v0.21.2

replace k8s.io/kubelet => k8s.io/kubelet v0.21.2

replace k8s.io/legacy-cloud-providers => k8s.io/legacy-cloud-providers v0.21.2

replace k8s.io/metrics => k8s.io/metrics v0.21.2

replace k8s.io/mount-utils => k8s.io/mount-utils v0.21.5-rc.0

replace k8s.io/sample-apiserver => k8s.io/sample-apiserver v0.21.2

replace k8s.io/sample-cli-plugin => k8s.io/sample-cli-plugin v0.21.2

replace k8s.io/sample-controller => k8s.io/sample-controller v0.21.2
