module github.com/igoricda/faas-continuum

go 1.22.2

replace (
	cloud.google.com/go => github.com/googleapis/google-cloud-go v0.110.10
	go.opencensus.io => github.com/census-instrumentation/opencensus-go v0.24.0
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp => github.com/open-telemetry/opentelemetry-go-contrib/instrumentation/net/http/otelhttp v0.45.0
	go.opentelemetry.io/otel => github.com/open-telemetry/opentelemetry-go v1.21.0
	go.opentelemetry.io/otel/metric => github.com/open-telemetry/opentelemetry-go/metric v1.21.0
	go.opentelemetry.io/otel/trace => github.com/open-telemetry/opentelemetry-go/trace v1.21.0
	golang.org/x/crypto => github.com/golang/crypto v0.21.0
	golang.org/x/exp => github.com/golang/exp v0.0.0-20240103183307-be819d1f06fc
	golang.org/x/mod => github.com/golang/mod v0.14.0
	golang.org/x/net => github.com/golang/net v0.23.0
	golang.org/x/oauth2 => github.com/golang/oauth2 v0.16.0
	golang.org/x/sync => github.com/golang/sync v0.5.0
	golang.org/x/sys => github.com/golang/sys v0.18.0
	golang.org/x/term => github.com/golang/term v0.18.0
	golang.org/x/text => github.com/golang/text v0.14.0
	golang.org/x/time => github.com/golang/time v0.5.0
	golang.org/x/tools => github.com/golang/tools v0.1.5
	golang.org/x/xerrors => github.com/golang/xerrors v0.0.0-20191204190536-9bdfabe68543
	google.golang.org/appengine => github.com/golang/appengine v1.6.7
	google.golang.org/genproto => github.com/googleapis/go-genproto v0.0.0-20231211222908-989df2bf70f3
	google.golang.org/genproto/googleapis/api => github.com/googleapis/go-genproto/googleapis/api v0.0.0-20231120223509-83a465c0220f
	google.golang.org/genproto/googleapis/rpc => github.com/googleapis/go-genproto/googleapis/rpc v0.0.0-20231212172506-995d672761c0
	google.golang.org/grpc => github.com/grpc/grpc-go v1.59.0
	google.golang.org/protobuf => github.com/protocolbuffers/protobuf-go v1.33.0
	gopkg.in/check.v1 => github.com/go-check/check v0.0.0-20201130134442-10cb98267c6c
	gopkg.in/yaml.v2 => github.com/go-yaml/yaml v2.4.0+incompatible
	gopkg.in/yaml.v3 => github.com/go-yaml/yaml v3.0.1+incompatible
)

require (
	github.com/containerd/cgroups/v3 v3.0.2
	github.com/containerd/containerd v1.7.23
	github.com/containerd/containerd/api v1.7.19
	github.com/containerd/typeurl/v2 v2.1.1
	github.com/distribution/reference v0.6.0
	github.com/opencontainers/runtime-spec v1.1.0
	golang.org/x/crypto v0.21.0
	golang.org/x/sys v0.18.0
	google.golang.org/protobuf v1.33.0
)

require (
	github.com/AdaLogics/go-fuzz-headers v0.0.0-20230811130428-ced1acdcaa24 // indirect
	github.com/AdamKorcz/go-118-fuzz-build v0.0.0-20230306123547-8075edf89bb0 // indirect
	github.com/Microsoft/go-winio v0.6.2 // indirect
	github.com/Microsoft/hcsshim v0.11.7 // indirect
	github.com/containerd/cgroups v1.1.0 // indirect
	github.com/containerd/continuity v0.4.2 // indirect
	github.com/containerd/errdefs v0.3.0 // indirect
	github.com/containerd/fifo v1.1.0 // indirect
	github.com/containerd/log v0.1.0 // indirect
	github.com/containerd/platforms v0.2.1 // indirect
	github.com/containerd/ttrpc v1.2.5 // indirect
	github.com/docker/go-events v0.0.0-20190806004212-e31b211e4f1c // indirect
	github.com/felixge/httpsnoop v1.0.3 // indirect
	github.com/go-logr/logr v1.3.0 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/google/uuid v1.4.0 // indirect
	github.com/klauspost/compress v1.16.7 // indirect
	github.com/moby/locker v1.0.1 // indirect
	github.com/moby/sys/mountinfo v0.6.2 // indirect
	github.com/moby/sys/sequential v0.5.0 // indirect
	github.com/moby/sys/signal v0.7.0 // indirect
	github.com/moby/sys/user v0.3.0 // indirect
	github.com/moby/sys/userns v0.1.0 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/image-spec v1.1.0 // indirect
	github.com/opencontainers/selinux v1.11.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.45.0 // indirect
	go.opentelemetry.io/otel v1.21.0 // indirect
	go.opentelemetry.io/otel/metric v1.21.0 // indirect
	go.opentelemetry.io/otel/trace v1.21.0 // indirect
	golang.org/x/net v0.23.0 // indirect
	golang.org/x/sync v0.5.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	google.golang.org/genproto v0.0.0-20231211222908-989df2bf70f3 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20231212172506-995d672761c0 // indirect
	google.golang.org/grpc v1.59.0 // indirect
)
