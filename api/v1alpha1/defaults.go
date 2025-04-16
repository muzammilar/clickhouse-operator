package v1alpha1

const (
	DefaultKeeperContainerRepository = "docker.io/clickhouse/clickhouse-keeper"
	DefaultKeeperContainerTag        = "latest"
	DefaultKeeperContainerPolicy     = "IfNotPresent"

	DefaultKeeperCPULimit      = "1"
	DefaultKeeperCPURequest    = "250m"
	DefaultKeeperMemoryLimit   = "1Gi"
	DefaultKeeperMemoryRequest = "256Mi"
)
