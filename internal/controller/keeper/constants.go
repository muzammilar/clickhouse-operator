package keeper

import (
	"time"

	"github.com/blang/semver/v4"
)

const (
	RequeueOnRefreshTimeout = time.Second * 1
	RequeueOnErrorTimeout   = time.Second * 5
	StatusRequestTimeout    = time.Second * 10

	PortNative           = 2181
	PortNativeSecure     = 2281
	PortPrometheusScrape = 9090
	PortInterserver      = 9234

	QuorumConfigPath       = "/etc/clickhouse-keeper/"
	QuorumConfigFileName   = "config.yaml"
	QuorumConfigVolumeName = "clickhouse-keeper-quorum-config-volume"

	ConfigPath       = QuorumConfigPath + "config.d/"
	ConfigFileName   = "00-config.yaml"
	ConfigVolumeName = "clickhouse-keeper-config-volume"

	BasePath            = "/var/lib/clickhouse-keeper/"
	StoragePath         = BasePath + "storage/"
	StorageLogPath      = BasePath + "coordination/log/"
	StorageSnapshotPath = BasePath + "coordination/snapshots/"

	ContainerName          = "clickhouse-keeper"
	DefaultRevisionHistory = 10
)

var (
	BreakingStatefulSetVersion, _ = semver.Parse("0.0.1")
)
