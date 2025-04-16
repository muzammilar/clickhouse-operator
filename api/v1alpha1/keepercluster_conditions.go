package v1alpha1

type KeeperConditionType string

const (
	// KeeperConditionTypeReconcileSucceeded indicates that latest reconciliation was successful.
	KeeperConditionTypeReconcileSucceeded KeeperConditionType = "TypeReconcileSucceeded"
	// KeeperConditionTypeReplicaStartupFailure indicates that certain replicas of the KeeperCluster can not be started.
	KeeperConditionTypeReplicaStartupFailure KeeperConditionType = "ReplicaStartupFailure"
	// KeeperConditionTypeDegraded indicates that certain replicas of the KeeperCluster are not ready to accept connections.
	KeeperConditionTypeDegraded KeeperConditionType = "Degraded"
	// KeeperConditionTypeClusterSizeAligned indicates that KeeperCluster replica amount matches the requested value.
	KeeperConditionTypeClusterSizeAligned KeeperConditionType = "ClusterSizeAligned"
	// KeeperConditionTypeConfigurationInSync indicates that KeeperCluster configuration is in desired state.
	KeeperConditionTypeConfigurationInSync KeeperConditionType = "ConfigurationInSync"
	// KeeperConditionTypeReady indicates that KeeperCluster is ready to serve client requests.
	KeeperConditionTypeReady KeeperConditionType = "Ready"
)

type KeeperConditionReason string

const (
	KeeperConditionReasonStepFailed        KeeperConditionReason = "ReconcileStepFailed"
	KeeperConditionReasonReconcileFinished KeeperConditionReason = "ReconcileFinished"

	KeeperConditionReasonReplicasRunning KeeperConditionReason = "ReplicasRunning"
	KeeperConditionReasonReplicaError    KeeperConditionReason = "ReplicaError"

	KeeperConditionReasonReplicasReady    KeeperConditionReason = "ReplicasReady"
	KeeperConditionReasonReplicasNotReady KeeperConditionReason = "ReplicasNotReady"

	KeeperConditionReasonUpToDate             KeeperConditionReason = "UpToDate"
	KeeperConditionReasonScalingDown          KeeperConditionReason = "ScalingDown"
	KeeperConditionReasonScalingUp            KeeperConditionReason = "ScalingUp"
	KeeperConditionReasonConfigurationChanged KeeperConditionReason = "ConfigurationChanged"

	KeeperConditionReasonStandaloneReady    KeeperConditionReason = "StandaloneReady"
	KeeperConditionReasonClusterReady       KeeperConditionReason = "ClusterReady"
	KeeperConditionReasonNoLeader           KeeperConditionReason = "NoLeader"
	KeeperConditionReasonInconsistentState  KeeperConditionReason = "InconsistentState"
	KeeperConditionReasonNotEnoughFollowers KeeperConditionReason = "NotEnoughFollowers"
)
