package v1alpha1

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
)

type ContainerImage struct {
	// Container image registry name
	// Example: docker.io/clickhouse/clickhouse
	// +optional
	Repository string `json:"repository,omitempty"`

	// Container image tag
	// Example: 25.3
	// +optional
	Tag string `json:"tag,omitempty"`

	// PullPolicy for the image, which defaults to IfNotPresent.
	// +optional
	// +kubebuilder:validation:Enum="Always";"Never";"IfNotPresent"
	PullPolicy corev1.PullPolicy `json:"pullPolicy,omitempty"`
}

func (c *ContainerImage) String() string {
	return fmt.Sprintf("%s:%s", c.Repository, c.Tag)
}

type PodPolicy struct {
	// NodeSelector specifies a map of key-value pairs. For the pod to be
	// eligible to run on a node, the node must have each of the indicated
	// key-value pairs as labels.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Resources is the resource requirements for the container.
	// This field cannot be updated once the cluster is created.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
}

type LoggerConfig struct {
	// If false then disable all logging to file.
	// +optional
	// +kubebuilder:default:=true
	LogToFile bool `json:"logToFile,omitempty"`

	// If true, then log in JSON format.
	// +optional
	// +kubebuilder:default:=false
	JSONLogs bool `json:"jsonLogs,omitempty"`

	// +optional
	// +kubebuilder:validation:Enum:=test;trace;debug;information;error;warning
	// +kubebuilder:default:=trace
	LoggerLevel string `json:"loggerLevel,omitempty"`
}
