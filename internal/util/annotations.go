package util

import (
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	AnnotationSpecHash    = "checksum/spec"
	AnnotationConfigHash  = "checksum/configuration"
	AnnotationRestartedAt = "kubectl.kubernetes.io/restartedAt"

	AnnotationServerVersion      = "clickhouse.com/server-version"
	AnnotationStatefulSetVersion = "clickhouse.com/statefulset-version"
)

func AddHashWithKeyToAnnotations(obj client.Object, key string, specHash string) {
	annotations := obj.GetAnnotations()
	if annotations == nil {
		obj.SetAnnotations(map[string]string{
			key: specHash,
		})
	} else {
		annotations[key] = specHash
	}
}

func AddSpecHashToAnnotations(obj client.Object, spec interface{}) error {
	specHash, err := DeepHashObject(spec)
	if err != nil {
		return fmt.Errorf("deep hash object spec: %w", err)
	}

	AddHashWithKeyToAnnotations(obj, AnnotationSpecHash, specHash)
	return nil
}

func GetSpecHashFromObject(found client.Object) string {
	annotations := found.GetAnnotations()
	if annotations == nil || len(annotations[AnnotationSpecHash]) == 0 {
		return ""
	}
	return annotations[AnnotationSpecHash]
}

func IsEqualSpecHash(found client.Object, spec interface{}) bool {
	specHash := GetSpecHashFromObject(found)
	if specHash == "" {
		// Return false when the hash is not found or 'empty'
		return false
	}

	hash, err := DeepHashObject(spec)
	if err != nil {
		return false
	}
	return hash == specHash
}

func GetConfigHashFromObject(found client.Object) string {
	annotations := found.GetAnnotations()
	if annotations == nil || len(annotations[AnnotationConfigHash]) == 0 {
		return ""
	}
	return annotations[AnnotationConfigHash]
}

func AddObjectConfigHash(obj client.Object, hash string) {
	AddHashWithKeyToAnnotations(obj, AnnotationConfigHash, hash)
}
