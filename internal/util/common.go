package util

import (
	"crypto/md5" //nolint:gosec
	"encoding/hex"
	"fmt"
	"maps"
	"reflect"
	"runtime"
	"strings"

	"github.com/davecgh/go-spew/spew"
	appsv1 "k8s.io/api/apps/v1"
)

// DeepHashObject writes specified object to hash using the spew library
// which follows pointers and prints actual values of the nested objects
// ensuring the hash does not change when a pointer changes.
// (copied from Kubernetes, with changes).
func DeepHashObject(objectToWrite interface{}) (string, error) {
	//nolint:gosec // Used just for hashing an object, don't care about security
	hasher := md5.New()
	printer := spew.ConfigState{
		Indent:         " ",
		SortKeys:       true,
		DisableMethods: true,
		SpewKeys:       true,
	}
	if _, err := printer.Fprintf(hasher, "%#v", objectToWrite); err != nil {
		return "", err
	}

	return hex.EncodeToString(hasher.Sum(nil)[0:]), nil
}

func MergeMaps(mapsToMerge ...map[string]string) map[string]string {
	result := map[string]string{}
	for _, m := range mapsToMerge {
		maps.Copy(result, m)
	}

	return result
}

func ReverseMap[M ~map[K]V, K comparable, V comparable](m M) map[V]K {
	r := make(map[V]K, len(m))
	for k, v := range m {
		r[v] = k
	}
	return r
}

func IsStatefulSetInSync(found *appsv1.StatefulSet, expectedReplicas *int32) bool {
	if found.Status.ObservedGeneration == 0 || found.Generation != found.Status.ObservedGeneration {
		return false
	}
	if found.Spec.Replicas == nil || expectedReplicas == nil {
		return false
	}
	return *expectedReplicas == *found.Spec.Replicas &&
		*expectedReplicas == found.Status.Replicas &&
		*expectedReplicas == found.Status.ReadyReplicas &&
		*expectedReplicas == found.Status.CurrentReplicas &&
		*expectedReplicas == found.Status.UpdatedReplicas &&
		*expectedReplicas == found.Status.AvailableReplicas
}

func GetFunctionName(temp interface{}) string {
	strs := strings.Split(runtime.FuncForPC(reflect.ValueOf(temp).Pointer()).Name(), ".")
	return strings.TrimSuffix(strs[len(strs)-1], "-fm")
}

func ApplyDefault[T any](source *T, defaults T) error {
	sourceValue := reflect.ValueOf(source).Elem()
	defaultValue := reflect.ValueOf(defaults)
	return applyDefaultRecursive(sourceValue, defaultValue)
}

func applyDefaultRecursive(sourceValue reflect.Value, defaults reflect.Value) error {
	if sourceValue.Kind() == reflect.Struct {
		for i := range sourceValue.NumField() {
			if err := applyDefaultRecursive(sourceValue.Field(i), defaults.Field(i)); err != nil {
				return fmt.Errorf("apply default value for field %s: %w", sourceValue.Type().Field(i).Name, err)
			}
		}

		return nil
	}

	if sourceValue.Kind() == reflect.Map {
		if sourceValue.IsNil() {
			sourceValue.Set(defaults)
			return nil
		}

		for _, key := range defaults.MapKeys() {
			if sourceValue.MapIndex(key).Kind() == reflect.Invalid {
				sourceValue.SetMapIndex(key, defaults.MapIndex(key))
			}
		}

		return nil
	}

	if sourceValue.Kind() == reflect.Ptr {
		if !sourceValue.IsNil() && !defaults.IsNil() {
			return applyDefaultRecursive(sourceValue.Elem(), defaults.Elem())
		}
	}

	if sourceValue.IsZero() && !defaults.IsZero() {
		sourceValue.Set(defaults)
	}

	return nil
}
