package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type InternalStruct struct {
	String string
	Number int
}

type SampleStruct struct {
	String  string
	Number  int
	Pointer *int
	Array   []int
	Map     map[int]string
	Struct  InternalStruct
}

func TestApplyDefault(t *testing.T) {
	varForPointer := 42
	allSet := SampleStruct{
		String:  "default",
		Number:  7,
		Pointer: &varForPointer,
		Array:   []int{1, 2, 3},
		Map:     map[int]string{1: "a", 2: "b", 3: "c"},
		Struct: InternalStruct{
			String: "internal",
			Number: 14,
		},
	}

	t.Run("all default values", func(t *testing.T) {
		source := SampleStruct{}
		require.NoError(t, ApplyDefault(&source, allSet))
		require.Equal(t, source, allSet)
	})

	t.Run("nothing to update", func(t *testing.T) {
		source := allSet
		defaults := SampleStruct{
			String: "another",
			Map:    map[int]string{1: "diff"},
			Struct: InternalStruct{
				Number: 77,
			},
		}
		require.NoError(t, ApplyDefault(&source, defaults))
		require.Equal(t, source, allSet)
	})

	t.Run("update several", func(t *testing.T) {
		source := allSet
		defaults := allSet

		source.String = ""
		source.Struct.Number = 100
		source.Map[7] = "custom"

		require.NoError(t, ApplyDefault(&source, defaults))
		require.Equal(t, source.String, allSet.String)
		require.Equal(t, source.Struct.Number, 100)
		require.Equal(t, source.Map[7], "custom")
	})
}
