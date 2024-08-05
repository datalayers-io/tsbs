package datalayers

import (
	"testing"

	"github.com/timescale/tsbs/pkg/data/serialize"
)

// Tests that the Datalayers' serializer works as expected.
// Warning: this test is out of date.
func TestDatalayersSerializerSerialize(t *testing.T) {
	cases := []serialize.SerializeCase{
		{
			Desc:       "a Point with no tags and no fields",
			InputPoint: serialize.TestPointEmpty(),
			Output:     "",
		},
		{
			Desc:       "a regular Point",
			InputPoint: serialize.TestPointDefault(),
			Output:     "1451606400000000000 host_0 eu-west-1 eu-west-1b 38.24311829\n",
		},
		{
			Desc:       "a regular Point using int as value",
			InputPoint: serialize.TestPointInt(),
			Output:     "1451606400000000000 host_0 eu-west-1 eu-west-1b 38\n",
		},
		{
			Desc:       "a regular Point with multiple fields",
			InputPoint: serialize.TestPointMultiField(),
			Output:     "1451606400000000000 host_0 eu-west-1 eu-west-1b 5000000000 38 38.24311829\n",
		},
		{
			Desc:       "a Point with no tags",
			InputPoint: serialize.TestPointNoTags(),
			Output:     "1451606400000000000 38.24311829\n",
		},
		{
			Desc:       "a Point with no fields",
			InputPoint: serialize.TestPointNoFields(),
			Output:     "1451606400000000000 host_0\n",
		},
		{
			Desc:       "a Point with a nil tag",
			InputPoint: serialize.TestPointWithNilTag(),
			Output:     "1451606400000000000 nil 38.24311829\n",
		},
		{
			Desc:       "a Point with a nil field",
			InputPoint: serialize.TestPointWithNilField(),
			Output:     "1451606400000000000 nil 38.24311829\n",
		},
		{
			Desc:       "a Point with a nil tag and a nil field",
			InputPoint: serialize.TestPointWithNilTagAndNilField(),
			Output:     "1451606400000000000 nil nil\n",
		},
	}

	serialize.SerializerTest(t, cases, &Serializer{})
}
