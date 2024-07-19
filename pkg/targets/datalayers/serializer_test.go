package datalayers

import (
	"testing"

	"github.com/timescale/tsbs/pkg/data/serialize"
)

// Tests that the Datalayers' serializer works as expected.
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
			Output:     "cpu 1451606400000000000 hostname=host_0 region=eu-west-1 datacenter=eu-west-1b usage_guest_nice=38.24311829 13,13,13,12\n",
		},
		{
			Desc:       "a regular Point using int as value",
			InputPoint: serialize.TestPointInt(),
			Output:     "cpu 1451606400000000000 hostname=host_0 region=eu-west-1 datacenter=eu-west-1b usage_guest=38 13,13,13,7\n",
		},
		{
			Desc:       "a regular Point with multiple fields",
			InputPoint: serialize.TestPointMultiField(),
			Output:     "cpu 1451606400000000000 hostname=host_0 region=eu-west-1 datacenter=eu-west-1b big_usage_guest=5000000000 usage_guest=38 usage_guest_nice=38.24311829 13,13,13,9,7,12\n",
		},
		{
			Desc:       "a Point with no tags",
			InputPoint: serialize.TestPointNoTags(),
			Output:     "cpu 1451606400000000000 usage_guest_nice=38.24311829 12\n",
		},
		{
			Desc:       "a Point with no fields",
			InputPoint: serialize.TestPointNoFields(),
			Output:     "cpu 1451606400000000000 hostname=host_0 13\n",
		},
		{
			Desc:       "a Point with a nil tag",
			InputPoint: serialize.TestPointWithNilTag(),
			Output:     "cpu 1451606400000000000 hostname=nil usage_guest_nice=38.24311829 0,12\n",
		},
		{
			Desc:       "a Point with a nil field",
			InputPoint: serialize.TestPointWithNilField(),
			Output:     "cpu 1451606400000000000 big_usage_guest=nil usage_guest_nice=38.24311829 0,12\n",
		},
		{
			Desc:       "a Point with a nil tag and a nil field",
			InputPoint: serialize.TestPointWithNilTagAndNilField(),
			Output:     "cpu 1451606400000000000 hostname=nil big_usage_guest=nil 0,0\n",
		},
	}

	serialize.SerializerTest(t, cases, &Serializer{})
}
