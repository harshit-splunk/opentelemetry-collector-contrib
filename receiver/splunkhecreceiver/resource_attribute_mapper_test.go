package splunkhecreceiver

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/splunk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
)

func arrangeNums(number, arrange []int) {
	left := 0
	for _, found := range arrange {
		if left != found {
			number[left], number[found] = number[found], number[left]
		}
		left++
	}
}

func TestArrangeSlice(t *testing.T) {
	// numbers := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	// arrange := []int{0, 2, 4, 6, 8}
	// // []int{1, 3, 5, 7, 9}
	// arrangeNums(numbers, arrange)
	// assert.EqualValues(t, []int{1, 3, 5, 7, 9}, numbers[:len(arrange)])

	tests := []struct {
		name     string
		numbers  []int
		arrange  []int
		expected []int
	}{
		{
			name:     "Odd",
			numbers:  []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			arrange:  []int{0, 2, 4, 6, 8},
			expected: []int{1, 3, 5, 7, 9},
		},
		{
			name:     "Even",
			numbers:  []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			arrange:  []int{1, 3, 5, 7, 9},
			expected: []int{2, 4, 6, 8, 10},
		},
		{
			name:     "random",
			numbers:  []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			arrange:  []int{4, 5, 6, 9},
			expected: []int{5, 6, 7, 10},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			copyArray := append([]int{}, test.numbers...)
			arrangeNums(test.numbers, test.arrange)
			assert.EqualValues(t, test.expected, test.numbers[:len(test.arrange)])
			sort.Ints(copyArray)
			sort.Ints(test.numbers)

			assert.EqualValues(t, copyArray, test.numbers)
		})
	}
}

func generateEvents() []*splunk.Event {
	return []*splunk.Event{
		{
			Fields: map[string]interface{}{
				"A": int64(1),
				"B": int64(1),
				"C": int64(1),
				"D": int64(1),
				"E": int64(1),
			},
			Event: "1",
		},
		{
			Fields: map[string]interface{}{
				"A": int64(2),
				"B": int64(1),
				"C": int64(1),
				"D": int64(1),
				"E": int64(2),
			},
			Event: "2",
		}, {
			Fields: map[string]interface{}{
				"A": int64(3),
				"B": int64(2),
				"C": int64(1),
				"D": int64(1),
				"E": int64(1),
			},
			Event: "3",
		},
	}
}

// func TestSplunkHecToLogData1(t *testing.T) {

// 	events := generateEvents()

// 	logs, err := splunkHecToLogData1(events, createDefaultConfig().(*Config))
// 	assert.NoError(t, err)
// 	assert.Equal(t, 3, logs.LogRecordCount())
// 	sort.Slice(events, func(i, j int) bool {
// 		return events[i].Event.(string) < events[j].Event.(string)
// 	})
// 	got := logToEvent(logs)
// 	assert.EqualValues(t, events, got)
// }

func logToEvent(logs plog.Logs) []*splunk.Event {
	events := make([]*splunk.Event, 0, logs.LogRecordCount())
	for i := 0; i < logs.ResourceLogs().Len(); i++ {
		lr := logs.ResourceLogs().At(i).ScopeLogs().At(0).LogRecords()
		resourceMap := logs.ResourceLogs().At(i).Resource().Attributes().AsRaw()
		for j := 0; j < lr.Len(); j++ {
			log := lr.At(j)
			fields := make(map[string]interface{})
			for k, v := range resourceMap {
				fields[k] = v
			}
			for k, v := range log.Attributes().AsRaw() {
				fields[k] = v
			}
			events = append(events, &splunk.Event{
				Event:  log.Body().AsRaw(),
				Fields: fields,
			})
		}
	}
	sort.Slice(events, func(i, j int) bool {
		return events[i].Event.(string) < events[j].Event.(string)
	})
	return events
}

// func BenchmarkFunction(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		events := generateEvents()
// 		logs, err := splunkHecToLogData1(events, createDefaultConfig().(*Config))
// 		assert.NoError(b, err)
// 		assert.Equal(b, 3, logs.LogRecordCount())
// 	}
// }

func getMap() map[string]interface{} {
	resource := map[string]interface{}{}
	r := rand.New(rand.NewSource(65))
	for len(resource) < 1000 {
		key := fmt.Sprint(r.Int())
		resource[key] = len(resource)
	}
	return resource
}
func BenchmarkSortCreate(b *testing.B) {
	for i := 0; i < b.N; i++ {
		resource := getMap()
		keys := make([]string, 0, len(resource))
		for k := range resource {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		attr := plog.NewLogs().ResourceLogs().AppendEmpty().Resource().Attributes()
		attr.EnsureCapacity(len(keys))
		for _, k := range keys {
			attr.PutEmpty(k).FromRaw(resource[k])
		}
	}

}
func BenchmarkCreateSort(b *testing.B) {
	for i := 0; i < b.N; i++ {
		resource := getMap()
		rl := plog.NewLogs().ResourceLogs().AppendEmpty()
		err := rl.Resource().Attributes().FromRaw(resource)
		require.NoError(b, err)
		rl.Resource().Attributes().Sort()
	}
}
