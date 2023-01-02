package splunkhecreceiver

import (
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/splunk"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

type pair struct {
	key string
	val interface{}
}

func newPair(key string, val interface{}) pair {
	return pair{key: key, val: val}
}

type tree struct {
	Pair     *pair
	IsLeaf   bool
	Found    *tree
	NotFound *tree
	Events   []*splunk.Event
}

func (t *tree) toLogData(logs plog.Logs, resource map[string]interface{}) error {
	if t.IsLeaf {
		rl := logs.ResourceLogs().AppendEmpty()
		if err := rl.Resource().Attributes().FromRaw(resource); err != nil {
			return err
		}
		rl.Resource().Attributes().Sort()
		lr := rl.ScopeLogs().AppendEmpty().LogRecords()
		lr.EnsureCapacity(len(t.Events))
		for _, e := range t.Events {
			log := lr.AppendEmpty()
			log.Body().FromRaw(e.Event)
			if e.Time != nil {
				log.SetTimestamp(pcommon.Timestamp(*e.Time * 1e9))
			}
			attrs := log.Attributes()
			for k, v := range e.Fields {
				if _, found := resource[k]; !found {
					if err := attrs.PutEmpty(k).FromRaw(v); err != nil {
						return err
					}
				}
			}
			attrs.Sort()
		}
		return nil
	}
	if t.NotFound != nil {
		if err := t.NotFound.toLogData(logs, resource); err != nil {
			return err
		}
	}
	resource[t.Pair.key] = t.Pair.val
	if err := t.Found.toLogData(logs, resource); err != nil {
		return err
	}
	delete(resource, t.Pair.key)
	return nil
}

func splunkHecToLogData1(events []*splunk.Event, config *Config) (plog.Logs, error) {
	for _, event := range events {
		if event.Host != "" {
			event.Fields[config.HecToOtelAttrs.Host] = event.Host
		}
		if event.Source != "" {
			event.Fields[config.HecToOtelAttrs.Source] = event.Source
		}
		if event.SourceType != "" {
			event.Fields[config.HecToOtelAttrs.SourceType] = event.SourceType
		}
		if event.Index != "" {
			event.Fields[config.HecToOtelAttrs.Index] = event.Index
		}
	}
	visited := make(map[pair]struct{})
	head := splunkHecToLogDataRecursive(events, visited)
	logs := plog.NewLogs()
	resource := make(map[string]interface{})
	if err := head.toLogData(logs, resource); err != nil {
		return logs, err
	}
	return logs, nil
}

func splunkHecToLogDataRecursive(events []*splunk.Event, visited map[pair]struct{}) *tree {
	if len(events) == 0 {
		return nil
	}
	counter := make(map[pair][]int)
	var maxPair pair
	var maxCount int
	for i, event := range events {
		for k, v := range event.Fields {
			p := newPair(k, v)
			if _, ok := visited[p]; !ok {
				counter[p] = append(counter[p], i)
			}
		}
	}
	for k, v := range counter {
		if len(v) > maxCount {
			maxCount = len(v)
			maxPair = k
		}
	}

	if maxCount <= 1 {
		return &tree{
			IsLeaf: true,
			Events: events,
		}
	}
	visited[maxPair] = struct{}{}

	left := 0
	for _, found := range counter[maxPair] {
		if left != found {
			events[left], events[found] = events[found], events[left]
		}
		left++
	}
	return &tree{
		Pair:     &maxPair,
		Found:    splunkHecToLogDataRecursive(events[:left], visited),
		NotFound: splunkHecToLogDataRecursive(events[left:], visited),
	}
}
