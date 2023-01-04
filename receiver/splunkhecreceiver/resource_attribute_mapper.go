package splunkhecreceiver

import (
	"fmt"
	"sort"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/splunk"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type keyValPair struct {
	key string
	val interface{}
}
type indexPair struct {
	event int
	pair  int
}

type eventEnvelope struct {
	*splunk.Event
	pairs   []keyValPair
	metrics map[string]interface{}
}

type tree struct {
	pair     *keyValPair
	isLeaf   bool
	found    *tree
	notFound *tree
	events   []eventEnvelope
}

// resourceAttributeMapper should not be resused as it is not thread-safe
type resourceAttributeMapper struct {
	config      *Config
	events      []*splunk.Event
	logs        plog.Logs
	metrics     pmetric.Metrics
	resource    map[string]interface{}
	hashToValue map[string]interface{}
}

func newResourceAttributeMapper(config *Config, events []*splunk.Event) *resourceAttributeMapper {
	return &resourceAttributeMapper{
		config:      config,
		events:      events,
		logs:        plog.NewLogs(),
		hashToValue: make(map[string]interface{}),
		resource:    make(map[string]interface{}),
	}
}

func (ram *resourceAttributeMapper) toLogData(t *tree) error {
	// If current tree node is not leaf, then
	// - traverse notFound subTree
	// - add t.pair to resource map
	// - traverse found subTree
	// - delete t.pair from resource map
	if !t.isLeaf {
		if t.notFound != nil {
			if err := ram.toLogData(t.notFound); err != nil {
				return err
			}
		}
		ram.resource[t.pair.key] = t.pair.val
		if err := ram.toLogData(t.found); err != nil {
			return err
		}
		delete(ram.resource, t.pair.key)
		return nil
	}

	rl := ram.logs.ResourceLogs().AppendEmpty()

	// Create ResourceLogs with `resource` atributes
	resourceKeys := make([]string, 0, len(ram.resource))
	for k := range ram.resource {
		resourceKeys = append(resourceKeys, k)
	}
	sort.Strings(resourceKeys)
	rl.Resource().Attributes().EnsureCapacity(len(resourceKeys))
	for _, k := range resourceKeys {
		if err := rl.Resource().Attributes().PutEmpty(k).FromRaw(ram.resource[k]); err != nil {
			return err
		}
	}

	lr := rl.ScopeLogs().AppendEmpty().LogRecords()
	lr.EnsureCapacity(len(t.events))

	// Add log record for each event
	for _, e := range t.events {
		log := lr.AppendEmpty()
		log.Body().FromRaw(e.Event.Event)
		if e.Time != nil {
			log.SetTimestamp(pcommon.Timestamp(*e.Time * 1e9))
		}

		// Retain original pairs slice using it's capacity and sort it
		pairs := e.pairs[:cap(e.pairs)]
		sort.Slice(pairs, func(i, j int) bool {
			return pairs[i].key < pairs[j].key
		})
		attrs := log.Attributes()
		attrs.EnsureCapacity(len(pairs) - len(resourceKeys))

		// Add all pair that are not added in resource attribute
		i := 0
		for j := 0; j < len(pairs); j++ {
			if i < len(resourceKeys) && resourceKeys[i] == pairs[j].key {
				i++
				continue
			}
			if err := attrs.PutEmpty(pairs[j].key).FromRaw(pairs[j].val); err != nil {
				return err
			}
		}
		// TODO: CLEANUP: remove
		if i < len(resourceKeys) {
			panic(fmt.Sprint("Not all resource attributes are consumed", e, ram.resource))
		}
	}
	return nil
}

func (ram *resourceAttributeMapper) toMetricData(t *tree) error {
	// If current tree node is not leaf, then
	// - traverse notFound subTree
	// - add t.pair to resource map
	// - traverse found subTree
	// - delete t.pair from resource map
	if !t.isLeaf {
		if t.notFound != nil {
			if err := ram.toLogData(t.notFound); err != nil {
				return err
			}
		}
		ram.resource[t.pair.key] = t.pair.val
		if err := ram.toLogData(t.found); err != nil {
			return err
		}
		delete(ram.resource, t.pair.key)
		return nil
	}

	rm := ram.metrics.ResourceMetrics().AppendEmpty()

	// Create ResourceLogs with `resource` atributes
	resourceKeys := make([]string, 0, len(ram.resource))
	for k := range ram.resource {
		resourceKeys = append(resourceKeys, k)
	}
	sort.Strings(resourceKeys)
	rm.Resource().Attributes().EnsureCapacity(len(resourceKeys))
	for _, k := range resourceKeys {
		if err := rm.Resource().Attributes().PutEmpty(k).FromRaw(ram.resource[k]); err != nil {
			return err
		}
	}

	metrics := rm.ScopeMetrics().AppendEmpty().Metrics()
	metrics.EnsureCapacity(len(t.events))

	// Add log record for each event
	for _, e := range t.events {
		// log := lr.AppendEmpty()
		// log.Body().FromRaw(e.Event.Event)
		// if e.Time != nil {
		// 	log.SetTimestamp(pcommon.Timestamp(*e.Time * 1e9))
		// }

		// Retain original pairs slice using it's capacity and sort it
		pairs := e.pairs[:cap(e.pairs)]
		sort.Slice(pairs, func(i, j int) bool {
			return pairs[i].key < pairs[j].key
		})
		attrs := pcommon.NewMap()
		attrs.EnsureCapacity(len(pairs) - len(resourceKeys))

		// Add all pair that are not added in resource attribute
		i := 0
		for j := 0; j < len(pairs); j++ {
			if i < len(resourceKeys) && resourceKeys[i] == pairs[j].key {
				i++
				continue
			}
			if err := attrs.PutEmpty(pairs[j].key).FromRaw(pairs[j].val); err != nil {
				return err
			}
		}
		// TODO: CLEANUP: remove
		if i < len(resourceKeys) {
			panic(fmt.Sprint("Not all resource attributes are consumed", e, ram.resource))
		}

	}
	return nil
}

func (ram *resourceAttributeMapper) splunkHecToLogData() (plog.Logs, error) {
	envelops := make([]eventEnvelope, 0, len(ram.events))
	// hashToValue := make(map[string]interface{})
	for _, event := range ram.events {
		hecToOtelPairs := make([]keyValPair, 0, 4)
		if event.Host != "" {
			hecToOtelPairs = append(hecToOtelPairs, keyValPair{ram.config.HecToOtelAttrs.Host, event.Host})
		}
		if event.Source != "" {
			hecToOtelPairs = append(hecToOtelPairs, keyValPair{ram.config.HecToOtelAttrs.Source, event.Source})
		}
		if event.SourceType != "" {
			hecToOtelPairs = append(hecToOtelPairs, keyValPair{ram.config.HecToOtelAttrs.SourceType, event.SourceType})
		}
		if event.Index != "" {
			hecToOtelPairs = append(hecToOtelPairs, keyValPair{ram.config.HecToOtelAttrs.Index, event.Index})
		}

		// It needs accurate capacity of the pairs to retrieve the original pairs
		// after multiple slicing operation
		pairs := make([]keyValPair, 0, len(event.Fields)+len(hecToOtelPairs))
		for k, v := range event.Fields {
			// TODO: Calculate hash for non-comparable values type(i.e. slice) and store it in hashToValue
			hecToOtelPairs = append(hecToOtelPairs, keyValPair{k, v})
		}
		pairs = append(pairs, hecToOtelPairs...)
		envelops = append(envelops, eventEnvelope{
			Event: event,
			pairs: pairs,
		})
	}

	head := splunkHecToLogDataRecursive(envelops)
	if err := ram.toLogData(head); err != nil {
		return ram.logs, err
	}
	return ram.logs, nil
}

func (ram *resourceAttributeMapper) splunkHecToMetricData() (pmetric.Metrics, error) {
	envelops := make([]eventEnvelope, 0, len(ram.events))

	for _, event := range ram.events {
		hecToOtelPairs := make([]keyValPair, 0, 4)
		if event.Host != "" {
			hecToOtelPairs = append(hecToOtelPairs, keyValPair{ram.config.HecToOtelAttrs.Host, event.Host})
		}
		if event.Source != "" {
			hecToOtelPairs = append(hecToOtelPairs, keyValPair{ram.config.HecToOtelAttrs.Source, event.Source})
		}
		if event.SourceType != "" {
			hecToOtelPairs = append(hecToOtelPairs, keyValPair{ram.config.HecToOtelAttrs.SourceType, event.SourceType})
		}
		if event.Index != "" {
			hecToOtelPairs = append(hecToOtelPairs, keyValPair{ram.config.HecToOtelAttrs.Index, event.Index})
		}

		metricValues := event.GetMetricValues()

		pairs := make([]keyValPair, 0, len(event.Fields)-len(metricValues)+len(hecToOtelPairs))
		for k, v := range event.Fields {
			if _, ok := metricValues[k]; ok {
				continue
			}
			// TODO: Calculate hash for non-comparable values type(i.e. slice) and store it in hashToValue
			hecToOtelPairs = append(hecToOtelPairs, keyValPair{k, v})
		}
		envelops = append(envelops, eventEnvelope{
			Event:   event,
			pairs:   pairs,
			metrics: metricValues,
		})

		head := splunkHecToLogDataRecursive(envelops)

		_ = head

	}
	return pmetric.NewMetrics(), nil
}

func splunkHecToLogDataRecursive(envelops []eventEnvelope) *tree {
	if len(envelops) == 0 {
		return nil
	}

	// Counter for each key-value pair.
	// indexPair[0] -> index of the event containg the key-value pair
	// indexPair[1] -> index of key-value pair in indexPair[0]th event
	counter := make(map[keyValPair][]indexPair)
	for i, envelop := range envelops {
		for j, p := range envelop.pairs {
			counter[p] = append(counter[p], indexPair{i, j})
		}
	}

	// Find the most frequent key-value pair
	var maxPair keyValPair
	var maxCount int
	for k, v := range counter {
		if len(v) > maxCount {
			maxCount = len(v)
			maxPair = k
		}
	}

	if maxCount <= 1 {
		return &tree{
			isLeaf: true,
			events: envelops,
		}
	}

	// For each event containing maxPair, move the maxPair at the end of the pairs slice
	for _, found := range counter[maxPair] {
		envelop := &envelops[found.event]
		lastIndex := len(envelop.pairs) - 1
		// move the maxPair at the end of the pairs slice
		envelop.pairs[found.pair], envelop.pairs[lastIndex] = envelop.pairs[lastIndex], envelop.pairs[found.pair]
		// update pairs slice
		envelop.pairs = envelop.pairs[:lastIndex]
	}

	// Group all the events containing the maxPair such that
	// envelops[:left] contains maxPair and
	// envelops[left:] doesn't contain maxPair
	left := 0
	for _, found := range counter[maxPair] {
		if left != found.event {
			envelops[left], envelops[found.event] = envelops[found.event], envelops[left]
		}
		left++
	}

	return &tree{
		pair:     &maxPair,
		found:    splunkHecToLogDataRecursive(envelops[:left]),
		notFound: splunkHecToLogDataRecursive(envelops[left:]),
	}
}
