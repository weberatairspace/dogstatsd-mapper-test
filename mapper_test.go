package main

import (
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/DataDog/datadog-agent/pkg/config"
)

func TestMapper(t *testing.T) {
	cases := []struct {
		Input             string
		ExpectedName      string
		ExpectedTags      []string
		ExpectedNilResult bool
	}{
		{
			Input:             "nsq.something-else",
			ExpectedNilResult: true,
		},
		{
			Input:             "nsq.statsd.topic.top-name.metric-name",
			ExpectedName:      "nsq.statsd.topic.metric-name",
			ExpectedTags:      []string{"nsq_topic:top-name"},
			ExpectedNilResult: false,
		},
		{
			Input:             "nsq.statsd.topic.top.name.metric-name",
			ExpectedName:      "nsq.statsd.topic.metric-name",
			ExpectedTags:      []string{"nsq_topic:top.name"},
			ExpectedNilResult: false,
		},
		{
			Input:             "nsq.statsd.topic.top-name.channel.chan-name.metric-name",
			ExpectedName:      "nsq.statsd.topic.channel.metric-name",
			ExpectedTags:      []string{"nsq_topic:top-name", "nsq_channel:chan-name"},
			ExpectedNilResult: false,
		},
		{
			Input:             "nsq.statsd.topic.top.name.channel.chan.name.metric-name",
			ExpectedName:      "nsq.statsd.topic.channel.metric-name",
			ExpectedTags:      []string{"nsq_topic:top.name", "nsq_channel:chan.name"},
			ExpectedNilResult: false,
		},
		{
			Input:             "nsq.statsd.topic.app.candidates.channel.app.something.candidate.backend_depth",
			ExpectedName:      "nsq.statsd.topic.channel.backend_depth",
			ExpectedTags:      []string{"nsq_topic:app.candidates", "nsq_channel:app.something.candidate"},
			ExpectedNilResult: false,
		},
		{

			Input:             "nsq.statsd.topic.app.commands.message_count",
			ExpectedName:      "nsq.statsd.topic.message_count",
			ExpectedTags:      []string{"nsq_topic:app.commands"},
			ExpectedNilResult: false,
		},
	}

	mapper, err := getMapper(`
dogstatsd_mapper_profiles:
- mappings:
  - match: nsq\.statsd\.topic\.(.*)\.channel\.(.*)\.([^.\r\n]+)$
    match_type: regex
    name: nsq.statsd.topic.channel.${3}
    tags:
      nsq_channel: ${2}
      nsq_topic: ${1}
  name: nsq_statsd_metric_mapper_topic_and_channel
  prefix: nsq.
- mappings:
  - match: nsq\.statsd\.topic\.([^.]+)\.([^.]+)\.([^.]+)\.([^.\r\n]+)$
    match_type: regex
    name: nsq.statsd.topic.${4}
    tags:
      nsq_topic: ${1}.${2}.${3}
  name: nsq_statsd_metric_mapper_topic_3
  prefix: nsq.
- mappings:
  - match: nsq\.statsd\.topic\.([^.]+)\.([^.]+)\.([^.\r\n]+)$
    match_type: regex
    name: nsq.statsd.topic.${3}
    tags:
      nsq_topic: ${1}.${2}
  name: nsq_statsd_metric_mapper_topic_2
  prefix: nsq.
- mappings:
  - match: nsq\.statsd\.topic\.([^.]+)\.([^.\r\n]+)$
    match_type: regex
    name: nsq.statsd.topic.${2}
    tags:
      nsq_topic: ${1}
  name: nsq_statsd_metric_mapper_topic_1
  prefix: nsq.`)

	if err != nil {
		t.Fatalf("failed to create mapper: %v", err)
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.Input, func(t *testing.T) {
			result := mapper.Map(tc.Input)
			t.Logf("Result for '%s': %#v", tc.Input, result)

			switch {
			case tc.ExpectedNilResult && result == nil:
				return
			case tc.ExpectedNilResult && result != nil:
				t.Fatalf("expected nil result for %v", tc.Input)
			case !tc.ExpectedNilResult && result == nil:
				t.Fatalf("expected match for %v", tc.Input)
			}

			if result.Name != tc.ExpectedName {
				t.Errorf("expected %v to equal %v", result.Name, tc.ExpectedName)
			}

			sort.Strings(result.Tags)
			sort.Strings(tc.ExpectedTags)
			if !reflect.DeepEqual(result.Tags, tc.ExpectedTags) {
				t.Errorf("expected %v to equal %v", result.Tags, tc.ExpectedTags)
			}
		})
	}
}

func getMapper(configString string) (*MetricMapper, error) {
	var profiles []config.MappingProfile
	config.Datadog.SetConfigType("yaml")
	err := config.Datadog.ReadConfig(strings.NewReader(configString))
	if err != nil {
		return nil, err
	}
	err = config.Datadog.UnmarshalKey("dogstatsd_mapper_profiles", &profiles)
	if err != nil {
		return nil, err
	}
	mapper, err := NewMetricMapper(profiles, 1000)
	if err != nil {
		return nil, err
	}
	return mapper, err
}
