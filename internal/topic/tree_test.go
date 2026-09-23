package topic

import (
	"BA-Broker/internal/mqtt"
	"reflect"
	"testing"
)

func TestTreeSubscribeAndMatch(t *testing.T) {

	test := map[string]mqtt.QoS{
		"1": mqtt.AtMostOnce,
	}
	tree := NewTree()
	err := tree.Subscribe("a/b", "1", mqtt.AtMostOnce)
	if err != nil {
		t.Errorf("Subscribe error: %s", err)
	}
	if got := tree.Match("a/b"); !reflect.DeepEqual(got, test) {
		t.Errorf("Match(%q) = %v, want %v", "a/b", got, test)
	}
}

func TestTreeMatchMergesMaxQoS(t *testing.T) {
	test := map[string]mqtt.QoS{
		"1": mqtt.ExactlyOnce,
	}
	subscriptions := []struct {
		filter   string
		clientID string
		QoS      mqtt.QoS
	}{
		{filter: "sport/+", clientID: "1", QoS: mqtt.AtMostOnce},
		{filter: "sport/tennis", clientID: "1", QoS: mqtt.ExactlyOnce},
	}
	tree := NewTree()
	for _, sub := range subscriptions {
		err := tree.Subscribe(sub.filter, sub.clientID, sub.QoS)
		if err != nil {
			t.Errorf("Subscribe error: %s", err)
		}
	}
	err := tree.Subscribe("sport/+", "1", mqtt.AtMostOnce)
	if err != nil {
		t.Errorf("Subscribe error: %s", err)
	}
	if got := tree.Match("sport/tennis"); !reflect.DeepEqual(got, test) {
		t.Errorf("Match(%q) = %v, want %v", "sport/tennis", got, test)
	}

}

func TestTreeUnsubscribe(t *testing.T) {

	topic := "a/b"
	clientID := "1"
	test := map[string]mqtt.QoS{
		clientID: mqtt.AtMostOnce,
	}
	tree := NewTree()
	err := tree.Subscribe(topic, clientID, mqtt.AtMostOnce)
	if err != nil {
		t.Errorf("Subscribe error: %s", err)
	}

	tree.Unsubscribe(topic, clientID)
	result := reflect.DeepEqual(test, tree.Match(topic))
	if result {
		t.Errorf("Client %v should have been unsubscribed to topic %v", clientID, topic)
	}

}

func TestTreeUnsubscribeUnknownFilter(t *testing.T) {

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("The code did panic")
		}
	}()

	topic := "a/b"
	clientID := "1"
	tree := NewTree()
	tree.Unsubscribe(topic, clientID)
}

func TestTreeSubscribeInvalidFilter(t *testing.T) {
	filter := "a/#/b"
	tree := NewTree()
	err := tree.Subscribe(filter, "1", mqtt.AtMostOnce)
	if err == nil {
		t.Errorf("Filter %s is invalid but no error was occurred", filter)
	}
}

func TestTreeWildcardDepth(t *testing.T) {
	filter := "sport/+"
	tree := NewTree()
	err := tree.Subscribe(filter, "1", mqtt.AtMostOnce)
	if err != nil {
		t.Errorf("Subscribe error: %s", err)
	}

	want := map[string]mqtt.QoS{"1": mqtt.AtMostOnce}
	if got := tree.Match("sport/tennis"); !reflect.DeepEqual(got, want) {
		t.Errorf("Match(%q) = %v, want %v", "sport/tennis", got, want)
	}

	if got := tree.Match("sport/tennis/scores"); len(got) != 0 {
		t.Errorf("Match(%q) = %v, want empty (%q is only one level deep)", "sport/tennis/scores", got, filter)
	}
}
