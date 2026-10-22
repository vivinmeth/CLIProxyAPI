package openai

import "testing"

func TestResponsesSSEFramerKeepsSubscriptionUsageForMuse(t *testing.T) {
	if !(&responsesSSEFramer{}).shouldFilterPrivateEvent("response.subscription_usage", "") {
		t.Fatal("non-Muse client should not see subscription_usage")
	}
	if (&responsesSSEFramer{isMuseClient: true}).shouldFilterPrivateEvent("response.subscription_usage", "response.subscription_usage") {
		t.Fatal("Muse client should see subscription_usage")
	}
	if !(&responsesSSEFramer{isMuseClient: true}).shouldFilterPrivateEvent("responsesapi.timing", "") {
		t.Fatal("Muse client should still not see responsesapi.* telemetry")
	}
}
