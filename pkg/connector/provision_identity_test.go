package connector

import "testing"

func TestNormalizeProvisionUserID(t *testing.T) {
	got, err := NormalizeProvisionUserID("WA-BIND:C2AF2FDE-26EA-4553-BCBC-CB688D0AB218")
	if err != nil {
		t.Fatal(err)
	}
	if got != "wa-bind:c2af2fde-26ea-4553-bcbc-cb688d0ab218" {
		t.Fatalf("新绑定键应规范成小写，got %s", got)
	}

	legacy := "@binding_c2af2fde26ea4553bcbccb688d0ab218:localhost"
	got, err = NormalizeProvisionUserID(legacy)
	if err != nil || string(got) != legacy {
		t.Fatalf("旧合成 MXID 必须原样保留，got %s err %v", got, err)
	}

	device := "@da_fit-AA-BB-CC-DD-EE-FF:localhost"
	got, err = NormalizeProvisionUserID(device)
	if err != nil || string(got) != device {
		t.Fatalf("已落库的旧设备键必须保留，got %s err %v", got, err)
	}

	for _, raw := range []string{"", "binding-id", "@onlylocalpart", "wa-bind:not-a-uuid", "wa-bind:"} {
		if _, err := NormalizeProvisionUserID(raw); err == nil {
			t.Fatalf("%q 应被拒绝", raw)
		}
	}
}
