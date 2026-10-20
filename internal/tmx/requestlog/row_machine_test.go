package requestlog

import "testing"

func TestMachineFromKey(t *testing.T) {
	cases := map[string]string{
		"sk-tmx-macbook-3f9a2c":                   "macbook",
		"sk-tmx-stg-macbook-3f9a2c":               "macbook",
		"sk-tmx-Alien.lab-x":                      "alien.lab",
		"sk-tmx-0c6cb8b04de6dcbf25f1cff1579a22ad": "",
		"sk-tmx-stg-1234abcd5678ef90":             "",
		"sk-tmx-deadbeef-cafe":                    "",
		"sk-tmx-macbook":                          "",
		"sk-other-macbook-1":                      "",
		"sk-tmx-mac book-1":                       "",
		"":                                        "",
	}
	for in, want := range cases {
		if got := MachineFromKey(in); got != want {
			t.Errorf("MachineFromKey(%q) = %q, want %q", in, got, want)
		}
	}
}
