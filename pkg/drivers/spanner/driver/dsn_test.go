package driver

import "testing"

func TestParseConnectionURI(t *testing.T) {
	tests := []struct {
		dsn         string
		expected    DSN
		expectError bool
	}{
		{"spanner:///projects/P/instances/I/databases/D",
			DSN{
				ProjectName:  "P",
				InstanceName: "I",
				DatabaseName: "D",
			}, false,
		},
		{"spanner:///projects/P/instances/I/databases/D/",
			DSN{
				ProjectName:  "P",
				InstanceName: "I",
				DatabaseName: "D",
			}, false,
		},
		{"spanner:///projects/P/instances/I%24/databases/D",
			DSN{
				ProjectName:  "P",
				InstanceName: "I$",
				DatabaseName: "D",
			}, false,
		},
		{"",
			DSN{}, true,
		},
		{"!#%!!$@!$!@$!%",
			DSN{}, true,
		},
		{"http:///projects/P/instances/I%24/databases/D",
			DSN{}, true,
		},
		{"spanner:///foo/bar/foo/bar/foo/bar",
			DSN{}, true,
		},
		{"spanner:///projects/P/instances/I%24/databases/D/foo",
			DSN{}, true,
		},
	}
	for _, test := range tests {
		t.Run(test.dsn, func(t *testing.T) {
			dsn, _, err := ParseConnectionURI(test.dsn)
			if test.expectError {
				if err == nil {
					t.Fatal("expected error")
				}
				t.Logf("parse error expected: %v", err)
				return
			}
			if !test.expectError && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if *dsn != test.expected {
				t.Fatalf("dsn doesnt match expected.\nwanted\n\t%#v\n\ngot\n\t%#v", test.expected, dsn)
			}
		})
	}
}

func TestDSNString(t *testing.T) {
	tests := []struct {
		dsn      DSN
		expected string
	}{
		{
			DSN{
				ProjectName:  "P",
				InstanceName: "I",
				DatabaseName: "D",
			},
			"projects/P/instances/I/databases/D",
		},
	}
	for _, test := range tests {
		t.Run(test.expected, func(t *testing.T) {
			str := test.dsn.String()
			if str != test.expected {
				t.Fatalf("expected '%s', got '%s'", test.expected, str)
			}
		})
	}
}
