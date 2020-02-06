package driver

import (
	"testing"
)

func TestStatementParser(t *testing.T) {
	tests := []struct {
		statement string
		expSQL    string
		expArgs   []string
	}{
		{
			"UPDATE balances SET balance = balance + $2 WHERE user_id = ?",
			"UPDATE balances SET balance = balance + @param_2 WHERE user_id = @param_1",
			[]string{
				"param_1", "param_2",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.statement, func(t *testing.T) {
			stmt := parseStatement(test.statement)
			if stmt.sql != test.expSQL {
				t.Errorf("expected sql '%s', but got '%s'", test.expSQL, stmt.sql)
			}
			if len(stmt.args) != len(test.expArgs) {
				t.Errorf("expected args count '%d', but got '%d'", len(test.expArgs), len(stmt.args))
			} else {
				for i, sarg := range stmt.args {
					if earg := test.expArgs[i]; sarg != earg {
						t.Errorf("expected args[%d] name '%s', got '%s'", i, earg, sarg)
					}
				}
			}
		})
	}
}
