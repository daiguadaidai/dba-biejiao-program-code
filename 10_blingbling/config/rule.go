package config

const (
	RULE_NAME_REG = `(?i)^[a-z_][a-z0-9_\$]*$`
	RULE_INDEX_NAME_REG = `^idx_[a-z0-9_]*$`
	RULE_ALLOW_COLUMN_NULL = true
	RULE_ALLOW_DUPLICATE_INDEX = false
	RULE_ALLOW_DELETE_NO_WHERE = false
	RULE_ALLOW_DELETE_MAX_ROWS = 10000
)
