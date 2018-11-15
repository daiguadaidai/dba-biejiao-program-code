package config

var reviewConfig *ReviewConfig

type ReviewConfig struct {
	RuleNameReg string
	RuleIndexNameReg string
	RuleAllowColumnNull bool
	RuleAllowDuplicateIndex bool
	RuleAllowDeleteNoWhere bool
	RuleAllowDeleteMaxRows int
}

func NewRviewConfig() *ReviewConfig {
	return &ReviewConfig{
		RuleNameReg: RULE_NAME_REG,
		RuleIndexNameReg: RULE_INDEX_NAME_REG,
		RuleAllowColumnNull: RULE_ALLOW_COLUMN_NULL,
		RuleAllowDuplicateIndex: RULE_ALLOW_DUPLICATE_INDEX,
		RuleAllowDeleteNoWhere: RULE_ALLOW_DELETE_NO_WHERE,
		RuleAllowDeleteMaxRows: RULE_ALLOW_DELETE_MAX_ROWS,
	}
}

func SetReviewConfig(_reviewConfig *ReviewConfig) {
	reviewConfig = _reviewConfig
}

func GetReviewConfig() *ReviewConfig {
	return reviewConfig
}
