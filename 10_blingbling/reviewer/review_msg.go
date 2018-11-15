package reviewer

type ReviewMSG struct {
	Sql string
	HaveError bool
	HaveWarning bool
	ErrorMSGs []string
	WarningMSGs []string
}

func NewReviewMSG() *ReviewMSG {
	reviewMSG := new(ReviewMSG)
	reviewMSG.ErrorMSGs = make([]string, 0, 1)
	reviewMSG.WarningMSGs = make([]string, 0, 1)

	return reviewMSG
}

func (this *ReviewMSG) AppendMSG(_haveError bool, _msg string) {
	if _haveError {
		this.HaveError = _haveError
		this.ErrorMSGs = append(this.ErrorMSGs, _msg)
	} else {
		if _msg != "" {
			this.HaveWarning = true
			this.WarningMSGs = append(this.WarningMSGs, _msg)
		}
	}
}
