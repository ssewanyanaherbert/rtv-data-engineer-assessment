#### Metric definitions

Every rate is a ratio of sums at the level shown, never an average of rates.

| Metric | Formula | Notes |
|---|---|---|
| Raw submissions | all submissions received | includes repeat uploads for the same household |
| Deduplicated submissions | latest submission per household per cycle | ties broken by `SubmissionDate`, `endtime`, then `KEY` |
| Duplicate rate | (raw - deduplicated) / raw | |
| Missing location rate | submissions without a valid GPS fix / submissions | valid = present, not (0,0), inside Uganda |
| Avg interview duration | interview minutes / timed interviews | completed interviews, 0 < duration <= 240 min |
| Completion rate | completed / households attempted | completed = found, consented and form finalised |
| Consent rate | consented / households found | consent is only asked once the household is found |
| Follow-up consent rate | agreed to follow-up / consented | |
| Change vs Baseline (pp) | (rate in cycle - Baseline rate) x 100 | Baseline export is pre-filtered to completed interviews |
| Households observed in 1/2/3 cycles | linked households by number of cycles with a submission | linked on tracking-sheet household ID |
| Baseline to Year N retention | completed at Baseline and in Year N / completed at Baseline | |
| Age bands | Under 25, 25-34, 35-44, 45-54, 55-64, 65+ | household head age |
