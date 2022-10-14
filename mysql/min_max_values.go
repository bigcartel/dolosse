package mysql

type MinMaxValues struct {
	MinDumpPks,
	MaxDumpPks Pks
	ValuesByServerId map[string]struct {
		MinTransactionId,
		MaxTransactionId uint64
	}
}

type MinMaxValuesMap map[string]struct {
	MinTransactionId,
	MaxTransactionId uint64
}

func GetMinMaxValues(rows []MysqlReplicationRowEvent) MinMaxValues {
	valuesByServerId := make(MinMaxValuesMap, 1)
	var minDumpPks,
		maxDumpPks Pks

	for _, r := range rows {
		comparingTransactionId := r.TransactionId
		currentMinMax := valuesByServerId[r.ServerId]

		if r.Action == "dump" && len(r.Pks) > 0 {
			if minDumpPks == nil {
				minDumpPks = r.Pks
				maxDumpPks = r.Pks
			}

			if minDumpPks.Compare(r.Pks) == 1 {
				minDumpPks = r.Pks
			}
			if maxDumpPks.Compare(r.Pks) <= 0 {
				maxDumpPks = r.Pks
			}
		} else {
			if currentMinMax.MaxTransactionId < comparingTransactionId {
				currentMinMax.MaxTransactionId = comparingTransactionId
			}
			if currentMinMax.MinTransactionId == 0 || currentMinMax.MinTransactionId > comparingTransactionId {
				currentMinMax.MinTransactionId = comparingTransactionId
			}

			valuesByServerId[r.ServerId] = currentMinMax
		}
	}

	return MinMaxValues{
		MinDumpPks:       minDumpPks,
		MaxDumpPks:       maxDumpPks,
		ValuesByServerId: valuesByServerId,
	}
}
