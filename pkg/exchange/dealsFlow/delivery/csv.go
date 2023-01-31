package delivery

import (
	"encoding/csv"
	"io"
	"os"
	"strconv"
	"time"

	dealPkg "github.com/KeynihAV/exchange/pkg/exchange/deal"
	"github.com/KeynihAV/exchange/pkg/logging"
	"go.uber.org/zap"
)

var lastSecond time.Time

func StartFlow(file *os.File, dealsFlowCh chan *dealPkg.Deal, logger *logging.Logger) error {

	r := csv.NewReader(file)
	r.Comma = ','

	firstRow := true
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if firstRow {
			firstRow = false
			continue
		}

		currentSecond, err := time.Parse("20060102030405", rec[2]+rec[3])
		if err != nil {
			logger.Zap.Error("parsing csv row",
				zap.String("logger", "dealsFlow"),
				zap.String("err", err.Error()),
			)
			continue
		}
		if lastSecond.IsZero() {
			lastSecond = currentSecond
		}

		if lastSecond != currentSecond {
			for lastSecond != currentSecond {
				time.Sleep(time.Second)
				lastSecond = lastSecond.Add(time.Second)
			}
		}

		vol, err := strconv.ParseInt(rec[5], 10, 32)
		if err != nil {
			logger.Zap.Error("parsing volume in csv row",
				zap.String("logger", "dealsFlow"),
				zap.String("err", err.Error()),
			)
			continue
		}
		price, err := strconv.ParseFloat(rec[4], 32)
		if err != nil {
			logger.Zap.Error("parsing price in csv row",
				zap.String("logger", "dealsFlow"),
				zap.String("err", err.Error()),
			)
			continue
		}
		dealsFlowCh <- &dealPkg.Deal{
			Ticker: rec[0],
			Price:  float32(price),
			Volume: int32(vol),
		}
	}

	return nil
}
