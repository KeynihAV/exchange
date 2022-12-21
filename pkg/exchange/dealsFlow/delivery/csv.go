package delivery

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	dealPkg "github.com/KeynihAV/exchange/pkg/exchange/deal"
)

var lastSecond time.Time

func StartFlow(file *os.File, dealsFlowCh chan *dealPkg.Deal) error {

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
			fmt.Printf("Error parsing csv row: %v", err)
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
			fmt.Printf("Error parsing volume in csv row: %v", err)
			continue
		}
		price, err := strconv.ParseFloat(rec[4], 32)
		if err != nil {
			fmt.Printf("Error parsing price in csv row: %v", err)
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
