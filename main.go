package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"sort"
	"time"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

type Candle struct {
	OpeningTime int64
	ClosingTime int64
	Open        float64
	Close       float64
	Low         float64
	High        float64
	Volume      float64
}

func handleError(err error, message string) {
	if err != nil {
		log.Fatal(err, "\n", message)
	}
}

func initializeData(filename string) (int64, map[string][]interface{}) {
	tradeData := make(map[string][]interface{})

	file, err := local.NewLocalFileReader("./data/" + filename + ".parquet")
	handleError(err, "Error Opening File")
	defer file.Close()

	pr, err := reader.NewParquetReader(file, nil, 4)
	handleError(err, "Error creating parquet reader")
	defer pr.ReadStop()

	numRows := int64(pr.GetNumRows())
	fmt.Println("Total Rows Read :", numRows)

	tradeData["date"], _, _, err = pr.ReadColumnByIndex(0, numRows)
	handleError(err, "Cannot Read Column Details")
	tradeData["open"], _, _, err = pr.ReadColumnByIndex(1, numRows)
	handleError(err, "Cannot Read Column Details")
	tradeData["high"], _, _, err = pr.ReadColumnByIndex(2, numRows)
	handleError(err, "Cannot Read Column Details")
	tradeData["low"], _, _, err = pr.ReadColumnByIndex(3, numRows)
	handleError(err, "Cannot Read Column Details")
	tradeData["close"], _, _, err = pr.ReadColumnByIndex(4, numRows)
	handleError(err, "Cannot Read Column Details")
	tradeData["volume"], _, _, err = pr.ReadColumnByIndex(5, numRows)
	handleError(err, "Cannot Read Column Details")

	return numRows, tradeData
}

func filterData(numRows int64, tradeData map[string][]interface{}) map[string][]interface{} {
	filterData := make(map[string][]interface{})

	for i := 0; i < int(numRows); i++ {
		ns, isOK := tradeData["date"][i].(int64)
		if !isOK {
			return nil
		}
		timestamp := time.Unix(0, ns).UTC()
		if timestamp.Format("2006-01-02") == "2024-01-10" {
			filterData["date"] = append(filterData["date"], tradeData["date"][i])
			filterData["open"] = append(filterData["open"], tradeData["open"][i])
			filterData["close"] = append(filterData["close"], tradeData["close"][i])
			filterData["high"] = append(filterData["high"], tradeData["high"][i])
			filterData["low"] = append(filterData["low"], tradeData["low"][i])
			filterData["volume"] = append(filterData["volume"], tradeData["volume"][i])
		}
	}
	return filterData
}

func create5MinuteWindow(filteredData map[string][]interface{}) map[time.Time]*Candle {
	candles := make(map[time.Time]*Candle)

	for i := 0; i < len(filteredData["date"]); i++ {
		timestamp := time.Unix(0, filteredData["date"][i].(int64)).UTC()
		// fmt.Println("Timestamp ", timestamp)
		roundedTime := timestamp.Truncate(5 * time.Minute) //round off to the nearest 5 minute mark
		// fmt.Println("Rounded Time ", roundedTime)
		if _, exists := candles[roundedTime]; !exists {
			candles[roundedTime] = &Candle{
				OpeningTime: filteredData["date"][i].(int64),
				ClosingTime: filteredData["date"][i].(int64),
				Open:        filteredData["open"][i].(float64),
				High:        filteredData["high"][i].(float64),
				Low:         filteredData["low"][i].(float64),
				Close:       filteredData["close"][i].(float64),
			}
		} else {
			// Update High, Low, Close
			c := candles[roundedTime]
			cTime := time.Unix(0, c.ClosingTime).UTC()
			oTime := time.Unix(0, c.OpeningTime).UTC()

			c.High = max(c.High, filteredData["high"][i].(float64))
			c.Low = min(c.Low, filteredData["low"][i].(float64))
			if cTime.Sub(timestamp) < 0 {
				// timestamp = time.Unix(0, filteredData["date"][i].(int64)).UTC()
				// fmt.Println("Closing Time : ", timestamp.Format("2006-01-02 15:04:00"))
				c.ClosingTime = filteredData["date"][i].(int64)
				c.Close = filteredData["close"][i].(float64)
			}
			if oTime.Sub(timestamp) > 0 {
				// timestamp = time.Unix(0, filteredData["date"][i].(int64)).UTC()
				// fmt.Println("Opening Time : ", timestamp.Format("2006-01-02 15:04:00"))
				c.OpeningTime = filteredData["date"][i].(int64)
				c.Open = filteredData["open"][i].(float64)
			}
		}
		// c := candles[roundedTime]
		// oTime := time.Unix(0, c.OpeningTime).UTC()
		// fmt.Println("Opening Time : ", oTime.Format("2006-01-02 15:04:00"))

	}
	return candles
}

func createCSV(candles map[time.Time]*Candle, filename string) []time.Time {
	file, err := os.Create("./5min_candles/" + filename)
	handleError(err, "Error Opening file")
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()
	writer.Write([]string{"Time", "Open", "High", "Low", "Close", "Volume"})

	var times []time.Time
	for t := range candles {
		times = append(times, t)
	}
	sort.Slice(times, func(i, j int) bool { return times[i].Before(times[j]) }) //sort by timestamp

	for _, t := range times {
		c := candles[t]
		writer.Write([]string{
			t.Format("2006-01-02 15:04"), // Format time
			fmt.Sprintf("%.2f", c.Open),
			fmt.Sprintf("%.2f", c.High),
			fmt.Sprintf("%.2f", c.Low),
			fmt.Sprintf("%.2f", c.Close),
			fmt.Sprintf("%.2f", c.Volume),
		})
	}
	fmt.Println("CSV file saved:", filename+".csv")

	fmt.Println("Calculating Fibonacci pivot points for ", filename, ".csv")
	calulateFibonacciPoints(candles, times)
	return times

}
func returnCandle(candles map[time.Time]*Candle, times []time.Time, userInput string) {
	for _, t := range times {
		c := candles[t]
		// fmt.Println("time", t)
		layout := "2006-01-02 15:04:05"

		parsedTime, err := time.Parse(layout, userInput)
		handleError(err, "Error parsing time")
		parsedTime = parsedTime.UTC()
		duration := parsedTime.Sub(t)
		// fmt.Println(parsedTime)
		// fmt.Println(d)
		// candletime := time.Unix(0, c.OpeningTime).UTC()
		// fmt.Println("Candle Opening at ", candletime.Format("2024-01-10 15:00:00"))

		if duration.Minutes() <= 5.0 && duration.Minutes() >= 0 {
			candletime := time.Unix(0, c.OpeningTime).UTC()
			fmt.Println("Candle Value at", candletime.Format("2006-01-02 15:04:00"))
			fmt.Printf("Open : %.2f\n", c.Open)
			fmt.Printf("High : %.2f\n", c.High)
			fmt.Printf("Low : %.2f\n", c.Low)
			fmt.Printf("Close : %.2f\n", c.Close)
			fmt.Printf("Volume : %.2f\n", c.Volume)

		}

	}
}
func calulateFibonacciPoints(candles map[time.Time]*Candle, times []time.Time) {
	var high, low, close float64
	for _, t := range times {
		high = max(candles[t].High, high)
		low = min(candles[t].Low, low)
		close = candles[t].Close
	}
	//pivot
	pivot := (high + low + close) / 3
	hl := high - low

	//resistance
	r1 := pivot + 0.382*hl
	r2 := pivot + 0.618*hl
	r3 := pivot + hl
	//support
	s1 := pivot - 0.382*hl
	s2 := pivot - 0.618*hl
	s3 := pivot - hl

	fmt.Println("Pivot Value : ", pivot)
	fmt.Println("Resistance Values : ")
	fmt.Printf("R1 : %v\nR2 : %v\nR3 : %v\n", r1, r2, r3)
	fmt.Println("Support Values : ")
	fmt.Printf("S1 : %v\nS2 : %v\nS3 : %v\n", s1, s2, s3)

}

func main() {

	entries, err := os.ReadDir("./data/")
	handleError(err, "Error Reading Directory")
	for _, e := range entries {
		f := e.Name()
		filename := f[:7]

		// fmt.Println("Reading data from", f)
		numRows, tradeData := initializeData(filename)

		filteredData := filterData(numRows, tradeData)
		// fmt.Println("Filtering data on 2024-01-10 from", f)

		candles := create5MinuteWindow(filteredData)
		// fmt.Println("Creating 5 minute candles for", f)

		times := createCSV(candles, filename)
		userInput := "2024-01-10 12:17:00"
		returnCandle(candles, times, userInput)

		// fmt.Println("All Process Completed For", f)
		fmt.Println()
	}
}
