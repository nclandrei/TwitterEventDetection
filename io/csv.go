package io

import (
	"bufio"
	"encoding/csv"
	"io"
	"log"
	"os"

	"github.com/nclandrei/TwitterEventDetection/main"
)

// ReadFromCSV reads a file and outputs a slice of tweets
func ReadFromCSV(path string) []main.Tweet {
	csvFile, _ := os.Open(path)
	reader := csv.NewReader(bufio.NewReader(csvFile))
	var tweets []main.Tweet
	for {
		line, error := reader.Read()
		if error == io.EOF {
			break
		} else if error != nil {
			log.Fatal(error)
		}
		tweets = append(tweets, main.Tweet{
			ClusterID:   convertCsvStringToInt(line[0]),
			NamedEntity: line[1],
			TweetID:     convertCsvStringToInt(line[2]),
			TimestampMS: convertCsvStringToInt(line[3]),
			UserID:      convertCsvStringToInt(line[4]),
			TweetTokens: line[5],
			TweetText:   line[6],
		})
	}
	return tweets
}
