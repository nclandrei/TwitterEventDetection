package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
)

// Cluster defines the structure of a cluster composed of multiple tweets
type Cluster struct {
	tweets []Tweet
}

// Tweet defines the structure of a tweet
type Tweet struct {
	clusterID   int
	namedEntity string
	tweetID     int
	userID      int
	timestampMS int
	tweetTokens string
	tweetText   string
}

func main() {
	csvFile, _ := os.Open("clusters.sortedby.clusterid.csv")
	reader := csv.NewReader(bufio.NewReader(csvFile))
	var tweets []Tweet
	for {
		line, error := reader.Read()
		if error == io.EOF {
			break
		} else if error != nil {
			log.Fatal(error)
		}
		tweets = append(tweets, Tweet{
			clusterID:   convertCsvStringToInt(line[0]),
			namedEntity: line[1],
			tweetID:     convertCsvStringToInt(line[2]),
			timestampMS: convertCsvStringToInt(line[3]),
			userID:      convertCsvStringToInt(line[4]),
			tweetTokens: line[5],
			tweetText:   line[6],
		})
	}
	for _, tweet := range tweets {
		fmt.Printf(tweet.namedEntity)
	}
}

func convertCsvStringToInt(str string) int {
	val, err := strconv.Atoi(str)
	if err != nil {
		panic(fmt.Sprintf("CSV value is not a string %v", str))
	}
	return val
}

func createClusters(tweets []Tweet) []Cluster {
	var clusters = []Cluster{}
}
