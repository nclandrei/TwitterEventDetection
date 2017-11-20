package io

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"

	"github.com/nclandrei/TwitterEventDetection/cluster"
)

// ReadFromCSV reads a file and outputs a slice of tweets
func ReadFromCSV(path string) []cluster.Tweet {
	csvFile, _ := os.Open(path)
	reader := csv.NewReader(bufio.NewReader(csvFile))
	var tweets []cluster.Tweet
	for {
		line, error := reader.Read()
		if error == io.EOF {
			break
		} else if error != nil {
			log.Fatal(error)
		}
		tweets = append(tweets, cluster.Tweet{
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

// WriteToCSV writes results to a new csv file
func WriteToCSV(tweets []cluster.Tweet, path string) {
	file, err := os.Create("result.csv")
	if err != nil {
		log.Fatal("Cannot create file", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	for _, tweet := range tweets {
		err := writer.Write(convertTweetToStringSlice(tweet))
		if err != nil {
			log.Fatal("Could not write to file", err)
		}
	}
}

func convertCsvStringToInt(str string) int {
	val, err := strconv.Atoi(str)
	if err != nil {
		panic(fmt.Sprintf("CSV value is not a string %v", str))
	}
	return val
}

func convertTweetToStringSlice(tweet cluster.Tweet) []string {
	var strList []string
	strList = append(strList, strconv.Itoa(tweet.ClusterID))
	strList = append(strList, tweet.NamedEntity)
	strList = append(strList, strconv.Itoa(tweet.TimestampMS))
	strList = append(strList, strconv.Itoa(tweet.TweetID))
	strList = append(strList, tweet.TweetText)
	strList = append(strList, tweet.TweetTokens)
	strList = append(strList, strconv.Itoa(tweet.UserID))
	return strList
}
