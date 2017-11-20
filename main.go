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
	ID            int
	ClusterTweets []Tweet
}

// AddClusterTweet adds tweet to that cluster
func (c *Cluster) AddClusterTweet(tweet Tweet) {
	c.ClusterTweets = append(c.ClusterTweets, tweet)
}

// Tweet defines the structure of a tweet
type Tweet struct {
	ClusterID   int
	NamedEntity string
	TweetID     int
	UserID      int
	TimestampMS int
	TweetTokens string
	TweetText   string
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
			ClusterID:   convertCsvStringToInt(line[0]),
			NamedEntity: line[1],
			TweetID:     convertCsvStringToInt(line[2]),
			TimestampMS: convertCsvStringToInt(line[3]),
			UserID:      convertCsvStringToInt(line[4]),
			TweetTokens: line[5],
			TweetText:   line[6],
		})
	}
	clusters := createClusters(tweets)
	clusters = filterByNumberOfTweets(clusters, 10)
	fmt.Printf("%v\n", len(clusters))
}

func convertCsvStringToInt(str string) int {
	val, err := strconv.Atoi(str)
	if err != nil {
		panic(fmt.Sprintf("CSV value is not a string %v", str))
	}
	return val
}

func createClusters(tweets []Tweet) []Cluster {
	clusters := []Cluster{}
	for _, tweet := range tweets {
		if len(clusters) == 0 || !contains(clusters, tweet.ClusterID) {
			cluster := Cluster{
				ID:            tweet.ClusterID,
				ClusterTweets: []Tweet{},
			}
			cluster.AddClusterTweet(tweet)
			clusters = append(clusters, cluster)
		} else {
			clusterIndex := getClusterByID(clusters, tweet.ClusterID)
			clusters[clusterIndex].AddClusterTweet(tweet)
		}
	}
	return clusters
}

func getClusterByID(clusters []Cluster, id int) int {
	for index, cluster := range clusters {
		if cluster.ID == id {
			return index
		}
	}
	return -1
}

func contains(clusters []Cluster, clusterID int) bool {
	for _, cluster := range clusters {
		if cluster.ClusterTweets[0].ClusterID == clusterID {
			return true
		}
	}
	return false
}

func filterByNumberOfTweets(clusters []Cluster, numberOfTweets int) []Cluster {
	for i := len(clusters) - 1; i >= 0; i-- {
		if len(clusters[i].ClusterTweets) < numberOfTweets {
			clusters = append(clusters[:i], clusters[i+1:]...)
		}
	}
	return clusters
}
