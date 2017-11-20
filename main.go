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
	createClusters(tweets)
}

func convertCsvStringToInt(str string) int {
	val, err := strconv.Atoi(str)
	if err != nil {
		panic(fmt.Sprintf("CSV value is not a string %v", str))
	}
	return val
}

func createClusters(tweets []Tweet) []Cluster {
	var clusters []Cluster
	for _, tweet := range tweets {
		if !contains(clusters, tweet.ClusterID) {
			cluster := new(Cluster)
			cluster.ID = tweet.ClusterID
			cluster.ClusterTweets = []Tweet{}
			cluster.ClusterTweets = append(cluster.ClusterTweets, tweet)
			clusters = append(clusters, *cluster)
		} else {
			cluster := getClusterByID(clusters, tweet.ClusterID)
			cluster.ClusterTweets = append(cluster.ClusterTweets, tweet)
		}
	}
	return clusters
}

func getClusterByID(clusters []Cluster, id int) Cluster {
	var cluster Cluster
	for _, tempCluster := range clusters {
		if tempCluster.ID == id {
			cluster = tempCluster
			break
		}
	}
	return cluster
}

func contains(clusters []Cluster, clusterID int) bool {
	for _, cluster := range clusters {
		if cluster.ClusterTweets[0].ClusterID == clusterID {
			return true
		}
	}
	return false
}
