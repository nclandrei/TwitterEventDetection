package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
)

// Cluster defines the structure of a cluster composed of multiple tweets
type Cluster struct {
	ID            int
	CentroidTime  float64
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

// AddClusterTweet adds tweet to that cluster
func (c *Cluster) AddClusterTweet(tweet Tweet) {
	c.ClusterTweets = append(c.ClusterTweets, tweet)
}

// AddAllClusterTweets adds all tweets from one cluster into another
func (c *Cluster) AddAllClusterTweets(cluster Cluster) {
	for _, tweet := range cluster.ClusterTweets {
		c.AddClusterTweet(tweet)
	}
}

// SetCentroidTime setter for centroidTime property
func (c *Cluster) SetCentroidTime(centroidTime float64) {
	c.CentroidTime = centroidTime
}

// ComputeCentroidTime computes the average timestamp for a cluster
func (c Cluster) ComputeCentroidTime() float64 {
	sum := 0
	for _, tweet := range c.ClusterTweets {
		sum += tweet.TimestampMS
	}
	return (float64)(sum / len(c.ClusterTweets))
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
	clusters = mergeEventsOnNamedEntities(clusters, 3600000)
	sort.Slice(clusters, func(i, j int) bool {
		return clusters[i].CentroidTime < clusters[j].CentroidTime
	})
	// clusters = filterByNumberOfTweets(clusters, 10)
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
	for _, cluster := range clusters {
		cluster.SetCentroidTime(cluster.ComputeCentroidTime())
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

func mergeEventsOnNamedEntities(clusters []Cluster, windowInterval int) []Cluster {
	clusterMap := make(map[string][]Cluster)
	for _, cluster := range clusters {
		namedEntity := cluster.ClusterTweets[0].NamedEntity
		if clusterMap[namedEntity] == nil {
			clusterMap[namedEntity] = append(clusterMap[namedEntity], cluster)
		}
		prevClusters := clusterMap[namedEntity]
		for _, prevCluster := range prevClusters {
			if (cluster.CentroidTime - prevCluster.CentroidTime) > (float64)(windowInterval) {
				prevClusters = append(prevClusters, cluster)
			}
			if namedEntity == prevCluster.ClusterTweets[0].NamedEntity {
				cluster.AddAllClusterTweets(prevCluster)
			}

		}
		clusterMap[namedEntity] = prevClusters
	}
	return convertClusterMapToSlice(clusterMap)
}

func convertClusterMapToSlice(clusterMap map[string][]Cluster) []Cluster {
	var clusterSlice []Cluster
	for _, clusters := range clusterMap {
		clusterSlice = append(clusterSlice, clusters...)
	}
	return clusterSlice
}
