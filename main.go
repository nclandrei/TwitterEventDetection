package main

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strconv"

	"github.com/nclandrei/TwitterEventDetection/cluster"
	"github.com/nclandrei/TwitterEventDetection/io"
)

func main() {
	fmt.Println("Welcome! Please type 1 for filtering technique and 2 for merging technique: ")
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	option := scanner.Text()
	fmt.Printf("%s\n", option)
	if option == "1" {
		fmt.Println("Please input source CSV file (needs to be in the same folder as this Go file: ")
		scanner.Scan()
		sourceCSVFile := scanner.Text()
		fmt.Println("Please input number of tweets threshold: ")
		scanner.Scan()
		numberOfTweetsStr := scanner.Text()
		numberOfTweetsInt, _ := strconv.Atoi(numberOfTweetsStr)
		tweets := io.ReadFromCSV(sourceCSVFile)
		clusters := cluster.CreateClusters(tweets)
		clusters = filterByNumberOfTweets(clusters, numberOfTweetsInt)
		io.WriteToCSV(cluster.ConvertListOfClustersToListOfTweets(clusters), "result.csv")
		fmt.Println("All done. Your output CSV file is in result.csv")
	} else if option == "2" {
		fmt.Println("Please input source CSV file (needs to be in the same folder as this Go file: ")
		scanner.Scan()
		sourceCSVFile := scanner.Text()
		fmt.Println("Please input number of tweets threshold: ")
		scanner.Scan()
		numberOfTweetsStr := scanner.Text()
		numberOfTweetsInt, _ := strconv.Atoi(numberOfTweetsStr)
		tweets := io.ReadFromCSV(sourceCSVFile)
		clusters := cluster.CreateClusters(tweets)
		clusters = filterByNumberOfTweets(clusters, numberOfTweetsInt)
		fmt.Println("Please input your window interval in milliseconds: ")
		scanner.Scan()
		windowInterval := scanner.Text()
		windowIntervalInt, _ := strconv.Atoi(windowInterval)
		clusters = mergeEventsOnNamedEntities(clusters, windowIntervalInt)
		sort.Slice(clusters, func(i, j int) bool {
			return clusters[i].CentroidTime < clusters[j].CentroidTime
		})
		io.WriteToCSV(cluster.ConvertListOfClustersToListOfTweets(clusters), "result.csv")
		fmt.Println("All done. Your output CSV file is in result.csv")
	} else {
		fmt.Println("You did not enter neither 1 nor 2. Please try again.")
	}
}

// filterByNumberOfTweets takes a slice of clusters and numberOfTweets threshold
// and drops all clusters that do not have at least numberOfTweets tweets
func filterByNumberOfTweets(clusters []cluster.Cluster, numberOfTweets int) []cluster.Cluster {
	for i := len(clusters) - 1; i >= 0; i-- {
		if len(clusters[i].ClusterTweets) < numberOfTweets {
			clusters = append(clusters[:i], clusters[i+1:]...)
		}
	}
	return clusters
}

// mergeEventsOnNamedEntities takes a slice of clusters and a window interval
// and performs event merging using that interval for burst detection
func mergeEventsOnNamedEntities(clusters []cluster.Cluster, windowInterval int) []cluster.Cluster {
	clusterMap := make(map[string][]cluster.Cluster)
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
	return cluster.ConvertClusterMapToSlice(clusterMap)
}
