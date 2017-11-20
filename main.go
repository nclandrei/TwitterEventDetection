package main

import (
	"sort"

	"github.com/nclandrei/TwitterEventDetection/cluster"
	"github.com/nclandrei/TwitterEventDetection/io"
)

func main() {
	tweets := io.ReadFromCSV("clusters.sortedby.clusterid.csv")
	clusters := cluster.CreateClusters(tweets)
	clusters = mergeEventsOnNamedEntities(clusters, 3600000)
	sort.Slice(clusters, func(i, j int) bool {
		return clusters[i].CentroidTime < clusters[j].CentroidTime
	})
	io.WriteToCSV(cluster.ConvertListOfClustersToListOfTweets(clusters), "result.csv")
	// clusters = filterByNumberOfTweets(clusters, 10)
}

//
func filterByNumberOfTweets(clusters []cluster.Cluster, numberOfTweets int) []cluster.Cluster {
	for i := len(clusters) - 1; i >= 0; i-- {
		if len(clusters[i].ClusterTweets) < numberOfTweets {
			clusters = append(clusters[:i], clusters[i+1:]...)
		}
	}
	return clusters
}

//
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
