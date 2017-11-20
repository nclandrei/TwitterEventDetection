package cluster

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

// CreateClusters creates clusters given a slice of tweets
func CreateClusters(tweets []Tweet) []Cluster {
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

// ConvertClusterMapToSlice takes a map of named entities to lists of clusters and flattens it out
func ConvertClusterMapToSlice(clusterMap map[string][]Cluster) []Cluster {
	var clusterSlice []Cluster
	for _, clusters := range clusterMap {
		clusterSlice = append(clusterSlice, clusters...)
	}
	return clusterSlice
}

// ConvertListOfClustersToListOfTweets converts a list of clusters to a concatenated list of all tweets within
func ConvertListOfClustersToListOfTweets(clusters []Cluster) []Tweet {
	var tweets []Tweet
	for _, cluster := range clusters {
		for _, tweet := range cluster.ClusterTweets {
			tweets = append(tweets, tweet)
		}
	}
	return tweets
}

func contains(clusters []Cluster, clusterID int) bool {
	for _, cluster := range clusters {
		if cluster.ClusterTweets[0].ClusterID == clusterID {
			return true
		}
	}
	return false
}

func getClusterByID(clusters []Cluster, id int) int {
	for index, cluster := range clusters {
		if cluster.ID == id {
			return index
		}
	}
	return -1
}
