package config

type Cluster struct {
	Name    string   `yaml:"name"`
	Address []string `yaml:"address"`
}

type Config struct {
	loadBalancer string     `yaml:"loadBalancer"`
	Cluster      []*Cluster `yaml:"cluster"`
}
