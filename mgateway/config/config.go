package config

type Config struct {
	Routes []RouteConfig `yaml:"routes"`
}

type RouteConfig struct {
	ID          string             `yaml:"id"`
	Method      string             `yaml:"method"`
	Path        string             `yaml:"path"`
	Upstream    UpstreamConfig     `yaml:"upstream"`
	Middlewares []MiddlewareConfig `yaml:"middlewares"`
}

type UpstreamConfig struct {
	Strategy string   `yaml:"strategy"`
	Targets  []string `yaml:"targets"`
}

type MiddlewareConfig struct {
	Name   string         `yaml:"name"`
	Config map[string]any `yaml:"config"`
}
