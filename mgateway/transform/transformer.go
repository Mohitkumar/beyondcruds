package transform

import (
	"net/http"
)

type Part struct {
	Name  string `yaml:"name" json:"name"`
	Value string `yaml:"value" json:"value"`
}

type HeaderTransform struct {
	Add    []Part   `yaml:"add" json:"add"`
	Remove []string `yaml:"remove" json:"remove"`
}

type QueryParamTransform struct {
	Add    []Part   `yaml:"add" json:"add"`
	Remove []string `yaml:"remove" json:"remove"`
}

type RequestTransform struct {
	Header     HeaderTransform     `yaml:"header" json:"header"`
	QueryParam QueryParamTransform `yaml:"query_param" json:"query_param"`
}

type ResponseTransform struct {
	Header HeaderTransform `yaml:"header" json:"header"`
}

type Transform struct {
	Request  RequestTransform  `yaml:"request" json:"request"`
	Response ResponseTransform `yaml:"response" json:"response"`
}

func (t *Transform) ApplyRequest(req *http.Request) {
	for _, part := range t.Request.Header.Add {
		req.Header.Add(part.Name, part.Value)
	}
	for _, name := range t.Request.Header.Remove {
		req.Header.Del(name)
	}
	query := req.URL.Query()
	for _, part := range t.Request.QueryParam.Add {
		query.Add(part.Name, part.Value)
	}
	for _, name := range t.Request.QueryParam.Remove {
		query.Del(name)
	}
	req.URL.RawQuery = query.Encode()
}

func (t *Transform) ApplyResponse(resp http.ResponseWriter) {
	for _, part := range t.Response.Header.Add {
		resp.Header().Add(part.Name, part.Value)
	}
	for _, name := range t.Response.Header.Remove {
		resp.Header().Del(name)
	}
}
