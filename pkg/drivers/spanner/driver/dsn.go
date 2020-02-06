package driver

import (
	"fmt"
	"net/url"
	"path"
	"regexp"
)

const Scheme = "spanner"

type DSN struct {
	ProjectName  string
	InstanceName string
	DatabaseName string
}

func (d *DSN) Instance() string {
	return path.Join("projects", d.ProjectName, "instances", d.InstanceName)
}

func (d *DSN) String() string {
	return path.Join("projects", d.ProjectName, "instances", d.InstanceName, "databases", d.DatabaseName)
}

var dnsPathRegexp = regexp.MustCompile(`^/?projects/([^/]+)/instances/([^/]+)/databases/([^/]+)/?$`)

func ParseConnectionURI(uri string) (*DSN, map[string]string, error) {
	u, err := url.Parse(uri)
	var dsn string
	if err != nil {
		return nil, nil, fmt.Errorf("spanner/driver: '%s' is not a valid uri: %w", dsn, err)
	}
	switch u.Scheme {
	case "", Scheme:
		dsn = u.Path
	default:
		return nil, nil, fmt.Errorf("spanner/driver: invalid uri scheme '%s'", u.Scheme)
	}
	match := dnsPathRegexp.FindStringSubmatch(dsn)
	if len(match) == 0 {
		return nil, nil, fmt.Errorf("spanner/driver: invalid uri '%s'; expected projects/P/instances/I/databases/D", u.Path)
	}
	prj, _ := url.PathUnescape(match[1])
	ins, _ := url.PathUnescape(match[2])
	dbn, _ := url.PathUnescape(match[3])
	params := make(map[string]string)
	for k, v := range u.Query() {
		if l := len(v); l > 0 {
			params[k] = v[l-1]
		}
	}
	return &DSN{
		ProjectName:  prj,
		InstanceName: ins,
		DatabaseName: dbn,
	}, params, nil
}
