package commands

import (
	"fmt"
	"strings"

	"github.com/mohitkumar/mredis/store"
)

type Cmd struct {
	Name string
	Args []string
}

type CommandMeta struct {
	Name      string
	HelpShort string
	Syntax    string
	Eval      func(c *Cmd, s *store.Store) (*CommandResponse, error)
}
type Command struct {
	ClientId string
	C        Cmd
}

type Status int32

const (
	Status_OK  Status = 0
	Status_ERR Status = 1
)

type Response struct {
	Status  Status
	Message string
}

type CommandResponse struct {
	ClientId string
	Response Response
}

func (c *Command) String() string {
	return fmt.Sprintf("%s %s", c.C.Name, strings.Join(c.C.Args, " "))
}

func (c *Command) Key() string {
	if len(c.C.Args) > 0 {
		return c.C.Args[0]
	}
	return ""
}
