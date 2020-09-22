package graph

import (
	"fmt"
	"strings"

	proto "github.com/golang/protobuf/proto"
)

// Graph is a map-basd adjacency list for storing a lineage graph.
// It contains the main adjacency list, Edges, for normal edges.
// For projector argument edges, it contains the ArgumentEdges adjacency list.
// It also contains a lookup dictionary of all nodes in the graph.
type Graph struct {
	Edges             map[int][]int
	ArgumentEdges     map[int][]int
	ConditionEdges    map[int][]int
	RootAndOutTargets map[string][]int
	Nodes             map[int]Node
	targetLineages    map[int]targetLineage
}

func (g Graph) String() string {
	nodeStrings := make([]string, 0, len(g.Edges)+len(g.ArgumentEdges)+len(g.ConditionEdges)+len(g.RootAndOutTargets)+4)
	nodeStrings = append(nodeStrings, "Primary edges:")
	for nodeID, ancestorIDs := range g.Edges {
		ancestors := make([]Node, len(ancestorIDs))
		for i, ancestorID := range ancestorIDs {
			ancestors[i] = g.Nodes[ancestorID]
		}
		nodeStrings = append(nodeStrings, fmt.Sprintf("\t%v\n\t\t->%v", g.Nodes[nodeID], ancestors))
	}
	nodeStrings = append(nodeStrings, "Argument edges:")
	for nodeID, ancestorIDs := range g.ArgumentEdges {
		ancestors := make([]Node, len(ancestorIDs))
		for i, ancestorID := range ancestorIDs {
			ancestors[i] = g.Nodes[ancestorID]
		}
		nodeStrings = append(nodeStrings, fmt.Sprintf("\t%v\n\t\t->%v", g.Nodes[nodeID], ancestors))
	}
	nodeStrings = append(nodeStrings, "Condition edges:")
	for nodeID, ancestorIDs := range g.ConditionEdges {
		ancestors := make([]Node, len(ancestorIDs))
		for i, ancestorID := range ancestorIDs {
			ancestors[i] = g.Nodes[ancestorID]
		}
		nodeStrings = append(nodeStrings, fmt.Sprintf("\t%v\n\t\t->%v", g.Nodes[nodeID], ancestors))
	}
	nodeStrings = append(nodeStrings, "'root' and 'out' targets:")
	for targetName, nodeIDs := range g.RootAndOutTargets {
		nodeStrings = append(nodeStrings, fmt.Sprintf("%v: %v", targetName, nodeIDs))
	}
	return strings.Join(nodeStrings, "\n")
}

var autoIncID *int

func setIncID(id int) {
	if autoIncID == nil {
		autoIncID = &id
	} else {
		*autoIncID = id
	}
}

func newIncID() int {
	if autoIncID == nil {
		startID := 0
		autoIncID = &startID
	}
	id := *autoIncID
	*autoIncID++
	return id
}

// FileMetaData represents file-specific meta data from whistle or json
type FileMetaData struct {
	FileName  string
	LineStart int
	LineEnd   int
	CharStart int
	CharEnd   int
}

/*
Node represents a node in the lineage graph. Implementations are:
 * TargetNode
 * ConstNodes (ConstBoolNode, ConstStringNode, ConstIntNode, ConstFloatNode)
 * ProjectorNode
 * ArgumentNode
 * RootNode
*/
type Node interface {
	ID() int
	setID(int)
	Equals(Node) bool
	protoMsg() proto.Message
	setProtoMsg(proto.Message)
}

// TargetNode is a node representing a whistler target
type TargetNode struct {
	id          int
	Name        string
	Context     string
	IsVariable  bool
	IsOverwrite bool
	IsRoot      bool
	IsOut       bool
	FileData    FileMetaData
	msg         proto.Message
}

// ID returns the node ID
func (n *TargetNode) ID() int { return n.id }

func (n *TargetNode) setID(id int) { n.id = id }

// Equals returns whether the nodes are equal
func (n *TargetNode) Equals(n2 Node) bool {
	if m, ok := n2.(*TargetNode); ok {
		return *n == *m
	}
	return false
}

func (n *TargetNode) protoMsg() proto.Message { return n.msg }

func (n *TargetNode) setProtoMsg(m proto.Message) { n.msg = m }

// ConstBoolNode is a node representing a whistler constant bool
type ConstBoolNode struct {
	id       int
	Value    bool
	Context  string
	FileData FileMetaData
	msg      proto.Message
}

// ID returns the node ID
func (n *ConstBoolNode) ID() int      { return n.id }
func (n *ConstBoolNode) setID(id int) { n.id = id }

// Equals returns whether the nodes are equal
func (n *ConstBoolNode) Equals(n2 Node) bool {
	if m, ok := n2.(*ConstBoolNode); ok {
		return *n == *m
	}
	return false
}

func (n *ConstBoolNode) protoMsg() proto.Message     { return n.msg }
func (n *ConstBoolNode) setProtoMsg(m proto.Message) { n.msg = m }

// ConstIntNode is a node representing a whistler constant int
type ConstIntNode struct {
	id       int
	Value    int
	Context  string
	FileData FileMetaData
	msg      proto.Message
}

// ID returns the node ID
func (n *ConstIntNode) ID() int      { return n.id }
func (n *ConstIntNode) setID(id int) { n.id = id }

// Equals returns whether the nodes are equal
func (n *ConstIntNode) Equals(n2 Node) bool {
	if m, ok := n2.(*ConstIntNode); ok {
		return *n == *m
	}
	return false
}

func (n *ConstIntNode) protoMsg() proto.Message     { return n.msg }
func (n *ConstIntNode) setProtoMsg(m proto.Message) { n.msg = m }

// ConstFloatNode is a node representing a whistler constant float
type ConstFloatNode struct {
	id       int
	Value    float32
	Context  string
	FileData FileMetaData
	msg      proto.Message
}

// ID returns the node ID
func (n *ConstFloatNode) ID() int      { return n.id }
func (n *ConstFloatNode) setID(id int) { n.id = id }

// Equals returns whether the nodes are equal
func (n *ConstFloatNode) Equals(n2 Node) bool {
	if m, ok := n2.(*ConstFloatNode); ok {
		return *n == *m
	}
	return false
}

func (n *ConstFloatNode) protoMsg() proto.Message     { return n.msg }
func (n *ConstFloatNode) setProtoMsg(m proto.Message) { n.msg = m }

// ConstStringNode is a node representing a whistler constant string
type ConstStringNode struct {
	id       int
	Value    string
	Context  string
	FileData FileMetaData
	msg      proto.Message
}

// ID returns the node ID
func (n *ConstStringNode) ID() int      { return n.id }
func (n *ConstStringNode) setID(id int) { n.id = id }

// Equals returns whether the nodes are equal
func (n *ConstStringNode) Equals(n2 Node) bool {
	if m, ok := n2.(*ConstStringNode); ok {
		return *n == *m
	}
	return false
}

func (n *ConstStringNode) protoMsg() proto.Message     { return n.msg }
func (n *ConstStringNode) setProtoMsg(m proto.Message) { n.msg = m }

// ProjectorNode is a node representing a whistler projector definition
type ProjectorNode struct {
	id        int
	Name      string
	Context   string
	IsBuiltin bool
	FileData  FileMetaData
	msg       proto.Message
}

// ID returns the node ID
func (n *ProjectorNode) ID() int      { return n.id }
func (n *ProjectorNode) setID(id int) { n.id = id }

// Equals returns whether the nodes are equal
func (n *ProjectorNode) Equals(n2 Node) bool {
	if m, ok := n2.(*ProjectorNode); ok {
		return *n == *m
	}
	return false
}

func (n *ProjectorNode) protoMsg() proto.Message     { return n.msg }
func (n *ProjectorNode) setProtoMsg(m proto.Message) { n.msg = m }

// ArgumentNode is a node representing a whistler InputSource for projector arguments
type ArgumentNode struct {
	id       int
	Index    int
	Field    string
	Context  string
	FileData FileMetaData
	msg      proto.Message
}

// ID returns the node ID
func (n *ArgumentNode) ID() int      { return n.id }
func (n *ArgumentNode) setID(id int) { n.id = id }

// Equals returns whether the nodes are equal
func (n *ArgumentNode) Equals(n2 Node) bool {
	if m, ok := n2.(*ArgumentNode); ok {
		return *n == *m
	}
	return false
}

func (n *ArgumentNode) protoMsg() proto.Message     { return n.msg }
func (n *ArgumentNode) setProtoMsg(m proto.Message) { n.msg = m }

type RootNode struct {
	id       int
	Field    string
	Context  string
	FileData FileMetaData
	msg      proto.Message
}

// ID returns the node ID
func (n *RootNode) ID() int { return n.id }

func (n *RootNode) setID(id int) { n.id = id }

func (n *RootNode) protoMsg() proto.Message { return n.msg }

func (n *RootNode) setProtoMsg(m proto.Message) { n.msg = m }

// Equals returns whether the nodes are equal
func (n *RootNode) Equals(n2 Node) bool {
	if m, ok := n2.(*RootNode); ok {
		return *n == *m
	}
	return false
}

func (n *TargetNode) String() string {
	return fmt.Sprintf("%v)   Target: %v", n.ID(), n.Name)
}

func (n *ConstBoolNode) String() string {
	return fmt.Sprintf("%v)   ConstBool: %v", n.ID(), n.Value)
}

func (n *ConstStringNode) String() string {
	return fmt.Sprintf("%v)   ConstString: %v", n.ID(), n.Value)
}

func (n *ConstFloatNode) String() string {
	return fmt.Sprintf("%v)   ConstFloat: %v", n.ID(), n.Value)
}

func (n *ConstIntNode) String() string {
	return fmt.Sprintf("%v)   ConstInt: %v", n.ID(), n.Value)
}

func (n *ProjectorNode) String() string {
	return fmt.Sprintf("%v)   Projector: %v", n.ID(), n.Name)
}

func (n *ArgumentNode) String() string {
	return fmt.Sprintf("%v)   Arg: %v%v", n.ID(), n.Index, n.Field)
}

func (n *RootNode) String() string {
	fieldStr := ""
	if n.Field != "" {
		fieldStr = fmt.Sprintf(": %v", n.Field)
	}
	return fmt.Sprintf("%v)   $Root%v", n.ID(), fieldStr)
}
