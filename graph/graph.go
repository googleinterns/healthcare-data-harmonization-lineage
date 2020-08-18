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
	Edges         map[IsID][]IsID
	ArgumentEdges map[IsID][]IsID
	Nodes         map[IsID]Node
}

func (g Graph) String() string {
	nodeStrings := make([]string, 0, len(g.Edges)+len(g.ArgumentEdges)+2)

	nodeStrings = append(nodeStrings, "Primary edges:")
	for nodeID, ancestorIDs := range g.Edges {
		ancestors := make([]Node, len(ancestorIDs))
		for i, ancestorID := range ancestorIDs {
			ancestors[i] = g.Nodes[ancestorID]
		}
		nodeStrings = append(nodeStrings, fmt.Sprintf("%v\n\t->%v", g.Nodes[nodeID], ancestors))
	}
	nodeStrings = append(nodeStrings, "Argument edges:")
	for nodeID, ancestorIDs := range g.ArgumentEdges {
		ancestors := make([]Node, len(ancestorIDs))
		for i, ancestorID := range ancestorIDs {
			ancestors[i] = g.Nodes[ancestorID]
		}
		nodeStrings = append(nodeStrings, fmt.Sprintf("%v\n\t->%v", g.Nodes[nodeID], ancestors))
	}
	return strings.Join(nodeStrings, "\n")
}

// IDfactory is an interface which creates new node IDs
type IDfactory interface {
	New() IsID
}

// IsID represents any type which can function as a node ID
type IsID interface {
	isID()
}

// autoIncFactory is a factory for an auto-incrementing intID
// it is not thread safe and needs to be updated for parallelized optimizations
type autoIncFactory struct {
	currentID int
}

// New creates a new ID
func (id *autoIncFactory) New() IsID {
	newID := id.currentID
	id.currentID++
	return intID(newID)
}

// intID is an integer-based ID
type intID int

func (intID) isID() {}

// Range represents a range over integers
type Range struct {
	Start int
	End   int
}

/*
Node represents a node in the lineage graph. Implementations are:
 * TargetNode
 * ConstNodes (ConstBoolNode, ConstStringNode, ConstIntNode, ConstFloatNode)
 * ProjectorNode
 * ArgumentNode
*/
type Node interface {
	ID() IsID
	setID(IsID)
	Equals(Node) bool
	protoMsg() proto.Message
	setProtoMsg(proto.Message)
}

// TargetNode is a node representing a whistler target
type TargetNode struct {
	id          IsID
	Name        string
	Context     string
	IsVariable  bool
	IsOverwrite bool
	IsRoot      bool
	IsOut       bool
	FileName    string
	LineRange   Range
	CharRange   Range
	msg         proto.Message
}

// ID returns the node ID
func (n *TargetNode) ID() IsID { return n.id }

func (n *TargetNode) setID(id IsID) { n.id = id }

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
	id        IsID
	Value     bool
	Context   string
	FileName  string
	LineRange Range
	CharRange Range
	msg       proto.Message
}

// ID returns the node ID
func (n *ConstBoolNode) ID() IsID      { return n.id }
func (n *ConstBoolNode) setID(id IsID) { n.id = id }

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
	id        IsID
	Value     int
	Context   string
	FileName  string
	LineRange Range
	CharRange Range
	msg       proto.Message
}

// ID returns the node ID
func (n *ConstIntNode) ID() IsID      { return n.id }
func (n *ConstIntNode) setID(id IsID) { n.id = id }

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
	id        IsID
	Value     float32
	Context   string
	FileName  string
	LineRange Range
	CharRange Range
	msg       proto.Message
}

// ID returns the node ID
func (n *ConstFloatNode) ID() IsID      { return n.id }
func (n *ConstFloatNode) setID(id IsID) { n.id = id }

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
	id        IsID
	Value     string
	Context   string
	FileName  string
	LineRange Range
	CharRange Range
	msg       proto.Message
}

// ID returns the node ID
func (n *ConstStringNode) ID() IsID      { return n.id }
func (n *ConstStringNode) setID(id IsID) { n.id = id }

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
	id        IsID
	Name      string
	Context   string
	FileName  string
	LineRange Range
	CharRange Range
	msg       proto.Message
}

// ID returns the node ID
func (n *ProjectorNode) ID() IsID      { return n.id }
func (n *ProjectorNode) setID(id IsID) { n.id = id }

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
	id        IsID
	Index     int
	Field     string
	Context   string
	FileName  string
	LineRange Range
	CharRange Range
	msg       proto.Message
}

// ID returns the node ID
func (n *ArgumentNode) ID() IsID      { return n.id }
func (n *ArgumentNode) setID(id IsID) { n.id = id }

// Equals returns whether the nodes are equal
func (n *ArgumentNode) Equals(n2 Node) bool {
	if m, ok := n2.(*ArgumentNode); ok {
		return *n == *m
	}
	return false
}

func (n *ArgumentNode) protoMsg() proto.Message     { return n.msg }
func (n *ArgumentNode) setProtoMsg(m proto.Message) { n.msg = m }

func (n *TargetNode) String() string {
	return fmt.Sprintf("(%v)Target: %v", n.ID(), n.Name)
}

func (n *ConstBoolNode) String() string {
	return fmt.Sprintf("(%v)ConstBool: %v", n.ID(), n.Value)
}

func (n *ConstStringNode) String() string {
	return fmt.Sprintf("(%v)ConstString: %v", n.ID(), n.Value)
}

func (n *ConstFloatNode) String() string {
	return fmt.Sprintf("(%v)ConstFloat: %v", n.ID(), n.Value)
}

func (n *ConstIntNode) String() string {
	return fmt.Sprintf("(%v)ConstInt: %v", n.ID(), n.Value)
}

func (n *ProjectorNode) String() string {
	return fmt.Sprintf("(%v)Projector: %v", n.ID(), n.Name)
}

func (n *ArgumentNode) String() string {
	return fmt.Sprintf("(%v)Arg: %v%v", n.ID(), n.Index, n.Field)
}
