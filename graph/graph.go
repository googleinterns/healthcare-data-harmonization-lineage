package graph

import (
	"fmt"
	"strings"
)

// Graph is a map-basd adjacency list for storing a lineage graph
type Graph struct {
	Graph map[IsID][]IsID
	Nodes map[IsID]Node
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
	Equals(Node) bool
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
}

// ID returns the node ID
func (n *TargetNode) ID() IsID { return n.id }

// Equals returns whether the nodes are equal
func (n *TargetNode) Equals(n2 Node) bool {
	if m, ok := n2.(*TargetNode); ok {
		return *n == *m
	}
	return false
}

// ConstBoolNode is a node representing a whistler constant bool
type ConstBoolNode struct {
	id        IsID
	Value     bool
	Context   string
	FileName  string
	LineRange Range
	CharRange Range
}

// ID returns the node ID
func (n *ConstBoolNode) ID() IsID { return n.id }

// Equals returns whether the nodes are equal
func (n *ConstBoolNode) Equals(n2 Node) bool {
	if m, ok := n2.(*ConstBoolNode); ok {
		return *n == *m
	}
	return false
}

// ConstIntNode is a node representing a whistler constant int
type ConstIntNode struct {
	id        IsID
	Value     int
	Context   string
	FileName  string
	LineRange Range
	CharRange Range
}

// ID returns the node ID
func (n *ConstIntNode) ID() IsID { return n.id }

// Equals returns whether the nodes are equal
func (n *ConstIntNode) Equals(n2 Node) bool {
	if m, ok := n2.(*ConstIntNode); ok {
		return *n == *m
	}
	return false
}

// ConstFloatNode is a node representing a whistler constant float
type ConstFloatNode struct {
	id        IsID
	Value     float32
	Context   string
	FileName  string
	LineRange Range
	CharRange Range
}

// ID returns the node ID
func (n *ConstFloatNode) ID() IsID { return n.id }

// Equals returns whether the nodes are equal
func (n *ConstFloatNode) Equals(n2 Node) bool {
	if m, ok := n2.(*ConstFloatNode); ok {
		return *n == *m
	}
	return false
}

// ConstStringNode is a node representing a whistler constant string
type ConstStringNode struct {
	id        IsID
	Value     string
	Context   string
	FileName  string
	LineRange Range
	CharRange Range
}

// ID returns the node ID
func (n *ConstStringNode) ID() IsID { return n.id }

// Equals returns whether the nodes are equal
func (n *ConstStringNode) Equals(n2 Node) bool {
	if m, ok := n2.(*ConstStringNode); ok {
		return *n == *m
	}
	return false
}

// ProjectorNode is a node representing a whistler projector definition
type ProjectorNode struct {
	id        IsID
	Name      string
	Context   string
	FileName  string
	LineRange Range
	CharRange Range
}

// ID returns the node ID
func (n *ProjectorNode) ID() IsID { return n.id }

// Equals returns whether the nodes are equal
func (n *ProjectorNode) Equals(n2 Node) bool {
	if m, ok := n2.(*ProjectorNode); ok {
		return *n == *m
	}
	return false
}

// ArgumentNode is a node representing a whistler InputSource for projector arguments
type ArgumentNode struct {
	id        IsID
	Index     int
	Field     string
	Context   string
	FileName  string
	LineRange Range
	CharRange Range
}

// ID returns the node ID
func (n *ArgumentNode) ID() IsID { return n.id }

// Equals returns whether the nodes are equal
func (n *ArgumentNode) Equals(n2 Node) bool {
	if m, ok := n2.(*ArgumentNode); ok {
		return *n == *m
	}
	return false
}

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

func (g Graph) String() string {
	nodeStrings := make([]string, len(g.Graph))
	i := 0
	for nodeID, ancestorIDs := range g.Graph {
		ancestors := make([]Node, len(ancestorIDs))
		for i, ancestorID := range ancestorIDs {
			ancestors[i] = g.Nodes[ancestorID]
		}
		nodeStrings[i] = fmt.Sprintf("%v\n\t->%v", g.Nodes[nodeID], ancestors)
		i++
	}
	return strings.Join(nodeStrings, "\n")
}
