package graph

import (
	"fmt"

	mbp "github.com/GoogleCloudPlatform/healthcare-data-harmonization/mapping_engine/proto"
	proto "github.com/golang/protobuf/proto"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

var idFactory IDfactory = &autoIncFactory{currentID: 0}

// setIDfactory sets the interface used for ID generation
func setIDfactory(factory IDfactory) {
	idFactory = factory
}

// env represents the lexical scope a whistler message and its corresponding node belong to.
// args is a list of the parent projector arguments.
// argLookup is a map from whistler projector arguments to the nodes they generate.
type env struct {
	name      string
	parent    *env
	args      []proto.Message
	argLookup map[proto.Message]Node
}

func projectorEnv(parentEnv *env, projector *mbp.ProjectorDefinition, argMsgs []proto.Message, argLookup map[proto.Message]Node) *env {
	return &env{
		name:      projector.GetName(),
		parent:    parentEnv,
		args:      argMsgs,
		argLookup: argLookup,
	}
}

// New uses a whistler MappingConfig to generate a new lineage graph.
func New(mpc *mbp.MappingConfig) (Graph, error) {
	projectors := make(map[string]*mbp.ProjectorDefinition)
	for _, p := range mpc.GetProjector() {
		projectors[p.GetName()] = p
	}

	graph := Graph{
		Edges:         map[IsID][]IsID{},
		ArgumentEdges: map[IsID][]IsID{},
		Nodes:         map[IsID]Node{},
	}
	e := &env{
		name:      "root",
		parent:    nil,
		args:      []proto.Message{},
		argLookup: map[proto.Message]Node{},
	}
	for _, mapping := range mpc.GetRootMapping() {
		if _, err := graph.addMsgLineage(e, mapping, nil, nil, projectors, false); err != nil {
			return Graph{}, fmt.Errorf("adding mapping '%v' failed with:\n%v", mapping, err)
		}
	}
	return graph, nil
}

/*
   TODO:
   addMsgLineage will be refactored in the conditionals PR. Currently, the basis of the graph recursion is whistler messages. Sometimes extra context is
   needed to process a message, like a ProjectorDef's ValueSource for arguments. Currently this context is provided
     1) by the descendantMsg
     2) by an argument lookup argLookup in the environment
   In the refactor, the basis of graph recursion is a whistlerNode, which stores a whistler msg, a projector value source (if relevant), and a
   previously-generated node (if relevant). This adds an additional step where a message is processed into a whistlerNode, but afterwards
   handling of each whistlerNode is more consistent and requires no special treatment to handle additional context.
*/
// addMsgLineage takes a whistler message, converts it to a node, and adds it to the graph. It also recursively adds that node's full lineage to the graph. It also returns the newly created node.
// if isArg is true, then the node being added will be treated as a projector's argument.
func (g Graph) addMsgLineage(e *env, msg proto.Message, descendantNode Node, descendantMsg proto.Message, projectors map[string]*mbp.ProjectorDefinition, isArg bool) (Node, error) {
	node, isNew, err := getNode(msg, descendantNode, e.argLookup)
	if err != nil {
		return nil, fmt.Errorf("failed get node from message %v:\n%w", msg, err)
	}
	if err = addNode(g, node, descendantNode, isArg, isNew); err != nil {
		return nil, fmt.Errorf("failed to add node %v to graph:\n%w", node, err)
	}
	if !isNew {
		return node, nil // the node is not new; it's lineage is already in the graph and can be returned now
	}

	// TODO: getting all ancestors (main ancestors and projector args) will moved to its own function in the conditionals PR
	ancestorEnv := e
	if projector, ok := msg.(*mbp.ProjectorDefinition); ok {
		argMsgs, err := projectorArgs(descendantMsg)
		if err != nil {
			return nil, fmt.Errorf("failed to get projector '%v' arguments:\n%w", projector.GetName(), err)
		}
		ancestorEnv, err = g.addArgLineages(e, argMsgs, node, projector, projectors)
		if err != nil {
			return nil, fmt.Errorf("failed to add lineages for arguments %v:\n%w", argMsgs, err)
		}
	}

	ancestors, err := getAncestors(e, msg, projectors)
	if err != nil {
		return nil, fmt.Errorf("failed to get ancestors for msg %v:\n%w", msg, err)
	}

	// TODO: adding ancestor lineages will be moved to its own function in the conditionals PR
	for _, ancestor := range ancestors {
		_, err := g.addMsgLineage(ancestorEnv, ancestor, node, msg, projectors, false)
		if err != nil {
			return nil, fmt.Errorf("failed to add lineage for message %v:\n%w", ancestor, err)
		}
	}
	return node, nil
}

// addArgLineages adds all the nodes and lineages belonging to argMsgs to the graph as Arguments of a projector.
// Evaluating the arguments of a projector creates a new environment which is returned at the end.
// arguments which are ValueSource_ProjectedValues are handled specially.
func (g Graph) addArgLineages(e *env, argMsgs []proto.Message, projNode Node, projMsg *mbp.ProjectorDefinition, projectors map[string]*mbp.ProjectorDefinition) (*env, error) {
	argLookup := map[proto.Message]Node{}
	for _, argMsg := range argMsgs {
		var argNode Node
		var err error
		if projectedValue, ok := getProjectedValue(argMsg); ok { // the argument is a projected value
			projDef, ok := projectors[projectedValue.GetProjector()] // the corresponding projector definition is the argument
			if !ok {
				return nil, fmt.Errorf("could not find projector belonging to projected value %v", projectedValue)
			}
			argNode, err = g.addMsgLineage(e, projDef, projNode, projectedValue, projectors, true)
		} else {
			argNode, err = g.addMsgLineage(e, argMsg, projNode, projMsg, projectors, true)
		}
		if err != nil {
			return nil, fmt.Errorf("failed to add argument %v lineage:\n%w", argMsg, err)
		}
		argLookup[argMsg] = argNode
	}
	return projectorEnv(e, projMsg, argMsgs, argLookup), nil
}

func getProjectedValue(msg proto.Message) (*mbp.ValueSource, bool) {
	if source, ok := msg.(*mbp.ValueSource); ok {
		if projectedValue, ok := source.GetSource().(*mbp.ValueSource_ProjectedValue); ok {
			return projectedValue.ProjectedValue, true
		}
	}
	return nil, false
}

// getNode returns a node given a whistler message.
// It checks if the node was already created as a projector argument and returns it if so.
// Otherwise, it creates and returns a new node for the message.
// It returns as a boolean whether the node was newly created (true) or not (false)
func getNode(msg proto.Message, descendantNode Node, argLookup map[proto.Message]Node) (Node, bool, error) {
	if _, ok := descendantNode.(*ArgumentNode); ok {
		node, ok := argLookup[msg]
		if !ok {
			return nil, false, fmt.Errorf("expected msg %v to be in argLookup, but it was not", msg)
		}
		return node, false, nil
	} else {
		node, err := newNode(msg)
		return node, true, err
	}
}

func newNode(msg proto.Message) (Node, error) {
	switch m := msg.(type) {
	case *mbp.FieldMapping:
		return targetFieldNode(m)
	case *mbp.ValueSource:
		return valueSourceNode(m)
	case *mbp.ProjectorDefinition:
		return projectorNode(m), nil
	default:
		return nil, fmt.Errorf("interpreting whistler message %v failed; message type %T not supported", msg, msg)
	}
}

func targetFieldNode(msg *mbp.FieldMapping) (*TargetNode, error) {
	switch target := msg.GetTarget().(type) {
	case *mbp.FieldMapping_TargetField:
		return &TargetNode{
			id:   idFactory.New(),
			Name: target.TargetField,
			msg:  msg,
		}, nil
	default:
		return nil, fmt.Errorf("interpreting whistler message %v failed; type %T not supported", target, target)
	}
}

func valueSourceNode(msg *mbp.ValueSource) (Node, error) {
	switch m := msg.GetSource().(type) {
	case *mbp.ValueSource_ConstBool:
		return constBoolNode(m, msg), nil
	case *mbp.ValueSource_ConstInt:
		return constIntNode(m, msg), nil
	case *mbp.ValueSource_ConstFloat:
		return constFloatNode(m, msg), nil
	case *mbp.ValueSource_ConstString:
		return constStringNode(m, msg), nil
	case *mbp.ValueSource_FromInput:
		return fromInputNode(m.FromInput, msg), nil
	case *mbp.ValueSource_ProjectedValue:
		return nil, nil
	default:
		return nil, fmt.Errorf("interpreting whistler message %v failed; type %T not supported", msg, msg)
	}
}

func constBoolNode(msg *mbp.ValueSource_ConstBool, source *mbp.ValueSource) *ConstBoolNode {
	return &ConstBoolNode{
		id:    idFactory.New(),
		Value: msg.ConstBool,
		msg:   source,
	}
}

func constIntNode(msg *mbp.ValueSource_ConstInt, source *mbp.ValueSource) *ConstIntNode {
	return &ConstIntNode{
		id:    idFactory.New(),
		Value: int(msg.ConstInt),
		msg:   source,
	}
}

func constFloatNode(msg *mbp.ValueSource_ConstFloat, source *mbp.ValueSource) *ConstFloatNode {
	return &ConstFloatNode{
		id:    idFactory.New(),
		Value: msg.ConstFloat,
		msg:   source,
	}
}

func constStringNode(msg *mbp.ValueSource_ConstString, source *mbp.ValueSource) *ConstStringNode {
	return &ConstStringNode{
		id:    idFactory.New(),
		Value: msg.ConstString,
		msg:   source,
	}
}

func fromInputNode(msg *mbp.ValueSource_InputSource, source *mbp.ValueSource) Node {
	return &ArgumentNode{
		id:    idFactory.New(),
		Index: int(msg.GetArg()),
		Field: msg.GetField(),
		msg:   source,
	}
}

func projectorNode(msg *mbp.ProjectorDefinition) *ProjectorNode {
	return &ProjectorNode{
		id:   idFactory.New(),
		Name: msg.GetName(),
		msg:  msg,
	}
}

// addNode adds a node to the appropriate graph. All nodes are added to the Nodes list.
// ProjectorDef nodes are added to both Edges and ArgumentEdges adjacency lists.
// Arguments of a projector are added as descendants to their projector only in the ArgumentEdges adjacency list.
// Sub-mappings of a projector are added as descendants to their projector only in the Edges adjacency list.
func addNode(g Graph, node Node, descendant Node, isArg bool, isNew bool) error {
	if isNew { // only make new entries if the node is new
		if _, ok := g.Nodes[node.ID()]; ok {
			return fmt.Errorf("node %v is already in the graph", node)
		}

		g.Nodes[node.ID()] = node
		g.Edges[node.ID()] = []IsID{}

		if _, ok := node.(*ProjectorNode); ok { // projectors can have Argument children
			g.ArgumentEdges[node.ID()] = []IsID{}
		}
	}

	if descendant == nil {
		return nil
	}

	var graphToAppend map[IsID][]IsID
	if isArg {
		graphToAppend = g.ArgumentEdges
	} else {
		graphToAppend = g.Edges
	}

	var ancestorList []IsID
	ancestorList, ok := graphToAppend[descendant.ID()]
	if !ok {
		return fmt.Errorf("expected node %v to have a descendant %v in the graph, but it didn't", node, descendant)
	}
	graphToAppend[descendant.ID()] = append(ancestorList, node.ID())
	if !isArg && isRecursive(g, node) {
		return fmt.Errorf("adding node %v causes a recursive dependency in the graph", node)
	}
	return nil
}

func isRecursive(g Graph, newNode Node) bool {
	previousAppearences := []IsID{}

	for id, node := range g.Nodes {
		if cmp.Diff(node.protoMsg(), newNode.protoMsg(), protocmp.Transform()) == "" {
			previousAppearences = append(previousAppearences, id)
		}
	}
	ancestors := []IsID{}
	for _, id := range previousAppearences {
		ancestors = append(ancestors, g.Edges[id]...)
	}

	for _, startNode := range ancestors {
		if isRecursiveHelper(g, newNode.ID(), startNode) {
			return true
		}
	}
	return false
}

func isRecursiveHelper(g Graph, newNode IsID, currNode IsID) bool {
	if _, ok := g.Nodes[currNode].(*ArgumentNode); ok {
		return false // can't form a cycle through an argument
	}
	if cmp.Diff(g.Nodes[newNode].protoMsg(), g.Nodes[currNode].protoMsg(), protocmp.Transform()) == "" {
		return true
	}
	for _, ancestor := range g.Edges[currNode] {
		if isRecursiveHelper(g, newNode, ancestor) {
			return true
		}
	}
	return false
}

// getAncestors extracts a list of proto message ancestors from a given proto message.
func getAncestors(e *env, msg proto.Message, projectors map[string]*mbp.ProjectorDefinition) ([]proto.Message, error) {
	switch m := msg.(type) {
	case *mbp.FieldMapping:
		ancestors, err := fieldMappingAncestors(m, projectors)
		if err != nil {
			return nil, fmt.Errorf("extracting ancestors from message %v failed with error: %w", msg, err)
		}
		return ancestors, nil
	case *mbp.ValueSource:
		ancestors, err := valueSourceAncestors(e, m)
		if err != nil {
			return nil, fmt.Errorf("extracting ancestors from message %v failed with error: %w", msg, err)
		}
		return ancestors, nil
	case *mbp.ProjectorDefinition:
		ancestors := projectorAncestors(m)
		return ancestors, nil
	default:
		return nil, fmt.Errorf("interpreting whistler message %v failed; type %T not supported", msg, msg)
	}
}

// If a FieldMapping's ValueSource has a projector set, then the ancestor is a ProjectorDefinition and must be
// looked up. Otheriwse it is just the ValueSource.
func fieldMappingAncestors(msg *mbp.FieldMapping, projectors map[string]*mbp.ProjectorDefinition) ([]proto.Message, error) {
	source := msg.GetValueSource()
	if projName := source.GetProjector(); projName != "" {
		projDef, ok := projectors[projName]
		if !ok {
			return nil, fmt.Errorf("projector %v could not be found", projName)
		}
		return []proto.Message{projDef}, nil
	}
	if source == nil {
		return []proto.Message{}, nil
	}
	return []proto.Message{source}, nil
}

func valueSourceAncestors(e *env, msg *mbp.ValueSource) ([]proto.Message, error) {
	switch m := msg.GetSource().(type) {
	case *mbp.ValueSource_ConstBool:
		return nil, nil
	case *mbp.ValueSource_ConstInt:
		return nil, nil
	case *mbp.ValueSource_ConstFloat:
		return nil, nil
	case *mbp.ValueSource_ConstString:
		return nil, nil
	case *mbp.ValueSource_FromInput:
		ancestor, err := argumentAncestor(e, m.FromInput)
		if err != nil {
			return nil, fmt.Errorf("getting ancestors for msg %v failed with error:\n%w", msg, err)
		}
		return []proto.Message{ancestor}, nil
	default:
		return nil, fmt.Errorf("interpreting whistler message %v failed; type %T not supported", m, m)
	}
}

func argumentAncestor(e *env, msg *mbp.ValueSource_InputSource) (proto.Message, error) {
	index := int(msg.Arg) - 1
	if index < 0 || index >= len(e.args) {
		return nil, fmt.Errorf("msg %v requested out-of-bounds argument %v from arguments %v", msg, index, e.args)
	}
	return e.args[index], nil
}

func projectorAncestors(msg *mbp.ProjectorDefinition) []proto.Message {
	ancestors := make([]proto.Message, len(msg.GetMapping()))
	for i, mapping := range msg.GetMapping() {
		ancestors[i] = proto.Message(mapping) // loop over the mappings to convert them to messages
	}
	return ancestors
}

// TODO: In the refactor, the "descendantMsg" is always the projector's valueSource and no switch is needed
//       This is because extra context is immediately stored in a whistlerNode,
//       rather than relying on the descendant message for extra context
// projectorArgs returns the arguments of a projector.
// Because a ProjectorDefinition message does not have the arguments, it uses
// the projector's descendant node. This can be a ValueSource if its descendant
// was a ProjectedValue, or a FieldMapping otherwise.
func projectorArgs(descendantMsg proto.Message) ([]proto.Message, error) {
	var source *mbp.ValueSource
	switch msg := descendantMsg.(type) {
	case *mbp.FieldMapping:
		source = msg.GetValueSource()
	case *mbp.ValueSource:
		if msg.GetProjector() == "" {
			return nil, fmt.Errorf("expected ValueSource message {%v} to have a projector set, but it did not", descendantMsg)
		}
		source = msg
	default:
		return nil, fmt.Errorf("expected message of type FieldMapping or ValueSource, but got %T", descendantMsg)
	}

	if source.GetSource() == nil {
		return []proto.Message{}, nil
	}
	args := make([]proto.Message, len(source.GetAdditionalArg())+1)
	args[0] = source
	for i, arg := range source.GetAdditionalArg() {
		args[i+1] = arg
	}
	return args, nil
}
