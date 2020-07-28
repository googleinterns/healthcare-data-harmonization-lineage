/*
Package graph provides core lineage graph services. It defines the graph datatype and provides the "New"
function for creating one.
*/
package graph

import (
	"fmt"

	mbp "github.com/GoogleCloudPlatform/healthcare-data-harmonization/mapping_engine/proto"
	proto "github.com/golang/protobuf/proto"
)

var idFactory IDfactory = &autoIncFactory{currentID: 0}

// setIDfactory sets the interface used for ID generation
func setIDfactory(factory IDfactory) {
	idFactory = factory
}

// msgContext is a wrapper around a proto message storing additional context, like parent node.
type msgContext struct {
	msg        proto.Message
	descendant Node
}

func (mc msgContext) String() string {
	return fmt.Sprintf("msg: %v, descendent: %v", mc.msg, mc.descendant)
}

// dfsBuildStack is a stack adapted for building a lineage graph with a DFS algorithm.
// In addition to pop and push, it has reversePush for maintaining target order.
// It is not thread safe and needs to be updated for parallelized optimizations
type dfsBuildStack struct {
	stack []msgContext
}

func (s *dfsBuildStack) push(mc msgContext) {
	if s.stack == nil {
		s.stack = []msgContext{mc}
	} else {
		s.stack = append(s.stack, mc)
	}
}

// reversePush pushes an array of proto messages onto the stack in reverse order. It assigns them all the same context.
// The order is reversed to preserve the ordering of locally-defined whistler targets. For example, an array
// of FieldMappings extracted from a Projector
func (s *dfsBuildStack) reversePush(msgContexts []msgContext) {
	for i := len(msgContexts) - 1; i >= 0; i-- {
		s.push(msgContexts[i])
	}
}

func (s *dfsBuildStack) pop() (msgContext, bool) {
	if s.len() <= 0 {
		return msgContext{}, false
	}
	n := s.len() - 1
	mc := s.stack[n]
	s.stack = s.stack[:n]
	return mc, true
}

func (s *dfsBuildStack) len() int {
	fmt.Printf("getting length!")
	return len(s.stack)
}

func (s *dfsBuildStack) String() string {
	return fmt.Sprintf("%v", s.stack)
}

func makeMsgContexts(msgs []proto.Message, descendant Node) []msgContext {
	msgContexts := make([]msgContext, len(msgs))
	for i, msg := range msgs {
		msgContexts[i] = msgContext{
			msg:        msg,
			descendant: descendant,
		}
	}
	return msgContexts
}

// New constructs lineage graph from a whistler MappingConfig
func New(mpc *mbp.MappingConfig) (*Graph, error) {
	projectors := make(map[string]*mbp.ProjectorDefinition)
	for _, p := range mpc.GetProjector() {
		projectors[p.GetName()] = p
	}

	frontier := &dfsBuildStack{}
	mappings := mpc.GetRootMapping()
	for i := len(mappings) - 1; i >= 0; i-- { // reversed so locally defined targets are well-ordered
		frontier.push(msgContext{
			msg: mappings[i],
		})
	}

	g, err := buildGraphDFS(frontier, projectors)
	if err != nil {
		return nil, fmt.Errorf("constructing lineage graph failed with error;\n\t%w", err)
	}
	return g, nil
}

// buildGraphDFS uses depth first search over whistler messages to build a lineage graph
func buildGraphDFS(frontier *dfsBuildStack, projectors map[string]*mbp.ProjectorDefinition) (*Graph, error) {
	g := Graph{
		Graph: make(map[IsID][]IsID),
		Nodes: make(map[IsID]Node),
	}
	for frontier.len() > 0 {
		var ctx msgContext
		var ok bool
		if ctx, ok = frontier.pop(); !ok {
			return nil, fmt.Errorf("tried to pop from an empty stack while building graph")
		}
		node, err := newNode(ctx.msg)
		if err != nil {
			return nil, fmt.Errorf("constructing new node from msg %v failed with error;\n\t%w", ctx.msg, err)
		}

		ancestors, err := ancestors(ctx.msg, projectors)
		if err != nil {
			return nil, fmt.Errorf("extracting the ancestors of msg %v failed with error;\n\t%w", ctx.msg, err)
		}

		frontier.reversePush(makeMsgContexts(ancestors, node))

		g.Graph[node.ID()] = []IsID{}
		if ctx.descendant != nil {
			g.Graph[ctx.descendant.ID()] = append(g.Graph[ctx.descendant.ID()], node.ID())
		}
		g.Nodes[node.ID()] = node
	}

	return &g, nil
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
		return nil, fmt.Errorf("interpreting whistler message %v failed; message type not supported", m)
	}
}

func targetFieldNode(msg *mbp.FieldMapping) (*TargetNode, error) {
	switch target := msg.GetTarget().(type) {
	case *mbp.FieldMapping_TargetField:
		return &TargetNode{
			id:   idFactory.New(),
			Name: target.TargetField,
		}, nil
	default:
		return nil, fmt.Errorf("interpreting whistler message %v failed; type %T not supported", target, target)
	}
}

func valueSourceNode(msg *mbp.ValueSource) (Node, error) {
	switch m := msg.GetSource().(type) {
	case *mbp.ValueSource_ConstBool:
		return constBoolNode(m), nil
	case *mbp.ValueSource_ConstInt:
		return constIntNode(m), nil
	case *mbp.ValueSource_ConstFloat:
		return constFloatNode(m), nil
	case *mbp.ValueSource_ConstString:
		return constStringNode(m), nil
	case *mbp.ValueSource_FromInput:
		return fromInputNode(m.FromInput), nil
	default:
		return nil, fmt.Errorf("interpreting whistler message %v failed; type %T not supported", m, m)
	}
}

func constBoolNode(msg *mbp.ValueSource_ConstBool) *ConstBoolNode {
	return &ConstBoolNode{
		id:    idFactory.New(),
		Value: msg.ConstBool,
	}
}

func constIntNode(msg *mbp.ValueSource_ConstInt) *ConstIntNode {
	return &ConstIntNode{
		id:    idFactory.New(),
		Value: int(msg.ConstInt),
	}
}

func constFloatNode(msg *mbp.ValueSource_ConstFloat) *ConstFloatNode {
	return &ConstFloatNode{
		id:    idFactory.New(),
		Value: msg.ConstFloat,
	}
}

func constStringNode(msg *mbp.ValueSource_ConstString) *ConstStringNode {
	return &ConstStringNode{
		id:    idFactory.New(),
		Value: msg.ConstString,
	}
}

func fromInputNode(msg *mbp.ValueSource_InputSource) Node {
	return &ArgumentNode{
		id:    idFactory.New(),
		Index: int(msg.GetArg()),
		Field: msg.GetField(),
	}
}

func projectorNode(msg *mbp.ProjectorDefinition) *ProjectorNode {
	return &ProjectorNode{
		id:   idFactory.New(),
		Name: msg.GetName(),
	}
}

// ancestors extracts a list of proto message ancestors from a given proto message.
func ancestors(msg proto.Message, projectors map[string]*mbp.ProjectorDefinition) ([]proto.Message, error) {
	switch m := msg.(type) {
	case *mbp.FieldMapping:
		ancestors, err := fieldMappingAncestors(m, projectors)
		if err != nil {
			return nil, fmt.Errorf("extracting ancestors from message %v failed with error;\n\t%w", msg, err)
		}
		return ancestors, nil
	case *mbp.ValueSource:
		ancestors, err := valueSourceAncestors(m)
		if err != nil {
			return nil, fmt.Errorf("extracting ancestors from message %v failed with error;\n\t%w", msg, err)
		}
		return ancestors, nil
	case *mbp.ProjectorDefinition:
		return projectorAncestors(m), nil
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

// TODO(jakeval): Add support for FromInput and ProjectedValue
func valueSourceAncestors(msg *mbp.ValueSource) ([]proto.Message, error) {
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
		return nil, nil
	default:
		return nil, fmt.Errorf("interpreting whistler message %v failed; type %T not supported", m, m)
	}
}

func projectorAncestors(msg *mbp.ProjectorDefinition) []proto.Message {
	ancestors := make([]proto.Message, len(msg.GetMapping()))
	for i, mapping := range msg.GetMapping() {
		ancestors[i] = proto.Message(mapping) // loop over the mappings to convert them to messages
	}
	return ancestors
}
