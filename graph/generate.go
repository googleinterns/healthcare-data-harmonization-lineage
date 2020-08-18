package graph

import (
	"fmt"
	"strings"

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
// targets is a map of all the targets defined in the environment
// vars is a list of all the local variables defined in the environment
type env struct {
	name    string
	parent  *env
	args    [][]whistlerNode // args, targets, and vars may have many different mappings due to conditions and overwrites
	targets map[string][]Node
	vars    map[string][]Node
}

type ancestorCollection struct {
	mainAncestors []whistlerNode
	projectorArgs [][]whistlerNode
	conditions    []whistlerNode
}

// a protobuf message and all of the context needed to interpret it
type whistlerNode struct {
	msg         proto.Message
	projSource  *mbp.ValueSource
	nodeInGraph Node   // if the node has already been generated in the graph.
	pathInGraph string // if the requested node is actually a previously-generated ancestor of nodeInGraph
}

// todo: getNodes should return multiple nodes

func getBuiltIns() []*mbp.ProjectorDefinition {
	names := []string{"$Div", "$Mod", "$Mul", "$Sub", "$Sum", "$Flatten", "$ListCat", "$ListLen", "$ListOf", "$SortAndTakeTop", "$UnionBy", "$Unique", "$UnnestArrays", "$CurrentTime", "$MultiFormatParseTime", "$ParseTime", "$ParseUnixTime", "$ReformatTime", "$SplitTime", "$Hash", "$IntHash", "$IsNil", "$IsNotNil", "$MergeJSON", "$UUID", "$DebugString", "$Void", "$And", "$Eq", "$Gt", "$GtEq", "$Lt", "$LtEq", "$NEq", "$Not", "$Or", "$MatchesRegex", "$ParseFloat", "$ParseInt", "$SubStr", "$StrCat", "$StrJoin", "$StrSplit", "$ToLower", "$ToUpper"}

	builtIns := make([]*mbp.ProjectorDefinition, len(names))
	for i, name := range names {
		builtIns[i] = &mbp.ProjectorDefinition{
			Name:    name,
			Mapping: []*mbp.FieldMapping{},
		}
	}
	return builtIns
}

// New uses a whistler MappingConfig to generate a new lineage graph.
func New(mpc *mbp.MappingConfig) (Graph, error) {
	projectors := make(map[string]*mbp.ProjectorDefinition)
	for _, p := range mpc.GetProjector() {
		projectors[p.GetName()] = p
	}
	for _, p := range getBuiltIns() {
		projectors[p.GetName()] = p
	}

	graph := Graph{
		Edges:          map[IsID][]IsID{},
		ArgumentEdges:  map[IsID][]IsID{},
		ConditionEdges: map[IsID][]IsID{},
		Nodes:          map[IsID]Node{},
	}
	e := &env{
		name:    "root",
		parent:  nil,
		args:    [][]whistlerNode{},
		targets: map[string][]Node{},
		vars:    map[string][]Node{},
	}
	wstlrNodes := make([]whistlerNode, len(mpc.GetRootMapping()))
	for i, mapping := range mpc.GetRootMapping() {
		wstlrNodes[i] = whistlerNode{msg: mapping}
	}
	if err := graph.addAncestorLineages(ancestorCollection{mainAncestors: wstlrNodes}, e, nil, projectors); err != nil {
		return Graph{}, fmt.Errorf("adding lineages for mapping config {%v} failed:\n%w", mpc, err)
	}
	return graph, nil
}

// addMsgLineage takes a whistler message, converts it to a node, and adds it to the graph. It also recursively adds that node's full lineage to the graph. It also returns the newly created node.
// if isArg is true, then the node being added will be treated as a projector's argument.
func (g Graph) addWhistlerLineage(wstlrNode whistlerNode, wstlrEnv *env, descendantNode Node, isArg bool, isCondition bool, projectors map[string]*mbp.ProjectorDefinition) (Node, error) {
	node, nodeIsNew, err := getNode(wstlrNode, g, len(wstlrEnv.args))
	if err != nil {
		return nil, fmt.Errorf("failed to get a node from msg {%v}:\n%w", wstlrNode.msg, err)
	}
	if err = addNode(g, node, descendantNode, isArg, isCondition, nodeIsNew); err != nil {
		return nil, fmt.Errorf("adding node %v to graph failed:\n%w", node, err)
	}
	if !nodeIsNew {
		return node, nil
	}

	allAncestors, err := getAllAncestors(wstlrNode, wstlrEnv, projectors)
	if err != nil {
		return nil, fmt.Errorf("getting ancestors for msg {%v} failed:\n%w", wstlrNode.msg, err)
	}
	if err = g.addAncestorLineages(allAncestors, wstlrEnv, node, projectors); err != nil {
		return nil, fmt.Errorf("adding lineage for ancestors of msg {%v} failed:\n%w", wstlrNode.msg, err)
	}

	return node, nil
}

func (g Graph) addAncestorLineages(allAncestors ancestorCollection, descendantEnv *env, descendantNode Node, projectors map[string]*mbp.ProjectorDefinition) error {
	ancestorEnv := descendantEnv
	if projNode, ok := descendantNode.(*ProjectorNode); ok { // if this descendant is a projector, then a new environment is made
		var err error
		if ancestorEnv, err = g.addArgLineages(allAncestors.projectorArgs, descendantEnv, projNode, projectors); err != nil {
			return fmt.Errorf("failed to add argument lineages to the graph:\n%w", err)
		}
	}

	if err := g.addConditionLineages(allAncestors.conditions, descendantEnv, descendantNode, projectors); err != nil {
		return fmt.Errorf("failed to add condition lineages to the graph:\n%w", err)
	}

	if err := g.addMainAncestorLineages(allAncestors.mainAncestors, ancestorEnv, descendantNode, projectors); err != nil {
		return fmt.Errorf("failed to add ancestor lineages to the graph:\n%w", err)
	}
	return nil
}

func (g Graph) addArgLineages(argLists [][]whistlerNode, descendantEnv *env, projNode *ProjectorNode, projectors map[string]*mbp.ProjectorDefinition) (*env, error) {
	envArgs := make([][]whistlerNode, len(argLists))
	for i, args := range argLists {
		envArgs[i] = make([]whistlerNode, len(args))
		for j, arg := range args {
			node, err := g.addWhistlerLineage(arg, descendantEnv, projNode, true, false, projectors)
			if err != nil {
				return nil, fmt.Errorf("adding lineage for projector argument {%v} failed:\n%w", arg, err)
			}
			envArgs[i][j] = whistlerNode{
				msg:         arg.msg,
				projSource:  arg.projSource,
				nodeInGraph: node,
			}
		}
	}
	var parentEnv *env
	if strings.HasPrefix(projNode.Name, "$anon_block_") {
		parentEnv = descendantEnv // only remember the parent if in a closure
	}
	return &env{
		name:    projNode.Name,
		parent:  parentEnv,
		args:    envArgs,
		targets: map[string][]Node{},
		vars:    map[string][]Node{},
	}, nil
}

func (g Graph) addConditionLineages(conditions []whistlerNode, descendantEnv *env, descendantNode Node, projectors map[string]*mbp.ProjectorDefinition) error {
	for _, condition := range conditions {
		if _, err := g.addWhistlerLineage(condition, descendantEnv, descendantNode, false, true, projectors); err != nil {
			return fmt.Errorf("adding lineage for condition {%v} failed:\n%w", condition, err)
		}
	}
	return nil
}

func (g Graph) addMainAncestorLineages(ancestors []whistlerNode, newEnv *env, descendantNode Node, projectors map[string]*mbp.ProjectorDefinition) error {
	for _, wstlrNode := range ancestors {
		node, err := g.addWhistlerLineage(wstlrNode, newEnv, descendantNode, false, false, projectors)
		if err != nil {
			return fmt.Errorf("adding lineage for message {%v} failed:\n%w", wstlrNode.msg, err)
		}
		if targetNode, ok := node.(*TargetNode); ok {
			if targetNode.IsVariable {
				appendOrAdd(newEnv.vars, targetNode, targetNode.Name)
			} else {
				appendOrAdd(newEnv.targets, targetNode, targetNode.Name)
			}
		}
	}
	return nil
}

func appendOrAdd(edges map[string][]Node, node Node, name string) {
	if nodeList, ok := edges[name]; ok {
		edges[name] = append(nodeList, node)
	} else {
		edges[name] = []Node{node}
	}
}

func getNode(wstlrNode whistlerNode, graph Graph, numArgs int) (Node, bool, error) {
	if wstlrNode.nodeInGraph == nil {
		node, err := newNode(wstlrNode.msg, numArgs)
		if err != nil {
			return nil, true, fmt.Errorf("making a new for msg {%v} failed:\n%w", wstlrNode.msg, err)
		}
		return node, true, nil
	} else {
		if wstlrNode.pathInGraph == "" {
			return wstlrNode.nodeInGraph, false, nil
		}

		path := strings.Split(wstlrNode.pathInGraph, ".")
		node, err := findNodeInGraph(wstlrNode.nodeInGraph, path, graph)
		if err != nil {
			return nil, false, fmt.Errorf("retrieving previously generated nodes for msg {%v} failed:\n%w", wstlrNode.msg, err)
		}
		return node, false, nil
	}
}

// return previously-generated nodes in the graph based on a path of target names
func findNodeInGraph(startNode Node, path []string, graph Graph) (Node, error) {
	if len(path) == 0 {
		return startNode, nil
	}
	ancestors, ok := graph.Edges[startNode.ID()]
	if !ok {
		return nil, fmt.Errorf("failed to find node with id %v in the graph", startNode.ID())
	}
	for _, ancestorID := range ancestors {
		ancestor, ok := graph.Nodes[ancestorID]
		if !ok {
			return nil, fmt.Errorf("failed to find node with id %v in the graph", startNode.ID())
		}
		if target, ok := ancestor.(*TargetNode); ok {
			matchingNodes := matchUpToDiff(strings.Split(target.Name, "."), path)
			if matchingNodes > 0 {
				return findNodeInGraph(target, path[matchingNodes:], graph)
			}
		} else {
			node, err := findNodeInGraph(ancestor, path, graph)
			if node != nil && err == nil {
				return node, nil
			}
		}
	}
	return nil, fmt.Errorf("could not find %v with path %v in the graph", startNode, path)
}

// matches the entirety of one path against another, returning the length of the shorter path if matched and zero otherwise
func matchUpToDiff(path []string, targetPath []string) int {
	minLen := len(path)
	if len(targetPath) < minLen {
		minLen = len(targetPath)
	}
	for i := 0; i < minLen; i++ {
		if path[i] != targetPath[i] {
			return 0
		}
	}
	return minLen
}

func newNode(msg proto.Message, numArgs int) (Node, error) {
	switch m := msg.(type) {
	case *mbp.FieldMapping:
		return targetFieldNode(m)
	case *mbp.ValueSource:
		return valueSourceNode(m, numArgs)
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
			id:         idFactory.New(),
			Name:       target.TargetField,
			msg:        msg,
			IsVariable: false,
		}, nil
	case *mbp.FieldMapping_TargetLocalVar:
		return &TargetNode{
			id:         idFactory.New(),
			Name:       target.TargetLocalVar,
			msg:        msg,
			IsVariable: true,
		}, nil
	default:
		return nil, fmt.Errorf("interpreting whistler message %v failed; type %T not supported", target, target)
	}
}

func valueSourceNode(msg *mbp.ValueSource, numArgs int) (Node, error) {
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
		return fromInputNode(m.FromInput, msg, numArgs), nil
	default:
		return nil, fmt.Errorf("interpreting whistler message %v failed; type %T not supported", msg, m)
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

func fromInputNode(msg *mbp.ValueSource_InputSource, source *mbp.ValueSource, numArgs int) Node {
	index := int(msg.GetArg())
	if index-1 == numArgs {
		return &RootNode{
			id:    idFactory.New(),
			Field: msg.GetField(),
			msg:   source,
		}
	} else {
		return &ArgumentNode{
			id:    idFactory.New(),
			Index: int(msg.GetArg()),
			Field: msg.GetField(),
			msg:   source,
		}
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
func addNode(g Graph, node Node, descendant Node, isArg bool, isCondition bool, nodeIsNew bool) error {
	if nodeIsNew {
		if _, ok := g.Nodes[node.ID()]; ok {
			return fmt.Errorf("node %v is already in the graph", node)
		}

		g.Nodes[node.ID()] = node
		g.Edges[node.ID()] = []IsID{}

		if _, ok := node.(*ProjectorNode); ok { // projectors can have Argument children
			g.ArgumentEdges[node.ID()] = []IsID{}
		}

		if _, ok := node.(*TargetNode); ok { // targets can have conditions
			g.ConditionEdges[node.ID()] = []IsID{}
		}
	}

	if descendant == nil {
		return nil
	}

	var graphToAppend map[IsID][]IsID
	if isArg {
		graphToAppend = g.ArgumentEdges
	} else if isCondition {
		graphToAppend = g.ConditionEdges
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

func getAllAncestors(wstlrNode whistlerNode, wstlrEnv *env, projectors map[string]*mbp.ProjectorDefinition) (ancestorCollection, error) {
	switch m := wstlrNode.msg.(type) {
	case *mbp.FieldMapping:
		allAncestors, err := fieldMappingAncestors(m, wstlrEnv, projectors)
		if err != nil {
			return ancestorCollection{}, fmt.Errorf("extracting ancestors from message %v failed with error: %w", wstlrNode.msg, err)
		}
		return allAncestors, nil
	case *mbp.ValueSource:
		ancestors, err := valueSourceAncestors(m, wstlrEnv)
		if err != nil {
			return ancestorCollection{}, fmt.Errorf("extracting ancestors from message %v failed with error: %w", wstlrNode.msg, err)
		}
		return ancestorCollection{mainAncestors: ancestors}, nil
	case *mbp.ProjectorDefinition:
		allAncestors, err := projectorAncestors(m, wstlrNode.projSource, wstlrEnv, projectors)
		if err != nil {
			return ancestorCollection{}, fmt.Errorf("failed to get projector ancestors for {%v}:\n%w", m, err)
		}
		return allAncestors, nil
	default:
		return ancestorCollection{}, fmt.Errorf("interpreting whistler message %v failed; type %T not supported", wstlrNode.msg, wstlrNode.msg)
	}
}

func fieldMappingAncestors(msg *mbp.FieldMapping, wstlrEnv *env, projectors map[string]*mbp.ProjectorDefinition) (ancestorCollection, error) {
	source := msg.GetValueSource()
	if source == nil {
		return ancestorCollection{}, fmt.Errorf("expected field mapping {%v} to have a source set", msg)
	}

	mainAncestors, err := whistlerNodesFromValueSource(source, wstlrEnv, true, projectors)
	if err != nil {
		return ancestorCollection{}, fmt.Errorf("failed to make wstlrNode for msg {%v}:\n%w", msg, err)
	}

	conditions, err := fieldMappingConditions(msg, wstlrEnv, projectors)
	if err != nil {
		return ancestorCollection{}, fmt.Errorf("failed to get conditions for msg {%v}:\n%w", msg, err)
	}
	return ancestorCollection{
		mainAncestors: mainAncestors,
		conditions:    conditions,
	}, nil
}

func fieldMappingConditions(msg *mbp.FieldMapping, wstlrEnv *env, projectors map[string]*mbp.ProjectorDefinition) ([]whistlerNode, error) {
	rootCondition := msg.GetCondition()
	if rootCondition == nil {
		return nil, nil
	}
	if rootCondition.GetProjector() == "$And" { // skip the $And node and directly return its ancestors
		conditions, err := projectorArgs(rootCondition, wstlrEnv, projectors) //todo: uhoh? it uses projector args
		if err != nil {
			return nil, fmt.Errorf("failed to get ancestors of the $And condition message {%v}:\n%w", rootCondition, err)
		}
		return flatten(conditions), nil
	} else {
		wstlrNodes, err := whistlerNodesFromValueSource(rootCondition, wstlrEnv, true, projectors)
		if err != nil {
			return nil, fmt.Errorf("failed to make a whistler node from condition {%v}:\n%w", rootCondition, err)
		}
		return wstlrNodes, nil
	}
}

func flatten(wstlrNodeLists [][]whistlerNode) []whistlerNode {
	wstlrNodes := []whistlerNode{}
	for _, wstlrNodeList := range wstlrNodeLists {
		wstlrNodes = append(wstlrNodes, wstlrNodeList...)
	}
	return wstlrNodes
}

func valueSourceAncestors(msg *mbp.ValueSource, e *env) ([]whistlerNode, error) {
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
		ancestors, err := fromInputAncestor(m.FromInput, e)
		if err != nil {
			return nil, fmt.Errorf("getting ancestors for msg %v failed with error:\n%w", msg, err)
		}
		return ancestors, nil
	default:
		return nil, fmt.Errorf("interpreting whistler message %v failed; type %T not supported", m, m)
	}
}

func fromInputAncestor(msg *mbp.ValueSource_InputSource, e *env) ([]whistlerNode, error) {
	index := int(msg.Arg) - 1 // whistler arguments are 1-based
	if index == len(e.args) {
		return nil, nil // this is $root.
	} else {
		return argumentAncestor(msg, e)
	}
}

func argumentAncestor(msg *mbp.ValueSource_InputSource, e *env) ([]whistlerNode, error) {
	index := int(msg.GetArg()) - 1 // whistler arguments are 1-based
	if index < 0 || index >= len(e.args) {
		return nil, fmt.Errorf("msg %v requested out-of-bounds argument %v from arguments %v", msg, index, e.args)
	}
	wstlrNodes := e.args[index]
	for i, wstlrNode := range wstlrNodes {
		wstlrNodes[i] = whistlerNode{
			msg:         wstlrNode.msg,
			projSource:  wstlrNode.projSource,
			nodeInGraph: wstlrNode.nodeInGraph,
			pathInGraph: strings.TrimLeft(msg.GetField(), "."),
		}
	}
	return wstlrNodes, nil
}

func projectorAncestors(msg *mbp.ProjectorDefinition, projValueSource *mbp.ValueSource, wstlrEnv *env, projectors map[string]*mbp.ProjectorDefinition) (ancestorCollection, error) {
	mappings := projectorMappings(msg)
	args, err := projectorArgs(projValueSource, wstlrEnv, projectors)
	if err != nil {
		return ancestorCollection{}, fmt.Errorf("adding arguments for projector {%v} failed:\n%w", msg, err)
	}
	return ancestorCollection{
		mainAncestors: mappings,
		projectorArgs: args,
	}, nil
}

func projectorMappings(msg *mbp.ProjectorDefinition) []whistlerNode {
	ancestors := make([]whistlerNode, len(msg.GetMapping()))
	for i, mapping := range msg.GetMapping() {
		ancestors[i] = whistlerNode{msg: proto.Message(mapping)}
	}
	return ancestors
}

func projectorArgs(projValueSource *mbp.ValueSource, wstlrEnv *env, projectors map[string]*mbp.ProjectorDefinition) ([][]whistlerNode, error) {
	if projValueSource.GetSource() == nil {
		return [][]whistlerNode{}, nil
	}
	args := make([][]whistlerNode, len(projValueSource.GetAdditionalArg())+1)
	var err error
	args[0], err = whistlerNodesFromValueSource(projValueSource, wstlrEnv, false, projectors)
	if err != nil {
		return nil, fmt.Errorf("failed to get whistler node from message %v:\n%w", projValueSource, err)
	}
	for i, arg := range projValueSource.GetAdditionalArg() {
		args[i+1], err = whistlerNodesFromValueSource(arg, wstlrEnv, false, projectors)
		if err != nil {
			return nil, fmt.Errorf("failed to process argument message %v:\n%w", arg, err)
		}
	}
	return args, nil
}

// a ValueSource requires processing (extracting projectors, looking up local & dest targets, etc) before it can be added to the graph.
// This function centralizes the processing.
func whistlerNodesFromValueSource(source *mbp.ValueSource, wstlrEnv *env, fromMapping bool, projectors map[string]*mbp.ProjectorDefinition) ([]whistlerNode, error) {
	if projName := source.GetProjector(); fromMapping && projName != "" {
		projDef, ok := projectors[projName]
		if !ok {
			return nil, fmt.Errorf("projector %v could not be found", projName)
		}
		return []whistlerNode{whistlerNode{
			msg:        projDef,
			projSource: source,
		}}, nil
	}

	switch msg := source.GetSource().(type) {
	case *mbp.ValueSource_FromDestination:
		nodeName, path := splitNodeAndPath(msg.FromDestination)
		if nodeName == "" {
			return nil, fmt.Errorf("No destination provided")
		}
		nodesInGraph, ok := wstlrEnv.targets[nodeName]
		if !ok {
			return nil, fmt.Errorf("failed to find dest target %v in the environment", nodeName)
		}
		wstlrNodes := make([]whistlerNode, len(nodesInGraph))
		for i, nodeInGraph := range nodesInGraph {
			wstlrNodes[i] = whistlerNode{
				msg:         source,
				nodeInGraph: nodeInGraph,
				pathInGraph: path,
			}
		}
		return wstlrNodes, nil
	case *mbp.ValueSource_FromLocalVar:
		nodeName, path := splitNodeAndPath(msg.FromLocalVar)
		if nodeName == "" {
			return nil, fmt.Errorf("No local variable provided")
		}
		nodesInGraph, err := readVarFromEnv(nodeName, wstlrEnv)
		if err != nil {
			return nil, fmt.Errorf("failed to find local variable %v in the environment:\n%w", path, err)
		}
		wstlrNodes := make([]whistlerNode, len(nodesInGraph))
		for i, nodeInGraph := range nodesInGraph {
			wstlrNodes[i] = whistlerNode{
				msg:         source,
				nodeInGraph: nodeInGraph,
				pathInGraph: path,
			}
		}
		return wstlrNodes, nil
	case *mbp.ValueSource_ProjectedValue:
		if msg.ProjectedValue == nil {
			return nil, fmt.Errorf("expected projected value source {%v} to have a projected value, but it did not", msg)
		}

		// in condition else blocks, the argument is always a ProjectedValue even if it has no projector
		if source.GetProjector() == "$Not" && msg.ProjectedValue.GetProjector() == "" {
			return whistlerNodesFromValueSource(msg.ProjectedValue, wstlrEnv, fromMapping, projectors)
		}

		projDef, ok := projectors[msg.ProjectedValue.GetProjector()]
		if !ok {
			return nil, fmt.Errorf("failed to look up projector '%v'", msg.ProjectedValue.GetProjector())
		}
		return []whistlerNode{whistlerNode{
			msg:        projDef,
			projSource: msg.ProjectedValue,
		}}, nil
	default:
		return []whistlerNode{whistlerNode{msg: source}}, nil
	}
}

func splitNodeAndPath(nodePath string) (string, string) {
	targets := strings.Split(nodePath, ".")
	if len(targets) == 0 {
		return "", ""
	}
	node := targets[0]
	path := strings.Join(targets[1:], ".")
	return node, path
}

func readVarFromEnv(varName string, e *env) ([]Node, error) {
	if nodes, ok := e.vars[varName]; ok {
		return nodes, nil
	} else if e.parent != nil {
		return readVarFromEnv(varName, e.parent)
	} else {
		return nil, fmt.Errorf("couldn't find the local variable '%v' in the environment", varName)
	}
}
