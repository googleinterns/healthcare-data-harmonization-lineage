package graph

import (
	"fmt"
	"strings"

	"github.com/GoogleCloudPlatform/healthcare-data-harmonization/mapping_engine/builtins"
	mbp "github.com/GoogleCloudPlatform/healthcare-data-harmonization/mapping_engine/proto"
	proto "github.com/golang/protobuf/proto"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

const anon_prefix = "$anon_block_"
const and_keyword = "$And"

// env represents the lexical scope a whistler message and its corresponding node belong to.
// args is a list of the parent projector arguments.
// targets is a map of all the targets defined in the environment
// vars is a list of all the local variables defined in the environment
type env struct {
	name    string
	parent  *env
	args    [][]argLineage // args, targets, and vars may have many multiple mappings due to conditions and overwrites
	targets map[string][]targetLineage
	vars    map[string][]targetLineage
}

// ancestorCollection is a composition containing lists of ancestors a whistler message can generate
type ancestorCollection struct {
	mainAncestors []whistlerNode
	projectorArgs [][]whistlerNode
	conditions    []whistlerNode
}

// a protobuf message and all of the context needed to interpret it
type whistlerNode struct {
	msg         proto.Message
	projSource  *mbp.ValueSource
	nodeInGraph Node // if the node has already been generated in the graph.
}

// targetLineage stores a node and any targets it has as ancestors
// this struct is used for finding a node path like "x.y.z" in the graph
type targetLineage struct {
	node         *TargetNode
	childTargets map[string][]targetLineage
}

// argLineage is a relaxed version of targetLineage; it allows a non-target entry point into a target lineage graph.
// this is useful for handling fields of arguments where the argument is a projector.
type argLineage struct {
	node         Node
	childTargets map[string][]targetLineage
}

// New uses a whistler MappingConfig to generate a new lineage graph.
func New(mpc *mbp.MappingConfig) (Graph, error) {
	projectors := make(map[string]*mbp.ProjectorDefinition)
	for _, p := range mpc.GetProjector() {
		projectors[p.GetName()] = p
	}
	for name, _ := range builtins.BuiltinFunctions {
		projectors[name] = &mbp.ProjectorDefinition{
			Name:    name,
			Mapping: []*mbp.FieldMapping{},
		}
	}

	graph := Graph{
		Edges:             map[int][]int{},
		ArgumentEdges:     map[int][]int{},
		ConditionEdges:    map[int][]int{},
		RootAndOutTargets: map[string][]int{},
		Nodes:             map[int]Node{},
		targetLineages:    map[int]targetLineage{},
	}
	e := &env{
		name:    "root",
		parent:  nil,
		args:    [][]argLineage{},
		targets: map[string][]targetLineage{},
		vars:    map[string][]targetLineage{},
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

// addWhistlerLineage takes a whistler message, converts it to a node, and adds it to the graph. It also recursively adds that node's full lineage to the graph. It also returns the newly created node.
// if isArg is true, then the node being added will be treated as a projector's argument.
// if isCondition is true, then the node being added will be added to the graph as a condition
func (g Graph) addWhistlerLineage(wstlrNode whistlerNode, wstlrEnv *env, descendantNode Node, isArg bool, isCondition bool, projectors map[string]*mbp.ProjectorDefinition) (Node, error) {
	node, nodeIsNew, err := getNode(wstlrNode, wstlrEnv)
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

// adds projector arguments and their lineages to the graph and returns the new projector environment they create
func (g Graph) addArgLineages(argLists [][]whistlerNode, descendantEnv *env, projNode *ProjectorNode, projectors map[string]*mbp.ProjectorDefinition) (*env, error) {
	envArgs := make([][]argLineage, len(argLists))
	for i, args := range argLists {
		envArgs[i] = make([]argLineage, len(args))
		for j, arg := range args {
			node, err := g.addWhistlerLineage(arg, descendantEnv, projNode, true, false, projectors)
			if err != nil {
				return nil, fmt.Errorf("adding lineage for projector argument {%v} failed:\n%w", arg, err)
			}
			var childTargets map[string][]targetLineage
			if target, ok := node.(*TargetNode); ok {
				l, ok := g.targetLineages[target.ID()]
				if !ok {
					return nil, fmt.Errorf("the target node %v should have a lineage in graph.targetLineages", target)
				}
				childTargets = l.childTargets
			}
			if argProj, ok := node.(*ProjectorNode); ok {
				targetIDs, ok := g.Edges[argProj.ID()]
				if !ok {
					return nil, fmt.Errorf("the node %v was not found in the graph", argProj)
				}
				childTargets = map[string][]targetLineage{}
				for _, targetID := range targetIDs {
					lineage, ok := g.targetLineages[targetID]
					if !ok {
						return nil, fmt.Errorf("the target node %v should have a lineage in graph.targetLineages", targetID)
					}
					appendOrAddTargetLineage(childTargets, lineage, lineage.node.Name)
				}
			}
			envArgs[i][j] = argLineage{
				node:         node,
				childTargets: childTargets,
			}
		}
	}
	var parentEnv *env
	if strings.HasPrefix(projNode.Name, anon_prefix) {
		parentEnv = descendantEnv // only remember the parent if in a closure
	}
	return &env{
		name:    projNode.Name,
		parent:  parentEnv,
		args:    envArgs,
		targets: map[string][]targetLineage{},
		vars:    map[string][]targetLineage{},
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
			lineage, err := targetLineageFromGraph(targetNode, g)
			if err != nil {
				return fmt.Errorf("failed to generate lineage for target %v:\n%w", targetNode, err)
			}
			g.targetLineages[targetNode.ID()] = lineage // cache the target's targetLineage in the graph
			if targetNode.IsVariable {
				appendOrAddTargetLineage(newEnv.vars, lineage, targetNode.Name)
			} else {
				appendOrAddTargetLineage(newEnv.targets, lineage, targetNode.Name)
			}
			if targetNode.IsOut || targetNode.IsRoot {
				appendOrAddID(g.RootAndOutTargets, targetNode.ID(), targetNode.Name)
			}
		}
	}
	return nil
}

func targetLineageFromGraph(node *TargetNode, g Graph) (targetLineage, error) {
	lineage := targetLineage{
		node:         node,
		childTargets: map[string][]targetLineage{},
	}
	if err := writeTargetLineage(node, &lineage, g); err != nil {
		return targetLineage{}, err
	}
	return lineage, nil
}

// writeTargetLineage finds a target's children targets (and their lineages).
// it looks through the graph for targets that are immediate ancestors of the node
// and adds the ancestors' lineages to the node's
func writeTargetLineage(node Node, lineage *targetLineage, g Graph) error {
	idList, ok := g.Edges[node.ID()]
	if !ok {
		return fmt.Errorf("couldn't find node %v in the graph", node)
	}

	for _, ancestorID := range idList {
		ancestor, ok := g.Nodes[ancestorID]
		if !ok {
			return fmt.Errorf("couldn't find ancestor %v in the graph", ancestor)
		}
		if targetNode, ok := ancestor.(*TargetNode); ok {
			childLineage, ok := g.targetLineages[targetNode.ID()]
			if !ok {
				return fmt.Errorf("found a target {%v} in the lineage with no targetLineage associated; it should already have been generated", targetNode)
			}
			appendOrAddTargetLineage(lineage.childTargets, childLineage, targetNode.Name)
		} else {
			if err := writeTargetLineage(ancestor, lineage, g); err != nil {
				return err
			}
		}
	}
	return nil
}

func appendOrAddTargetLineage(childTargets map[string][]targetLineage, lineage targetLineage, name string) {
	if childLineage, ok := childTargets[name]; ok {
		childTargets[name] = append(childLineage, lineage)
	} else {
		childTargets[name] = []targetLineage{lineage}
	}
}

func appendOrAddID(idLists map[string][]int, id int, name string) {
	if idList, ok := idLists[name]; ok {
		idLists[name] = append(idList, id)
	} else {
		idLists[name] = []int{id}
	}
}

func getNode(wstlrNode whistlerNode, wstlrEnv *env) (Node, bool, error) {
	if wstlrNode.nodeInGraph == nil {
		node, err := newNode(wstlrNode.msg, wstlrEnv)
		if err != nil {
			return nil, true, fmt.Errorf("making a new for msg {%v} failed:\n%w", wstlrNode.msg, err)
		}
		return node, true, nil
	} else {
		return wstlrNode.nodeInGraph, false, nil
	}
}

func newNode(msg proto.Message, wstlrEnv *env) (Node, error) {
	switch m := msg.(type) {
	case *mbp.FieldMapping:
		return targetNode(m, wstlrEnv)
	case *mbp.ValueSource:
		return valueSourceNode(m, wstlrEnv)
	case *mbp.ProjectorDefinition:
		return projectorNode(m, wstlrEnv), nil
	default:
		return nil, fmt.Errorf("interpreting whistler message %v failed; message type %T not supported", msg, msg)
	}
}

func targetNode(msg *mbp.FieldMapping, wstlrEnv *env) (*TargetNode, error) {
	switch target := msg.GetTarget().(type) {
	case *mbp.FieldMapping_TargetField:
		return &TargetNode{
			id:      newIncID(),
			Name:    target.TargetField,
			Context: wstlrEnv.name,
			msg:     msg,
		}, nil
	case *mbp.FieldMapping_TargetLocalVar:
		return &TargetNode{
			id:         newIncID(),
			Name:       target.TargetLocalVar,
			msg:        msg,
			Context:    wstlrEnv.name,
			IsVariable: true,
		}, nil
	case *mbp.FieldMapping_TargetRootField:
		return &TargetNode{
			id:      newIncID(),
			Name:    target.TargetRootField,
			msg:     msg,
			Context: wstlrEnv.name,
			IsRoot:  true,
		}, nil
	case *mbp.FieldMapping_TargetObject:
		return &TargetNode{
			id:      newIncID(),
			Name:    target.TargetObject,
			msg:     msg,
			Context: wstlrEnv.name,
			IsOut:   true,
		}, nil
	default:
		return nil, fmt.Errorf("interpreting whistler message %v failed; type %T not supported", target, target)
	}
}

func valueSourceNode(msg *mbp.ValueSource, wstlrEnv *env) (Node, error) {
	switch m := msg.GetSource().(type) {
	case *mbp.ValueSource_ConstBool:
		return constBoolNode(m, msg, wstlrEnv), nil
	case *mbp.ValueSource_ConstInt:
		return constIntNode(m, msg, wstlrEnv), nil
	case *mbp.ValueSource_ConstFloat:
		return constFloatNode(m, msg, wstlrEnv), nil
	case *mbp.ValueSource_ConstString:
		return constStringNode(m, msg, wstlrEnv), nil
	case *mbp.ValueSource_FromInput:
		return fromInputNode(m.FromInput, msg, wstlrEnv), nil
	default:
		return nil, fmt.Errorf("interpreting whistler message %v failed; type %T not supported", msg, m)
	}
}

func constBoolNode(msg *mbp.ValueSource_ConstBool, source *mbp.ValueSource, wstlrEnv *env) *ConstBoolNode {
	return &ConstBoolNode{
		id:      newIncID(),
		Value:   msg.ConstBool,
		Context: wstlrEnv.name,
		msg:     source,
	}
}

func constIntNode(msg *mbp.ValueSource_ConstInt, source *mbp.ValueSource, wstlrEnv *env) *ConstIntNode {
	return &ConstIntNode{
		id:      newIncID(),
		Value:   int(msg.ConstInt),
		Context: wstlrEnv.name,
		msg:     source,
	}
}

func constFloatNode(msg *mbp.ValueSource_ConstFloat, source *mbp.ValueSource, wstlrEnv *env) *ConstFloatNode {
	return &ConstFloatNode{
		id:      newIncID(),
		Value:   msg.ConstFloat,
		Context: wstlrEnv.name,
		msg:     source,
	}
}

func constStringNode(msg *mbp.ValueSource_ConstString, source *mbp.ValueSource, wstlrEnv *env) *ConstStringNode {
	return &ConstStringNode{
		id:      newIncID(),
		Value:   msg.ConstString,
		Context: wstlrEnv.name,
		msg:     source,
	}
}

func fromInputNode(msg *mbp.ValueSource_InputSource, source *mbp.ValueSource, wstlrEnv *env) Node {
	index := int(msg.GetArg())
	if index-1 == len(wstlrEnv.args) {
		return &RootNode{
			id:      newIncID(),
			Field:   msg.GetField(),
			Context: wstlrEnv.name,
			msg:     source,
		}
	} else {
		return &ArgumentNode{
			id:      newIncID(),
			Index:   int(msg.GetArg()),
			Field:   msg.GetField(),
			Context: wstlrEnv.name,
			msg:     source,
		}
	}
}

func projectorNode(msg *mbp.ProjectorDefinition, wstlrEnv *env) *ProjectorNode {
	var isBuiltin bool
	_, isBuiltin = builtins.BuiltinFunctions[msg.GetName()]
	return &ProjectorNode{
		id:        newIncID(),
		Name:      msg.GetName(),
		IsBuiltin: isBuiltin,
		Context:   wstlrEnv.name,
		msg:       msg,
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
		g.Edges[node.ID()] = []int{}

		if _, ok := node.(*ProjectorNode); ok { // projectors can have Argument children
			g.ArgumentEdges[node.ID()] = []int{}
		}

		if _, ok := node.(*TargetNode); ok { // targets can have conditions
			g.ConditionEdges[node.ID()] = []int{}
		}
	}

	if descendant == nil {
		return nil
	}

	var graphToAppend map[int][]int
	if isArg {
		graphToAppend = g.ArgumentEdges
	} else if isCondition {
		graphToAppend = g.ConditionEdges
	} else {
		graphToAppend = g.Edges
	}

	var ancestorList []int
	ancestorList, ok := graphToAppend[descendant.ID()]
	if !ok {
		return fmt.Errorf("expected node %v to have a descendant %v in the graph, but it didn't", node, descendant)
	}
	graphToAppend[descendant.ID()] = append(ancestorList, node.ID())
	if !isArg {
		recursive, err := isRecursive(g, node)
		if err != nil {
			return fmt.Errorf("failed to check if graph %v is recursive", g)
		}
		if recursive {
			return fmt.Errorf("adding node %v causes a recursive dependency in the graph", node)
		}
	}
	return nil
}

func isRecursive(g Graph, newNode Node) (bool, error) {
	previousAppearences := []int{}
	for id, node := range g.Nodes {
		if cmp.Equal(node.protoMsg(), newNode.protoMsg(), protocmp.Transform()) {
			previousAppearences = append(previousAppearences, id)
		}
	}
	ancestors := []int{}
	for _, id := range previousAppearences {
		ancestorsToAppend, ok := g.Edges[id]
		if !ok {
			return false, fmt.Errorf("couldn't find ID %v in the graph", id)
		}
		ancestors = append(ancestors, ancestorsToAppend...)
	}

	for _, startNode := range ancestors {
		recursive, err := isRecursiveHelper(g, newNode.ID(), startNode)
		if err != nil {
			return false, err
		}
		if recursive {
			return true, nil
		}
	}
	return false, nil
}

func isRecursiveHelper(g Graph, newNodeID int, currNodeID int) (bool, error) {
	currNode, ok := g.Nodes[currNodeID]
	if !ok {
		return false, fmt.Errorf("couldn't find ID %v in the graph", currNodeID)
	}
	newNode, ok := g.Nodes[newNodeID]
	if !ok {
		return false, fmt.Errorf("couldn't find ID %v in the graph", newNodeID)
	}
	if _, ok := currNode.(*ArgumentNode); ok {
		return false, nil // can't form a cycle through an argument
	}

	if cmp.Equal(newNode.protoMsg(), currNode.protoMsg(), protocmp.Transform()) {
		return true, nil
	}
	ancestors, ok := g.Edges[currNode.ID()]
	if !ok {
		return false, fmt.Errorf("couldn't find node %v in the Edges", currNode)
	}
	for _, ancestor := range ancestors {
		recursive, err := isRecursiveHelper(g, newNodeID, ancestor)
		if err != nil {
			return false, err
		}
		if recursive {
			return true, nil
		}
	}
	return false, nil
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
	if rootCondition.GetProjector() == and_keyword { // skip the $And node and directly return its ancestors
		conditions, err := projectorArgs(rootCondition, wstlrEnv, projectors)
		if err != nil {
			return nil, fmt.Errorf("failed to get ancestors of the $And condition message {%v}:\n%w", rootCondition, err)
		}
		return flatten(conditions), nil // the nested structure isn't important, since these aren't treated as indexed arguments
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

	wstlrNodes := make([]whistlerNode, 0)
	argNodes := e.args[index]
	for _, argNode := range argNodes {
		if msg.GetField() == "" { // the argument refers directly to a node in the graph and we can simply return this
			wstlrNodes = append(wstlrNodes, whistlerNode{
				msg:         nil,
				projSource:  nil,
				nodeInGraph: argNode.node,
			})
		} else { // the argument refers to a child of a target in the graph; we must search the graph for the child
			path := strings.Split(strings.TrimLeft(msg.GetField(), "."), ".") // trim the leading "." and split into target names
			if argNode.childTargets == nil {
				return nil, fmt.Errorf("lineage for node %v was not cached", argNode)
			}
			nodesInGraph, err := findNodesInGraph(path, argNode.node, argNode.childTargets)
			if err != nil {
				return nil, fmt.Errorf("failed to find node %v with path %v in the environment:\n%w", argNode.node, path, err)
			}
			for _, node := range nodesInGraph {
				wstlrNodes = append(wstlrNodes, whistlerNode{
					msg:         nil,
					projSource:  nil,
					nodeInGraph: node,
				})
			}
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
		nodesInGraph, err := findNodesInGraph(strings.Split(msg.FromDestination, "."), nil, wstlrEnv.targets)
		if err != nil {
			return nil, fmt.Errorf("failed to find dest target %v in the environment:\n%w", msg.FromDestination, err)
		}

		wstlrNodes := make([]whistlerNode, len(nodesInGraph))
		for i, node := range nodesInGraph {
			wstlrNodes[i] = whistlerNode{
				msg:         source,
				nodeInGraph: node,
			}
		}
		return wstlrNodes, nil
	case *mbp.ValueSource_FromLocalVar:
		nodesInGraph, err := findNodesInGraph(strings.Split(msg.FromLocalVar, "."), nil, wstlrEnv.vars)
		if err != nil {
			return nil, fmt.Errorf("failed to find local variable %v in the environment:\n%w", msg.FromLocalVar, err)
		}

		wstlrNodes := make([]whistlerNode, len(nodesInGraph))
		for i, node := range nodesInGraph {
			wstlrNodes[i] = whistlerNode{
				msg:         source,
				nodeInGraph: node,
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

func readVarFromEnv(varName string, e *env) ([]targetLineage, error) {
	if targetLineages, ok := e.vars[varName]; ok {
		return targetLineages, nil
	} else if e.parent != nil {
		return readVarFromEnv(varName, e.parent)
	} else {
		return nil, fmt.Errorf("couldn't find the local variable '%v' in the environment", varName)
	}
}

// return previously-generated nodes in the graph based on a path of target names
func findNodesInGraph(path []string, currNode Node, lineages map[string][]targetLineage) ([]Node, error) {
	if len(path) == 0 {
		if currNode == nil {
			return nil, fmt.Errorf("couldn't find path %v in the environment", path)
		}
		return []Node{currNode}, nil
	}
	matchingNodes := make([]Node, 0)
	for targetName, lineages := range lineages {
		numMatchingNodes := matchUpToDiff(strings.Split(targetName, "."), path)
		if numMatchingNodes > 0 {
			for _, childLineage := range lineages {
				nodes, _ := findNodesInGraph(path[numMatchingNodes:], childLineage.node, childLineage.childTargets)
				matchingNodes = append(matchingNodes, nodes...)
			}
		}
	}
	if len(matchingNodes) == 0 {
		return nil, fmt.Errorf("couldn't find path %v in the environment", path)
	}
	return matchingNodes, nil
}

// Matches the entirety of one path against another, returning the length of the shorter path if matched and zero otherwise.
// This is needed because a target name may be like "x.y", and this should be treated as two separate targets when
// querying a target in the graph.
// Because composite target names are not split, querying the target name "a" of the target "a.b" will return "a.b" in its
// entirety; the node "a" does not actually exist.
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
