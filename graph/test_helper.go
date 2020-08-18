package graph

import (
	"fmt"

	mbp "github.com/GoogleCloudPlatform/healthcare-data-harmonization/mapping_engine/proto"
)

func makeIntNode(val int, id int) *ConstIntNode {
	return &ConstIntNode{
		id:    intID(id),
		Value: val,
		msg:   makeIntMsg(val),
	}
}

func makeBoolNode(val bool, id int) *ConstBoolNode {
	return &ConstBoolNode{
		id:    intID(id),
		Value: val,
		msg:   makeBoolMsg(val),
	}
}

func makeFloatNode(val float32, id int) *ConstFloatNode {
	return &ConstFloatNode{
		id:    intID(id),
		Value: val,
		msg:   makeFloatMsg(val),
	}
}

func makeStringNode(val string, id int) *ConstStringNode {
	return &ConstStringNode{
		id:    intID(id),
		Value: val,
		msg:   makeStringMsg(val),
	}
}

func makeTargetNode(name string, id int) *TargetNode {
	return &TargetNode{
		id:   intID(id),
		Name: name,
		msg: &mbp.FieldMapping{
			Target: &mbp.FieldMapping_TargetField{
				TargetField: name,
			},
		},
	}
}

func makeVarNode(name string, id int) *TargetNode {
	return &TargetNode{
		id:         intID(id),
		Name:       name,
		IsVariable: true,
		msg: &mbp.FieldMapping{
			Target: &mbp.FieldMapping_TargetLocalVar{
				TargetLocalVar: name,
			},
		},
	}
}

func makeProjNode(name string, id int) *ProjectorNode {
	return &ProjectorNode{
		id:   intID(id),
		Name: name,
		msg: &mbp.ProjectorDefinition{
			Name: name,
		},
	}
}

func makeArgNode(index int, field string, id int) *ArgumentNode {
	return &ArgumentNode{
		id:    intID(id),
		Index: index,
		Field: field,
		msg: &mbp.ValueSource{
			Source: &mbp.ValueSource_FromInput{
				FromInput: &mbp.ValueSource_InputSource{
					Arg:   int32(index),
					Field: field,
				},
			},
		},
	}
}

func makeProjValMsg(projector string) *mbp.ValueSource {
	return &mbp.ValueSource{
		Source: &mbp.ValueSource_ProjectedValue{
			ProjectedValue: &mbp.ValueSource{
				Projector: projector,
			},
		},
	}
}

func makeLocalVarMsg(varName string) *mbp.ValueSource {
	return &mbp.ValueSource{
		Source: &mbp.ValueSource_FromLocalVar{
			FromLocalVar: varName,
		},
	}
}

func makeDestValSourceMsg(destination string) *mbp.ValueSource {
	return &mbp.ValueSource{
		Source: &mbp.ValueSource_FromDestination{
			FromDestination: destination,
		},
	}
}

func makeProjDefMsg(projector string, mappings []*mbp.FieldMapping) *mbp.ProjectorDefinition {
	return &mbp.ProjectorDefinition{
		Name:    projector,
		Mapping: mappings,
	}
}

func makeMappingConfigMsg(projectors []*mbp.ProjectorDefinition, mappings []*mbp.FieldMapping) *mbp.MappingConfig {
	return &mbp.MappingConfig{
		Projector:   projectors,
		RootMapping: mappings,
	}
}

func makeVarMappingMsg(target string, valueSource *mbp.ValueSource, condition *mbp.ValueSource) *mbp.FieldMapping {
	return &mbp.FieldMapping{
		Target: &mbp.FieldMapping_TargetLocalVar{
			TargetLocalVar: target,
		},

		ValueSource: valueSource,
		Condition:   condition,
	}
}

func makeMappingMsg(target string, valueSource *mbp.ValueSource, condition *mbp.ValueSource) *mbp.FieldMapping {
	return &mbp.FieldMapping{
		Target: &mbp.FieldMapping_TargetField{
			TargetField: target,
		},
		ValueSource: valueSource,
		Condition:   condition,
	}
}

func makeArgMsg(index int, field string) *mbp.ValueSource {
	return &mbp.ValueSource{
		Source: &mbp.ValueSource_FromInput{
			FromInput: &mbp.ValueSource_InputSource{
				Arg:   int32(index),
				Field: field,
			},
		},
	}
}

func makeProjSourceMsg(projector string, source *mbp.ValueSource, args []*mbp.ValueSource) *mbp.ValueSource {
	return &mbp.ValueSource{
		Source:        source.GetSource(),
		Projector:     projector,
		AdditionalArg: args,
	}
}

func makeProjectedSourceMsg(projectedVal *mbp.ValueSource, projector string, args []*mbp.ValueSource) *mbp.ValueSource {
	return &mbp.ValueSource{
		Source: &mbp.ValueSource_ProjectedValue{
			ProjectedValue: projectedVal,
		},
		Projector:     projector,
		AdditionalArg: args,
	}
}

func makeIntMsg(val int) *mbp.ValueSource {
	return &mbp.ValueSource{
		Source: &mbp.ValueSource_ConstInt{
			ConstInt: int32(val),
		},
	}
}

func makeBoolMsg(val bool) *mbp.ValueSource {
	return &mbp.ValueSource{
		Source: &mbp.ValueSource_ConstBool{
			ConstBool: val,
		},
	}
}

func makeFloatMsg(val float32) *mbp.ValueSource {
	return &mbp.ValueSource{
		Source: &mbp.ValueSource_ConstFloat{
			ConstFloat: val,
		},
	}
}

func makeStringMsg(val string) *mbp.ValueSource {
	return &mbp.ValueSource{
		Source: &mbp.ValueSource_ConstString{
			ConstString: val,
		},
	}
}

func compareGraphs(g, h map[IsID][]IsID, gNodes, hNodes map[IsID]Node) (bool, string) {
	for gID := range g {
		gNode := gNodes[gID]
		hMatches := findNodesInMap(gNode, hNodes)
		if len(hMatches) == 0 {
			return false, fmt.Sprintf("expected node %v to be in the graph %v", gNode, h)
		}
		nodesMatch := false
		for _, hMatch := range hMatches {
			gAncestorIDs := g[gID]
			hAncestorIDs := h[hMatch.ID()]
			gAncestors := make([]Node, len(gAncestorIDs))
			hAncestors := make([]Node, len(hAncestorIDs))
			if len(gAncestors) != len(hAncestors) {
				continue
			}
			for i, _ := range gAncestors {
				gAncestors[i] = gNodes[gAncestorIDs[i]]
				hAncestors[i] = hNodes[hAncestorIDs[i]]
			}

			if nodeSlicesMatch(gAncestors, hAncestors) {
				nodesMatch = true
				break
			}
		}
		if !nodesMatch {
			return false, fmt.Sprintf("node %v of graph H has no match in graph G. Nodes considered: %v", gNode, hNodes)
		}
	}
	return true, ""
}

func nodeSlicesMatch(gNodes, hNodes []Node) bool {
	if len(gNodes) != len(hNodes) {
		return false
	}
	for _, gNode := range gNodes {
		if _, found := findNodeInSlice(gNode, hNodes); !found {
			return false
		}
	}
	return true
}

func findNodeInSlice(targetNode Node, nodes []Node) (Node, bool) {
	for _, node := range nodes {
		if equalsIgnoreID(targetNode, node) {
			return node, true
		}
	}
	return nil, false
}

func findNodesInMap(targetNode Node, nodes map[IsID]Node) []Node {
	foundNodes := []Node{}
	for _, node := range nodes {
		if equalsIgnoreID(targetNode, node) {
			foundNodes = append(foundNodes, node)
		}
	}
	return foundNodes
}

func equalsIgnoreID(n1 Node, n2 Node) bool {
	if n1 == nil && n2 == nil {
		return true
	} else if n1 == nil || n2 == nil {
		return false
	}
	n1ID := n1.ID()
	n2ID := n2.ID()

	msg1 := n1.protoMsg()
	msg2 := n2.protoMsg()

	n1.setID(intID(0))
	n2.setID(intID(0))

	n1.setProtoMsg(nil)
	n2.setProtoMsg(nil)

	areEqual := n1.Equals(n2)

	n1.setID(n1ID)
	n2.setID(n2ID)

	n1.setProtoMsg(msg1)
	n2.setProtoMsg(msg2)

	return areEqual
}

func ids0() []IsID {
	return []IsID{}
}

func ids1(id int) []IsID {
	return []IsID{intID(id)}
}

func ids2(id1, id2 int) []IsID {
	return []IsID{intID(id1), intID(id2)}
}
