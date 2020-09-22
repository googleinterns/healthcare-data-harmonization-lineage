package graph

import (
	"fmt"

	gpb "github.com/googleinterns/healthcare-data-harmonization-lineage/graph/proto"
)

// WriteGraph takes a graph and creates a protobuf representation of it
func WriteProtobuf(g Graph) (*gpb.Graph, error) {
	pbGraph := gpb.Graph{
		Edges:             map[int32]*gpb.EdgeList{},
		ArgumentEdges:     map[int32]*gpb.EdgeList{},
		ConditionEdges:    map[int32]*gpb.EdgeList{},
		RootAndOutTargets: map[string]*gpb.EdgeList{},
		Nodes:             map[int32]*gpb.Node{},
	}

	for id, node := range g.Nodes {
		pbNode, err := convertNode(node)
		if err != nil {
			return nil, fmt.Errorf("failed to convert node %v to protobuf:\n%w", node, err)
		}
		pbGraph.Nodes[int32(id)] = pbNode
	}

	for id, idList := range g.Edges {
		pbGraph.Edges[int32(id)] = newEdgeList(idList)
	}
	for id, idList := range g.ArgumentEdges {
		pbGraph.ArgumentEdges[int32(id)] = newEdgeList(idList)
	}
	for id, idList := range g.ConditionEdges {
		pbGraph.ConditionEdges[int32(id)] = newEdgeList(idList)
	}
	for name, idList := range g.RootAndOutTargets {
		pbGraph.RootAndOutTargets[name] = newEdgeList(idList)
	}

	return &pbGraph, nil
}

func newEdgeList(idList []int) *gpb.EdgeList {
	pbIDlist := make([]int32, len(idList))
	for i, id := range idList {
		pbIDlist[i] = int32(id)
	}
	return &gpb.EdgeList{
		Edges: pbIDlist,
	}
}

func convertNode(node Node) (*gpb.Node, error) {
	switch n := node.(type) {
	case *TargetNode:
		return &gpb.Node{
			Node: &gpb.Node_TargetNode{
				TargetNode: &gpb.TargetNode{
					Id:          int32(n.ID()),
					Name:        n.Name,
					Context:     n.Context,
					IsVariable:  n.IsVariable,
					IsOverwrite: n.IsOverwrite,
					IsRoot:      n.IsRoot,
					IsOut:       n.IsOut,
					FileData:    convertFileData(n.FileData),
				},
			},
		}, nil
	case *ConstIntNode:
		return &gpb.Node{
			Node: &gpb.Node_ConstIntNode{
				ConstIntNode: &gpb.ConstIntNode{
					Id:       int32(n.ID()),
					Value:    int32(n.Value),
					Context:  n.Context,
					FileData: convertFileData(n.FileData),
				},
			},
		}, nil
	case *ConstFloatNode:
		return &gpb.Node{
			Node: &gpb.Node_ConstFloatNode{
				ConstFloatNode: &gpb.ConstFloatNode{
					Id:       int32(n.ID()),
					Value:    n.Value,
					Context:  n.Context,
					FileData: convertFileData(n.FileData),
				},
			},
		}, nil
	case *ConstBoolNode:
		return &gpb.Node{
			Node: &gpb.Node_ConstBoolNode{
				ConstBoolNode: &gpb.ConstBoolNode{
					Id:       int32(n.ID()),
					Value:    n.Value,
					Context:  n.Context,
					FileData: convertFileData(n.FileData),
				},
			},
		}, nil
	case *ConstStringNode:
		return &gpb.Node{
			Node: &gpb.Node_ConstStringNode{
				ConstStringNode: &gpb.ConstStringNode{
					Id:       int32(n.ID()),
					Value:    n.Value,
					Context:  n.Context,
					FileData: convertFileData(n.FileData),
				},
			},
		}, nil
	case *ProjectorNode:
		return &gpb.Node{
			Node: &gpb.Node_ProjectorNode{
				ProjectorNode: &gpb.ProjectorNode{
					Id:        int32(n.ID()),
					Name:      n.Name,
					IsBuiltin: n.IsBuiltin,
					Context:   n.Context,
					FileData:  convertFileData(n.FileData),
				},
			},
		}, nil
	case *ArgumentNode:
		return &gpb.Node{
			Node: &gpb.Node_ArgumentNode{
				ArgumentNode: &gpb.ArgumentNode{
					Id:       int32(n.ID()),
					Index:    int32(n.Index),
					Field:    n.Field,
					Context:  n.Context,
					FileData: convertFileData(n.FileData),
				},
			},
		}, nil
	case *RootNode:
		return &gpb.Node{
			Node: &gpb.Node_RootNode{
				RootNode: &gpb.RootNode{
					Id:       int32(n.ID()),
					Field:    n.Field,
					Context:  n.Context,
					FileData: convertFileData(n.FileData),
				},
			},
		}, nil
	default:
		return nil, fmt.Errorf("message %v of type %T is not supported", n, n)
	}
}

func convertFileData(data FileMetaData) *gpb.FileMetaData {
	return &gpb.FileMetaData{
		FileName:  data.FileName,
		LineStart: int32(data.LineStart),
		LineEnd:   int32(data.LineEnd),
		CharStart: int32(data.CharStart),
		CharEnd:   int32(data.CharEnd),
	}
}
