package graph

import (
	"bytes"
	"fmt"
	"log"

	"github.com/goccy/go-graphviz"
	"github.com/goccy/go-graphviz/cgraph"
)

func WriteDOTpng(graph Graph, outputFile string) (string, error) {
	g := graphviz.New()
	dotGraph, err := g.Graph()
	if err != nil {
		return "", fmt.Errorf("failed to create new dot graph:\n%w", err)
	}
	defer func() {
		if err := dotGraph.Close(); err != nil {
			log.Fatal(err)
		}
		g.Close()
	}()

	dotNodes := map[int]*cgraph.Node{}
	for id, node := range graph.Nodes {
		label, err := getNodeLabel(node)
		if err != nil {
			return "", fmt.Errorf("failed to create label for node %v:\n%w", node, err)
		}
		dotNode, err := dotGraph.CreateNode(fmt.Sprintf("%v", id))
		if err != nil {
			return "", fmt.Errorf("failed to create node for %v:\n%w", node, err)
		}
		dotNode.SetLabel(label)
		dotNodes[id] = dotNode
	}

	for nodeID, ancestorIDs := range graph.Edges {
		for _, ancestorID := range ancestorIDs {
			_, err := dotGraph.CreateEdge("", dotNodes[nodeID], dotNodes[ancestorID])
			if err != nil {
				return "", err
			}
		}
	}

	for nodeID, ancestorIDs := range graph.ArgumentEdges {
		for _, ancestorID := range ancestorIDs {
			e, err := dotGraph.CreateEdge("", dotNodes[nodeID], dotNodes[ancestorID])
			if err != nil {
				return "", err
			}
			e.SetStyle(cgraph.DashedEdgeStyle)
			e.SetLabel("arg")
		}
	}

	for nodeID, ancestorIDs := range graph.ConditionEdges {
		for _, ancestorID := range ancestorIDs {
			e, err := dotGraph.CreateEdge("", dotNodes[nodeID], dotNodes[ancestorID])
			if err != nil {
				return "", err
			}
			e.SetStyle(cgraph.DottedEdgeStyle)
			e.SetLabel("cond")
		}
	}

	var buf bytes.Buffer
	if err := g.Render(dotGraph, "dot", &buf); err != nil {
		return "", fmt.Errorf("%v", err)
	}
	dotString := buf.String()

	if outputFile != "" {
		if err := g.RenderFilename(dotGraph, graphviz.PNG, outputFile); err != nil {
			return "", fmt.Errorf("could not write PNG image for graph %v\n%w", dotString, err)
		}
	}

	return dotString, nil
}

func getNodeLabel(node Node) (string, error) {
	switch n := node.(type) {
	case *ConstBoolNode:
		return fmt.Sprintf("%v", n.Value), nil
	case *ConstStringNode:
		return fmt.Sprintf("\"%v\"", n.Value), nil
	case *ConstIntNode:
		return fmt.Sprintf("%v", n.Value), nil
	case *ConstFloatNode:
		return fmt.Sprintf("%v", n.Value), nil
	case *TargetNode:
		modString := ""
		if n.IsVariable {
			modString = "var "
		} else if n.IsRoot {
			modString = "root "
		} else if n.IsOut {
			modString = "out "
		}
		return fmt.Sprintf("%v%v", modString, n.Name), nil
	case *ProjectorNode:
		return fmt.Sprintf("def %v", n.Name), nil
	case *ArgumentNode:
		fieldString := ""
		if n.Field != "" {
			fieldString = fmt.Sprintf("\nfield %v", n.Field)
		}
		return fmt.Sprintf("arg %v%v", n.Index, fieldString), nil
	case *RootNode:
		fieldString := ""
		if n.Field != "" {
			fieldString = fmt.Sprintf("\nfield %v", n.Field)
		}
		return fmt.Sprintf("$root%v", fieldString), nil
	default:
		return "", fmt.Errorf("node of type %T is not supported", n)
	}
}
