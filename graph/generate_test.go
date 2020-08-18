package graph

import (
	"fmt"
	"testing"

	mbp "github.com/GoogleCloudPlatform/healthcare-data-harmonization/mapping_engine/proto"
	"github.com/GoogleCloudPlatform/healthcare-data-harmonization/mapping_language/transpiler"
	proto "github.com/golang/protobuf/proto"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

type mockIDfactory int

func (id mockIDfactory) New() IsID {
	return intID(id)
}

// TODO: in the conditionals refactor, this is replaced with helper functions for creating nodes in-place
// a map of convenient nodes
func getNodeMap() map[string]Node {
	msgMap := getMsgMap()
	return map[string]Node{
		"targetX": &TargetNode{
			id:   idFactory.New(),
			Name: "x",
			msg: &mbp.FieldMapping{
				Target: &mbp.FieldMapping_TargetField{
					TargetField: "x",
				},
			},
		},
		"targetY": &TargetNode{
			id:   idFactory.New(),
			Name: "y",
			msg: &mbp.FieldMapping{
				Target: &mbp.FieldMapping_TargetField{
					TargetField: "y",
				},
			},
		},
		"targetZ": &TargetNode{
			id:   idFactory.New(),
			Name: "z",
			msg: &mbp.FieldMapping{
				Target: &mbp.FieldMapping_TargetField{
					TargetField: "z",
				},
			},
		},
		"bool": &ConstBoolNode{
			id:    idFactory.New(),
			Value: true,
			msg:   msgMap["bool"],
		},
		"float": &ConstFloatNode{
			id:    idFactory.New(),
			Value: 5.0,
			msg:   msgMap["float"],
		},
		"string": &ConstStringNode{
			id:    idFactory.New(),
			Value: "foo",
			msg:   msgMap["string"],
		},
		"int": &ConstIntNode{
			id:    idFactory.New(),
			Value: 0,
			msg:   msgMap["int"],
		},
		"proj1": &ProjectorNode{
			id:   idFactory.New(),
			Name: "proj1",
			msg:  msgMap["proj1"],
		},
		"proj2": &ProjectorNode{
			id:   idFactory.New(),
			Name: "proj2",
			msg: &mbp.ProjectorDefinition{
				Name: "proj2",
			},
		},
		"proj_empty": &ProjectorNode{
			id:   idFactory.New(),
			Name: "proj_empty",
			msg:  msgMap["proj_empty"],
		},
		"arg0": &ArgumentNode{
			id:    idFactory.New(),
			Index: 1,
			msg:   msgMap["arg0"],
		},
		"arg1": &ArgumentNode{
			id:    idFactory.New(),
			Index: 2,
			msg:   msgMap["arg1"],
		},
	}
}

// a map of convenient proto messages
func getMsgMap() map[string]proto.Message {
	boolMsg := &mbp.ValueSource{
		Source: &mbp.ValueSource_ConstBool{
			ConstBool: true,
		},
	}
	intMsg := &mbp.ValueSource{
		Source: &mbp.ValueSource_ConstInt{
			ConstInt: 0,
		},
	}
	source_noargs := &mbp.ValueSource{
		Projector: "proj1",
	}
	source_args := &mbp.ValueSource{
		Projector: "proj1",
		Source: &mbp.ValueSource_ConstBool{
			ConstBool: true,
		},
		AdditionalArg: []*mbp.ValueSource{intMsg},
	}
	targetX := &mbp.FieldMapping_TargetField{
		TargetField: "x",
	}
	targetY := &mbp.FieldMapping_TargetField{
		TargetField: "y",
	}
	mapping := &mbp.FieldMapping{
		Target:      targetX,
		ValueSource: boolMsg,
	}
	mapping2 := &mbp.FieldMapping{
		Target:      targetY,
		ValueSource: boolMsg,
	}
	mapping_proj1 := &mbp.FieldMapping{
		Target:      targetX,
		ValueSource: source_noargs,
	}
	mapping_proj2 := &mbp.FieldMapping{
		Target:      targetX,
		ValueSource: source_args,
	}
	mapping_nil := &mbp.FieldMapping{}
	arg0 := &mbp.ValueSource{
		Source: &mbp.ValueSource_FromInput{
			FromInput: &mbp.ValueSource_InputSource{
				Arg: 1,
			},
		},
	}
	proj1 := &mbp.ProjectorDefinition{
		Name:    "proj1",
		Mapping: []*mbp.FieldMapping{mapping},
	}
	proj_empty := &mbp.ProjectorDefinition{
		Name:    "proj_empty",
		Mapping: []*mbp.FieldMapping{},
	}
	arg1 := &mbp.ValueSource{
		Source: &mbp.ValueSource_FromInput{
			FromInput: &mbp.ValueSource_InputSource{
				Arg: 2,
			},
		},
	}
	projectedVal := &mbp.ValueSource{
		Source: &mbp.ValueSource_ProjectedValue{
			ProjectedValue: boolMsg,
		},
	}
	projVal2 := &mbp.ValueSource{
		Source: &mbp.ValueSource_ProjectedValue{
			ProjectedValue: &mbp.ValueSource{
				Projector: "proj_empty",
			},
		},
	}

	return map[string]proto.Message{
		"bool": boolMsg,
		"float": &mbp.ValueSource{
			Source: &mbp.ValueSource_ConstFloat{
				ConstFloat: 5.0,
			},
		},
		"string": &mbp.ValueSource{
			Source: &mbp.ValueSource_ConstString{
				ConstString: "foo",
			},
		},
		"int":           intMsg,
		"source_noargs": source_noargs,
		"source_args":   source_args,
		"projectedVal":  projectedVal,
		"projVal2":      projVal2,
		"mapping":       mapping,
		"mapping2":      mapping2,
		"mapping_proj1": mapping_proj1,
		"mapping_proj2": mapping_proj2,
		"mapping_nil":   mapping_nil,
		"proj1":         proj1,
		"proj_empty":    proj_empty,
		"arg0":          arg0,
		"arg1":          arg1,
	}
}

func compareGraphs(g, h map[IsID][]IsID, gNodes map[IsID]Node, hNodes map[IsID]Node) (bool, string) {
	for gID, gAncestorIDs := range g {
		gNode := gNodes[gID]
		hNode, ok := findNodeInMap(gNode, hNodes)
		if !ok {
			return false, fmt.Sprintf("expected node %v to be in the graph %v", gNode, h)
		}
		hAncestorIDs := h[hNode.ID()]
		if len(gAncestorIDs) != len(hAncestorIDs) {
			return false, fmt.Sprintf("node %v wanted %v ancestors, but has %v", gNode, len(gAncestorIDs), len(hAncestorIDs))
		}

		for _, gAncestorID := range gAncestorIDs {
			gAncestor := gNodes[gAncestorID]
			hAncestors := make([]Node, len(hAncestorIDs))
			for i, hAncestorID := range hAncestorIDs {
				hAncestors[i] = hNodes[hAncestorID]
			}
			if _, found := findNodeInSlice(gAncestor, hAncestors); !found {
				return false, fmt.Sprintf("expected node %v to be an ancestor of %v, but it was not", gAncestor, gNode)
			}
		}
	}
	return true, ""
}

func findNodeInSlice(targetNode Node, nodes []Node) (Node, bool) {
	for _, node := range nodes {
		if EqualsIgnoreID(targetNode, node) {
			return node, true
		}
	}
	return nil, false
}

func findNodeInMap(targetNode Node, nodes map[IsID]Node) (Node, bool) {
	for _, node := range nodes {
		if EqualsIgnoreID(targetNode, node) {
			return node, true
		}
	}
	return nil, false
}

func EqualsIgnoreID(n1 Node, n2 Node) bool {
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

func TestNew_Whistle(t *testing.T) {
	nodeMap := getNodeMap()
	tests := []struct {
		name       string
		whistle    string
		want       Graph
		wantErrors bool
	}{
		{
			name:    "test constant mapping",
			whistle: "x: true",
			want: Graph{
				Edges: map[IsID][]IsID{
					nodeMap["targetX"].ID(): []IsID{nodeMap["bool"].ID()},
					nodeMap["bool"].ID():    []IsID{},
				},
				ArgumentEdges: map[IsID][]IsID{},
				Nodes: map[IsID]Node{
					nodeMap["targetX"].ID(): nodeMap["targetX"],
					nodeMap["bool"].ID():    nodeMap["bool"],
				},
			},
			wantErrors: false,
		},
		{
			name:       "test empty mapping",
			whistle:    "",
			want:       Graph{},
			wantErrors: false,
		},
		{
			name: "test projector",
			whistle: `
			x: proj1()
			def proj1() {
				y: 5.0
			}
			`,
			want: Graph{
				Edges: map[IsID][]IsID{
					nodeMap["targetX"].ID(): []IsID{nodeMap["proj1"].ID()},
					nodeMap["proj1"].ID():   []IsID{nodeMap["targetY"].ID()},
					nodeMap["targetY"].ID(): []IsID{nodeMap["float"].ID()},
					nodeMap["float"].ID():   []IsID{},
				},
				ArgumentEdges: map[IsID][]IsID{
					nodeMap["proj1"].ID(): []IsID{},
				},
				Nodes: map[IsID]Node{
					nodeMap["targetX"].ID(): nodeMap["targetX"],
					nodeMap["proj1"].ID():   nodeMap["proj1"],
					nodeMap["targetY"].ID(): nodeMap["targetY"],
					nodeMap["float"].ID():   nodeMap["float"],
				},
			},
			wantErrors: false,
		},
		{
			name: "test projector arguments",
			whistle: `
			x: proj1(true, 5.0)
			def proj1(arg1, arg2) {
				y: arg1
			}
			`,
			want: Graph{
				Edges: map[IsID][]IsID{
					nodeMap["targetX"].ID(): []IsID{nodeMap["proj1"].ID()},
					nodeMap["proj1"].ID():   []IsID{nodeMap["targetY"].ID()},
					nodeMap["targetY"].ID(): []IsID{nodeMap["arg0"].ID()},
					nodeMap["arg0"].ID():    []IsID{nodeMap["bool"].ID()},
					nodeMap["bool"].ID():    []IsID{},
					nodeMap["float"].ID():   []IsID{},
				},
				ArgumentEdges: map[IsID][]IsID{
					nodeMap["proj1"].ID(): []IsID{nodeMap["bool"].ID(), nodeMap["float"].ID()},
				},
				Nodes: map[IsID]Node{
					nodeMap["targetX"].ID(): nodeMap["targetX"],
					nodeMap["proj1"].ID():   nodeMap["proj1"],
					nodeMap["targetY"].ID(): nodeMap["targetY"],
					nodeMap["arg0"].ID():    nodeMap["arg0"],
					nodeMap["bool"].ID():    nodeMap["bool"],
					nodeMap["float"].ID():   nodeMap["float"],
				},
			},
			wantErrors: false,
		},
		{
			name: "test nested projectors",
			whistle: `
			x: proj1(proj2())

                        def proj2() {
                                z: "foo"
                        }

			def proj1(arg1) {
				y: arg1
			}
			`,
			want: Graph{
				Edges: map[IsID][]IsID{
					nodeMap["targetX"].ID(): []IsID{nodeMap["proj1"].ID()},
					nodeMap["proj1"].ID():   []IsID{nodeMap["targetY"].ID()},
					nodeMap["targetY"].ID(): []IsID{nodeMap["arg0"].ID()},
					nodeMap["arg0"].ID():    []IsID{nodeMap["proj2"].ID()},
					nodeMap["proj2"].ID():   []IsID{nodeMap["targetZ"].ID()},
					nodeMap["targetZ"].ID(): []IsID{nodeMap["string"].ID()},
					nodeMap["string"].ID():  []IsID{},
				},
				ArgumentEdges: map[IsID][]IsID{
					nodeMap["proj1"].ID(): []IsID{nodeMap["proj2"].ID()},
					nodeMap["proj2"].ID(): []IsID{},
				},
				Nodes: map[IsID]Node{
					nodeMap["targetX"].ID(): nodeMap["targetX"],
					nodeMap["proj1"].ID():   nodeMap["proj1"],
					nodeMap["targetY"].ID(): nodeMap["targetY"],
					nodeMap["arg0"].ID():    nodeMap["arg0"],
					nodeMap["proj2"].ID():   nodeMap["proj2"],
					nodeMap["targetZ"].ID(): nodeMap["targetZ"],
					nodeMap["string"].ID():  nodeMap["string"],
				},
			},
			wantErrors: false,
		},
		{
			name: "test recursion simple",
			whistle: `
			x: proj1()

			def proj1() {
				z: proj1()
			}
			`,
			wantErrors: true,
		},
		{
			name: "test cycle",
			whistle: `
			x: foo()
			
			def foo() {
				y: bar()
			}

			def bar() {
				z: foo()
			}
			`,
			wantErrors: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mpc, err := transpiler.Transpile(test.whistle)
			if err != nil {
				t.Errorf("transpiling whistle '%v' failed with error %w", test.whistle, err)
			}
			g, err := New(mpc)
			if test.wantErrors && err == nil {
				t.Errorf("expected error building graph")
			} else if !test.wantErrors && err != nil {
				t.Errorf("building graph for %v failed: %w", test.whistle, err)
			}

			if !test.wantErrors {
				if len(test.want.Nodes) != len(g.Nodes) {
					t.Errorf("expected %v nodes, but got %v; %v", len(test.want.Nodes), len(g.Nodes), g.Nodes)
				}
				for _, wantNode := range test.want.Nodes {
					if _, found := findNodeInMap(wantNode, g.Nodes); !found {
						t.Errorf("expected node {%v} to be in the graph, but it was not", wantNode)
					}
				}
				if equal, errStr := compareGraphs(test.want.Edges, g.Edges, test.want.Nodes, g.Nodes); !equal {
					t.Errorf("the graph edges are not as expected:\n%v", errStr)
				}
				if equal, errStr := compareGraphs(test.want.ArgumentEdges, g.ArgumentEdges, test.want.Nodes, g.Nodes); !equal {
					t.Errorf("the graph argument edges are not as expected:\n%v", errStr)
				}
			}
		},
		)
	}
}

func TestNew_WhistlerProto(t *testing.T) {
	nodeMap := getNodeMap()
	tests := []struct {
		name       string
		mpc        *mbp.MappingConfig
		want       Graph
		wantErrors bool
	}{
		{
			name: "test constant mapping",
			mpc: &mbp.MappingConfig{
				Projector: []*mbp.ProjectorDefinition{},
				RootMapping: []*mbp.FieldMapping{
					&mbp.FieldMapping{
						ValueSource: &mbp.ValueSource{
							Source: &mbp.ValueSource_ConstBool{
								ConstBool: true,
							},
						},
						Target: &mbp.FieldMapping_TargetField{
							TargetField: "x",
						},
					},
				},
			},
			want: Graph{
				Edges: map[IsID][]IsID{
					nodeMap["targetX"].ID(): []IsID{nodeMap["bool"].ID()},
					nodeMap["bool"].ID():    []IsID{},
				},
				ArgumentEdges: map[IsID][]IsID{},
				Nodes: map[IsID]Node{
					nodeMap["targetX"].ID(): nodeMap["targetX"],
					nodeMap["bool"].ID():    nodeMap["bool"],
				},
			},
			wantErrors: false,
		},
		{
			name: "test empty mapping",
			mpc: &mbp.MappingConfig{
				RootMapping: []*mbp.FieldMapping{},
			},
			want:       Graph{},
			wantErrors: false,
		},
		{
			name: "test projector",
			mpc: &mbp.MappingConfig{
				RootMapping: []*mbp.FieldMapping{
					&mbp.FieldMapping{
						ValueSource: &mbp.ValueSource{
							Projector: "proj1",
						},
						Target: &mbp.FieldMapping_TargetField{
							TargetField: "x",
						},
					},
				},
				Projector: []*mbp.ProjectorDefinition{
					&mbp.ProjectorDefinition{
						Name: "proj1",
						Mapping: []*mbp.FieldMapping{
							&mbp.FieldMapping{
								ValueSource: &mbp.ValueSource{
									Source: &mbp.ValueSource_ConstInt{
										ConstInt: 0,
									},
								},
								Target: &mbp.FieldMapping_TargetField{
									TargetField: "y",
								},
							},
						},
					},
				},
			},
			want: Graph{
				Edges: map[IsID][]IsID{
					nodeMap["targetX"].ID(): []IsID{nodeMap["proj1"].ID()},
					nodeMap["proj1"].ID():   []IsID{nodeMap["targetY"].ID()},
					nodeMap["targetY"].ID(): []IsID{nodeMap["int"].ID()},
					nodeMap["int"].ID():     []IsID{},
				},
				ArgumentEdges: map[IsID][]IsID{
					nodeMap["proj1"].ID(): []IsID{},
				},
				Nodes: map[IsID]Node{
					nodeMap["targetX"].ID(): nodeMap["targetX"],
					nodeMap["proj1"].ID():   nodeMap["proj1"],
					nodeMap["targetY"].ID(): nodeMap["targetY"],
					nodeMap["int"].ID():     nodeMap["int"],
				},
			},
			wantErrors: false,
		},
		{
			name: "test projector arguments",
			mpc: &mbp.MappingConfig{
				RootMapping: []*mbp.FieldMapping{
					&mbp.FieldMapping{
						ValueSource: &mbp.ValueSource{
							Source: &mbp.ValueSource_ConstBool{
								ConstBool: true,
							},
							AdditionalArg: []*mbp.ValueSource{
								&mbp.ValueSource{
									Source: &mbp.ValueSource_ConstFloat{
										ConstFloat: 5.0,
									},
								},
							},
							Projector: "proj1",
						},
						Target: &mbp.FieldMapping_TargetField{
							TargetField: "x",
						},
					},
				},
				Projector: []*mbp.ProjectorDefinition{
					&mbp.ProjectorDefinition{
						Name: "proj1",
						Mapping: []*mbp.FieldMapping{
							&mbp.FieldMapping{
								ValueSource: &mbp.ValueSource{
									Source: &mbp.ValueSource_FromInput{
										FromInput: &mbp.ValueSource_InputSource{
											Arg: 1,
										},
									},
								},
								Target: &mbp.FieldMapping_TargetField{
									TargetField: "y",
								},
							},
						},
					},
				},
			},
			want: Graph{
				Edges: map[IsID][]IsID{
					nodeMap["targetX"].ID(): []IsID{nodeMap["proj1"].ID()},
					nodeMap["proj1"].ID():   []IsID{nodeMap["targetY"].ID()},
					nodeMap["targetY"].ID(): []IsID{nodeMap["arg0"].ID()},
					nodeMap["arg0"].ID():    []IsID{nodeMap["bool"].ID()},
					nodeMap["bool"].ID():    []IsID{},
					nodeMap["float"].ID():   []IsID{},
				},
				ArgumentEdges: map[IsID][]IsID{
					nodeMap["proj1"].ID(): []IsID{nodeMap["bool"].ID(), nodeMap["float"].ID()},
				},
				Nodes: map[IsID]Node{
					nodeMap["targetX"].ID(): nodeMap["targetX"],
					nodeMap["proj1"].ID():   nodeMap["proj1"],
					nodeMap["targetY"].ID(): nodeMap["targetY"],
					nodeMap["arg0"].ID():    nodeMap["arg0"],
					nodeMap["bool"].ID():    nodeMap["bool"],
					nodeMap["float"].ID():   nodeMap["float"],
				},
			},
			wantErrors: false,
		},
		{
			name: "test no projector",
			mpc: &mbp.MappingConfig{
				RootMapping: []*mbp.FieldMapping{
					&mbp.FieldMapping{
						ValueSource: &mbp.ValueSource{
							Projector: "projector",
						},
						Target: &mbp.FieldMapping_TargetField{
							TargetField: "x",
						},
					},
				},
				Projector: []*mbp.ProjectorDefinition{},
			},
			wantErrors: true,
		},
		{
			name: "test nested projectors",
			mpc: &mbp.MappingConfig{
				RootMapping: []*mbp.FieldMapping{
					&mbp.FieldMapping{
						ValueSource: &mbp.ValueSource{
							Source: &mbp.ValueSource_ProjectedValue{
								ProjectedValue: &mbp.ValueSource{
									Projector: "proj2",
								},
							},
							Projector: "proj1",
						},
						Target: &mbp.FieldMapping_TargetField{
							TargetField: "x",
						},
					},
				},
				Projector: []*mbp.ProjectorDefinition{
					&mbp.ProjectorDefinition{
						Name: "proj1",
						Mapping: []*mbp.FieldMapping{
							&mbp.FieldMapping{
								ValueSource: &mbp.ValueSource{
									Source: &mbp.ValueSource_FromInput{
										FromInput: &mbp.ValueSource_InputSource{
											Arg: 1,
										},
									},
								},
								Target: &mbp.FieldMapping_TargetField{
									TargetField: "y",
								},
							},
						},
					},
					&mbp.ProjectorDefinition{
						Name: "proj2",
						Mapping: []*mbp.FieldMapping{
							&mbp.FieldMapping{
								ValueSource: &mbp.ValueSource{
									Source: &mbp.ValueSource_ConstString{
										ConstString: "foo",
									},
								},
								Target: &mbp.FieldMapping_TargetField{
									TargetField: "z",
								},
							},
						},
					},
				},
			},
			want: Graph{
				Edges: map[IsID][]IsID{
					nodeMap["targetX"].ID(): []IsID{nodeMap["proj1"].ID()},
					nodeMap["proj1"].ID():   []IsID{nodeMap["targetY"].ID()},
					nodeMap["targetY"].ID(): []IsID{nodeMap["arg0"].ID()},
					nodeMap["arg0"].ID():    []IsID{nodeMap["proj2"].ID()},
					nodeMap["proj2"].ID():   []IsID{nodeMap["targetZ"].ID()},
					nodeMap["targetZ"].ID(): []IsID{nodeMap["string"].ID()},
					nodeMap["string"].ID():  []IsID{},
				},
				ArgumentEdges: map[IsID][]IsID{
					nodeMap["proj1"].ID(): []IsID{nodeMap["proj2"].ID()},
					nodeMap["proj2"].ID(): []IsID{},
				},
				Nodes: map[IsID]Node{
					nodeMap["targetX"].ID(): nodeMap["targetX"],
					nodeMap["proj1"].ID():   nodeMap["proj1"],
					nodeMap["targetY"].ID(): nodeMap["targetY"],
					nodeMap["arg0"].ID():    nodeMap["arg0"],
					nodeMap["proj2"].ID():   nodeMap["proj2"],
					nodeMap["targetZ"].ID(): nodeMap["targetZ"],
					nodeMap["string"].ID():  nodeMap["string"],
				},
			},
			wantErrors: false,
		},
		{
			name: "test recursive projector",
			mpc: &mbp.MappingConfig{
				RootMapping: []*mbp.FieldMapping{
					&mbp.FieldMapping{
						ValueSource: &mbp.ValueSource{
							Projector: "proj1",
						},
						Target: &mbp.FieldMapping_TargetField{
							TargetField: "x",
						},
					},
				},
				Projector: []*mbp.ProjectorDefinition{
					&mbp.ProjectorDefinition{
						Name: "proj1",
						Mapping: []*mbp.FieldMapping{
							&mbp.FieldMapping{
								ValueSource: &mbp.ValueSource{
									Projector: "proj1",
								},
								Target: &mbp.FieldMapping_TargetField{
									TargetField: "y",
								},
							},
						},
					},
				},
			},
			wantErrors: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			g, err := New(test.mpc)
			if test.wantErrors && err == nil {
				t.Errorf("expected error building graph")
			} else if !test.wantErrors && err != nil {
				t.Errorf("building graph for %v failed: %w", test.mpc, err)
			}

			if !test.wantErrors {
				if len(test.want.Nodes) != len(g.Nodes) {
					t.Errorf("expected %v nodes, but got %v; %v", len(test.want.Nodes), len(g.Nodes), g.Nodes)
				}
				for _, wantNode := range test.want.Nodes {
					if _, found := findNodeInMap(wantNode, g.Nodes); !found {
						t.Errorf("expected node {%v} to be in the graph, but it was not", wantNode)
					}
				}
				if equal, errStr := compareGraphs(test.want.Edges, g.Edges, test.want.Nodes, g.Nodes); !equal {
					t.Errorf("the graph edges are not as expected:\n%v", errStr)
				}
				if equal, errStr := compareGraphs(test.want.ArgumentEdges, g.ArgumentEdges, test.want.Nodes, g.Nodes); !equal {
					t.Errorf("the graph argument edges are not as expected:\n%v", errStr)
				}
			}
		},
		)
	}
}

func TestGetProjectedValue(t *testing.T) {
	msgMap := getMsgMap()
	tests := []struct {
		name        string
		msg         proto.Message
		want        *mbp.ValueSource
		wantSuccess bool
	}{
		{
			name:        "test projected value",
			msg:         msgMap["projectedVal"],
			want:        msgMap["bool"].(*mbp.ValueSource),
			wantSuccess: true,
		},
		{
			name:        "test wrong value source",
			msg:         msgMap["bool"],
			wantSuccess: false,
		},
		{
			name:        "test wrong msg",
			msg:         msgMap["mapping"],
			wantSuccess: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			msg, ok := getProjectedValue(test.msg)
			if !test.wantSuccess && ok {
				t.Errorf("expected failure getting projected value from msg {%v}", test.msg)
			} else if test.wantSuccess && !ok {
				t.Errorf("getting projected value for msg {%v} failed", test.msg)
			}

			if test.wantSuccess {
				if !cmp.Equal(test.want, msg, protocmp.Transform()) {
					t.Errorf("expected msg %v, but got %v", test.want, msg)
				}
			}
		},
		)
	}
}

func TestAddArgLineages(t *testing.T) {
	nodeMap := getNodeMap()
	msgMap := getMsgMap()
	tests := []struct {
		name       string
		e          *env
		graph      Graph
		argMsgs    []proto.Message
		projNode   Node
		projMsg    *mbp.ProjectorDefinition
		projectors map[string]*mbp.ProjectorDefinition
		want       *env
		wantErrors bool
	}{
		{
			name: "add argument",
			e: &env{
				name: "root",
			},
			graph: Graph{
				Edges: map[IsID][]IsID{
					nodeMap["proj1"].ID(): []IsID{},
				},
				ArgumentEdges: map[IsID][]IsID{
					nodeMap["proj1"].ID(): []IsID{},
				},
				Nodes: map[IsID]Node{
					nodeMap["proj1"].ID(): nodeMap["proj1"],
				},
			},
			argMsgs:  []proto.Message{msgMap["bool"], msgMap["int"]},
			projNode: nodeMap["proj1"],
			projMsg:  msgMap["proj1"].(*mbp.ProjectorDefinition),
			projectors: map[string]*mbp.ProjectorDefinition{
				"proj1": msgMap["proj1"].(*mbp.ProjectorDefinition),
			},
			want: &env{
				name: "proj1",
				args: []proto.Message{msgMap["bool"], msgMap["int"]},
				argLookup: map[proto.Message]Node{
					msgMap["bool"]: nodeMap["bool"],
					msgMap["int"]:  nodeMap["int"],
				},
			},
			wantErrors: false,
		},
		{
			name: "add projected value argument",
			e: &env{
				name: "root",
			},
			graph: Graph{
				Edges: map[IsID][]IsID{
					nodeMap["proj1"].ID(): []IsID{},
				},
				ArgumentEdges: map[IsID][]IsID{
					nodeMap["proj1"].ID(): []IsID{},
				},
				Nodes: map[IsID]Node{
					nodeMap["proj1"].ID(): nodeMap["proj1"],
				},
			},
			argMsgs:  []proto.Message{msgMap["projVal2"]},
			projNode: nodeMap["proj1"],
			projMsg:  msgMap["proj1"].(*mbp.ProjectorDefinition),
			projectors: map[string]*mbp.ProjectorDefinition{
				"proj1":      msgMap["proj1"].(*mbp.ProjectorDefinition),
				"proj_empty": msgMap["proj_empty"].(*mbp.ProjectorDefinition),
			},
			want: &env{
				name: "proj1",
				args: []proto.Message{msgMap["projVal2"]},
				argLookup: map[proto.Message]Node{
					msgMap["projVal2"]: nodeMap["proj_empty"],
				},
			},
			wantErrors: false,
		},
		{
			name: "no projector",
			e: &env{
				name: "root",
			},
			graph: Graph{
				Edges: map[IsID][]IsID{
					nodeMap["proj1"].ID(): []IsID{},
				},
				ArgumentEdges: map[IsID][]IsID{
					nodeMap["proj1"].ID(): []IsID{},
				},
				Nodes: map[IsID]Node{
					nodeMap["proj1"].ID(): nodeMap["proj1"],
				},
			},
			argMsgs:  []proto.Message{msgMap["projVal2"]},
			projNode: nodeMap["proj1"],
			projMsg:  msgMap["proj1"].(*mbp.ProjectorDefinition),
			projectors: map[string]*mbp.ProjectorDefinition{
				"proj1": msgMap["proj1"].(*mbp.ProjectorDefinition),
			},
			want: &env{
				name: "proj1",
				args: []proto.Message{msgMap["projVal2"]},
				argLookup: map[proto.Message]Node{
					msgMap["projVal2"]: nodeMap["proj_empty"],
				},
			},
			wantErrors: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			e, err := test.graph.addArgLineages(test.e, test.argMsgs, test.projNode, test.projMsg, test.projectors)
			if test.wantErrors && err == nil {
				t.Errorf("expected error getting argument lineages")
			} else if !test.wantErrors && err != nil {
				t.Errorf("adding argument lineages for %v failed:\n%w", test.argMsgs, err)
			}

			if !test.wantErrors {
				if test.want.name != e.name {
					t.Errorf("expected env named %v, but got %v", test.want.name, e.name)
				}
				if len(test.want.args) != len(e.args) {
					t.Errorf("expected %v arguments, but got %v; %v", len(test.want.args), len(e.args), e.args)
				}
				for i, wantMsg := range test.want.args {
					if !cmp.Equal(wantMsg, e.args[i], protocmp.Transform()) {
						t.Errorf("expected msg {%v}, but got %v", wantMsg, e.args[i])
					}

					var node Node
					var ok bool
					if node, ok = e.argLookup[e.args[i]]; !ok {
						t.Errorf("expected msg {%v} to be in the argLookup", e.args[i])
					}

					if !EqualsIgnoreID(test.want.argLookup[wantMsg], node) {
						t.Errorf("expected node {%v}, but got {%v}", test.want.argLookup[wantMsg], node)
					}
				}
			}
		},
		)
	}
}

func TestAddNode(t *testing.T) {
	nodeMap := getNodeMap()
	tests := []struct {
		name       string
		graph      Graph
		node       Node
		descendant Node
		isArg      bool
		isNew      bool
		want       Graph
		wantErrors bool
	}{
		{
			name: "new no descendant",
			graph: Graph{
				Edges: map[IsID][]IsID{},
				Nodes: map[IsID]Node{},
			},
			node:  nodeMap["bool"],
			isArg: false,
			isNew: true,
			want: Graph{
				Edges: map[IsID][]IsID{
					nodeMap["bool"].ID(): []IsID{},
				},
				Nodes: map[IsID]Node{
					nodeMap["bool"].ID(): nodeMap["bool"],
				},
			},
			wantErrors: false,
		},
		{
			name: "new ProjectorNdoe no descendant",
			graph: Graph{
				Edges:         map[IsID][]IsID{},
				ArgumentEdges: map[IsID][]IsID{},
				Nodes:         map[IsID]Node{},
			},
			node:  nodeMap["proj1"],
			isArg: false,
			isNew: true,
			want: Graph{
				Edges: map[IsID][]IsID{
					nodeMap["proj1"].ID(): []IsID{},
				},
				ArgumentEdges: map[IsID][]IsID{
					nodeMap["proj1"].ID(): []IsID{},
				},
				Nodes: map[IsID]Node{
					nodeMap["proj1"].ID(): nodeMap["proj1"],
				},
			},
			wantErrors: false,
		},
		{
			name: "new already in graph",
			graph: Graph{
				Edges: map[IsID][]IsID{
					nodeMap["bool"].ID(): []IsID{},
				},
				Nodes: map[IsID]Node{
					nodeMap["bool"].ID(): nodeMap["bool"],
				},
			},
			node:  nodeMap["bool"],
			isArg: false,
			isNew: true,
			want: Graph{
				Edges: map[IsID][]IsID{
					nodeMap["bool"].ID(): []IsID{},
				},
				Nodes: map[IsID]Node{
					nodeMap["bool"].ID(): nodeMap["bool"],
				},
			},
			wantErrors: true,
		},
		{
			name: "new with descendant",
			graph: Graph{
				Edges: map[IsID][]IsID{
					nodeMap["targetX"].ID(): []IsID{},
				},
				Nodes: map[IsID]Node{
					nodeMap["targetX"].ID(): nodeMap["targetX"],
				},
			},
			node:       nodeMap["bool"],
			descendant: nodeMap["targetX"],
			isArg:      false,
			isNew:      true,
			want: Graph{
				Edges: map[IsID][]IsID{
					nodeMap["targetX"].ID(): []IsID{nodeMap["bool"].ID()},
					nodeMap["bool"].ID():    []IsID{},
				},
				Nodes: map[IsID]Node{
					nodeMap["targetX"].ID(): nodeMap["targetX"],
					nodeMap["bool"].ID():    nodeMap["bool"],
				},
			},
			wantErrors: false,
		},
		{
			name: "new descendant not in graph",
			graph: Graph{
				Edges: map[IsID][]IsID{},
				Nodes: map[IsID]Node{},
			},
			node:       nodeMap["bool"],
			descendant: nodeMap["targetX"],
			isArg:      false,
			isNew:      true,
			want:       Graph{},
			wantErrors: true,
		},
		{
			name: "old no descendant",
			graph: Graph{
				Edges: map[IsID][]IsID{
					nodeMap["targetX"].ID(): []IsID{},
				},
				Nodes: map[IsID]Node{
					nodeMap["targetX"].ID(): nodeMap["targetX"],
				},
			},
			node:  nodeMap["targetX"],
			isArg: false,
			isNew: false,
			want: Graph{
				Edges: map[IsID][]IsID{
					nodeMap["targetX"].ID(): []IsID{},
				},
				Nodes: map[IsID]Node{
					nodeMap["targetX"].ID(): nodeMap["targetX"],
				},
			},
			wantErrors: false,
		},
		{
			name: "old descendant",
			graph: Graph{
				Edges: map[IsID][]IsID{
					nodeMap["targetX"].ID(): []IsID{},
					nodeMap["bool"].ID():    []IsID{},
				},
				Nodes: map[IsID]Node{
					nodeMap["targetX"].ID(): nodeMap["targetX"],
					nodeMap["bool"].ID():    nodeMap["bool"],
				},
			},
			node:       nodeMap["bool"],
			descendant: nodeMap["targetX"],
			isArg:      false,
			isNew:      false,
			want: Graph{
				Edges: map[IsID][]IsID{
					nodeMap["targetX"].ID(): []IsID{nodeMap["bool"].ID()},
					nodeMap["bool"].ID():    []IsID{},
				},
				Nodes: map[IsID]Node{
					nodeMap["targetX"].ID(): nodeMap["targetX"],
					nodeMap["bool"].ID():    nodeMap["bool"],
				},
			},
			wantErrors: false,
		},
		{
			name: "old isArg descendant",
			graph: Graph{
				Edges: map[IsID][]IsID{
					nodeMap["proj1"].ID(): []IsID{},
					nodeMap["bool"].ID():  []IsID{},
				},
				ArgumentEdges: map[IsID][]IsID{
					nodeMap["proj1"].ID(): []IsID{},
				},
				Nodes: map[IsID]Node{
					nodeMap["proj1"].ID(): nodeMap["proj1"],
					nodeMap["bool"].ID():  nodeMap["bool"],
				},
			},
			node:       nodeMap["bool"],
			descendant: nodeMap["proj1"],
			isArg:      true,
			isNew:      false,
			want: Graph{
				Edges: map[IsID][]IsID{
					nodeMap["proj1"].ID(): []IsID{},
					nodeMap["bool"].ID():  []IsID{},
				},
				ArgumentEdges: map[IsID][]IsID{
					nodeMap["proj1"].ID(): []IsID{nodeMap["bool"].ID()},
				},
				Nodes: map[IsID]Node{
					nodeMap["proj1"].ID(): nodeMap["proj1"],
					nodeMap["bool"].ID():  nodeMap["bool"],
				},
			},
			wantErrors: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := addNode(test.graph, test.node, test.descendant, test.isArg, test.isNew)
			graph := test.graph
			if test.wantErrors && err == nil {
				t.Errorf("expected errors adding node {%v}", test.node)
			} else if !test.wantErrors && err != nil {
				t.Errorf("adding node {%v} failed:\n%w", test.node, err)
			}

			if !test.wantErrors {
				if len(test.want.Nodes) != len(graph.Nodes) {
					t.Errorf("expected %v nodes, but got %v; %v", len(test.want.Nodes), len(graph.Nodes), graph.Nodes)
				}
				for _, wantNode := range test.want.Nodes {
					if _, found := findNodeInMap(wantNode, graph.Nodes); !found {
						t.Errorf("expected node {%v} to be in the graph, but it was not", wantNode)
					}
				}
				if equal, errStr := compareGraphs(test.want.Edges, graph.Edges, test.want.Nodes, graph.Nodes); !equal {
					t.Errorf("the graph edges are not as expected:\n%v", errStr)
				}
				if equal, errStr := compareGraphs(test.want.ArgumentEdges, graph.ArgumentEdges, test.want.Nodes, graph.Nodes); !equal {
					t.Errorf("the graph argument edges are not as expected:\n%v", errStr)
				}
			}
		},
		)
	}
}

func TestGetNode(t *testing.T) {
	msgMap := getMsgMap()
	tests := []struct {
		name           string
		msg            proto.Message
		descendantNode Node
		argLookup      map[proto.Message]Node
		wantErrors     bool
		want           Node
		wantNew        bool
	}{
		{
			name:           "test new node",
			descendantNode: &TargetNode{},
			msg:            msgMap["bool"],
			want: &ConstBoolNode{
				id:    intID(0),
				Value: true,
			},
			argLookup:  map[proto.Message]Node{},
			wantNew:    true,
			wantErrors: false,
		},
		{
			name:           "test new with argLookup",
			descendantNode: &TargetNode{},
			msg:            msgMap["float"],
			want: &ConstFloatNode{
				id:    intID(0),
				Value: 5.0,
			},
			argLookup: map[proto.Message]Node{
				msgMap["float"]: &ConstFloatNode{
					id:    intID(0),
					Value: 5.0,
				},
			},
			wantNew:    true,
			wantErrors: false,
		},
		{
			name:           "test lookup",
			descendantNode: &ArgumentNode{},
			msg:            msgMap["int"],
			want: &ConstIntNode{
				id:    intID(0),
				Value: 0,
			},
			argLookup: map[proto.Message]Node{
				msgMap["int"]: &ConstIntNode{
					id:    intID(0),
					Value: 0,
				},
			},
			wantNew:    false,
			wantErrors: false,
		},
		{
			name:           "test lookup wrong argLookup",
			descendantNode: &ArgumentNode{},
			msg:            msgMap["string"],
			argLookup: map[proto.Message]Node{
				msgMap["float"]: &ConstStringNode{
					id:    intID(0),
					Value: "foo",
				},
			},
			want: &ConstStringNode{
				id:    intID(0),
				Value: "foo",
			},
			wantNew:    false,
			wantErrors: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			node, isNew, err := getNode(test.msg, test.descendantNode, test.argLookup)
			if test.wantErrors && err == nil {
				t.Errorf("expected errors getting node for msg {%v}", test.msg)
			} else if !test.wantErrors && err != nil {
				t.Errorf("getting node for msg {%v} failed:\n%w", test.msg, err)
			}

			if !test.wantErrors {
				if test.wantNew != isNew {
					t.Errorf("wanted to make a new node: %v. getNode made a new node: %v. bools should agree.", test.wantNew, isNew)
				}
				if !EqualsIgnoreID(test.want, node) {
					t.Errorf("wanted node %v but got node %v", test.want, node)
				}
			}
		},
		)
	}
}

func TestNewNode(t *testing.T) {
	msgMap := getMsgMap()
	tests := []struct {
		name       string
		msg        proto.Message
		wantErrors bool
		want       Node
	}{
		{
			name: "test new constant bool",
			msg:  msgMap["bool"],
			want: &ConstBoolNode{
				id:    intID(0),
				Value: true,
			},
			wantErrors: false,
		},
		{
			name: "test new constant int",
			msg:  msgMap["int"],
			want: &ConstIntNode{
				id:    intID(0),
				Value: 0,
			},
			wantErrors: false,
		},
		{
			name: "test new constant float",
			msg:  msgMap["float"],
			want: &ConstFloatNode{
				id:    intID(0),
				Value: 5.0,
			},
			wantErrors: false,
		},
		{
			name: "test new constant string",
			msg:  msgMap["string"],
			want: &ConstStringNode{
				id:    intID(0),
				Value: "foo",
			},
			wantErrors: false,
		},
		{
			name: "test new field mapping",
			msg:  msgMap["mapping"],
			want: &TargetNode{
				id:   intID(0),
				Name: "x",
			},
			wantErrors: false,
		},
		{
			name: "test new projector",
			msg:  msgMap["proj1"],
			want: &ProjectorNode{
				id:   intID(0),
				Name: "proj1",
			},
			wantErrors: false,
		},
		{
			name: "test new argument",
			msg:  msgMap["arg0"],
			want: &ArgumentNode{
				id:    intID(0),
				Index: 1,
			},
			wantErrors: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			node, err := newNode(test.msg)
			if test.wantErrors && err == nil {
				t.Errorf("expected errors getting node for msg {%v}", test.msg)
			} else if !test.wantErrors && err != nil {
				t.Errorf("getting node for msg {%v} failed:\n%w", test.msg, err)
			}

			if !test.wantErrors {
				if !EqualsIgnoreID(test.want, node) {
					t.Errorf("wanted node %v but got node %v", test.want, node)
				}
			}
		},
		)
	}
}

// Test only that the switch delegates correctly
func TestGetAncestors(t *testing.T) {
	msgMap := getMsgMap()
	tests := []struct {
		name       string
		msg        proto.Message
		want       proto.Message
		wantErrors bool
	}{
		{
			name:       "test FieldMapping",
			msg:        msgMap["mapping"],
			want:       msgMap["bool"],
			wantErrors: false,
		},
		{
			name:       "test ValueSource",
			msg:        msgMap["bool"],
			want:       nil,
			wantErrors: false,
		},
		{
			name:       "test ProjectorDefinition",
			msg:        msgMap["proj1"],
			want:       msgMap["mapping"],
			wantErrors: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ancestors, err := getAncestors(nil, test.msg, nil)
			if test.wantErrors && err == nil {
				t.Errorf("expected errors getting ancestors for msg {%v}", test.msg)
			} else if !test.wantErrors && err != nil {
				t.Errorf("getting ancestors for msg {%v} failed:\n%w", test.msg, err)
			}

			if !test.wantErrors {
				if test.want == nil && len(ancestors) != 0 {
					t.Errorf("expected zero ancestors, but got %v", ancestors)
				} else if test.want != nil && len(ancestors) == 0 {
					t.Errorf("expected ancestor %v, but got none", test.want)
				} else if test.want != nil && !cmp.Equal(test.want, ancestors[0], protocmp.Transform()) {
					t.Errorf("expected msg %v, but got %v", test.want, ancestors[0])
				}
			}
		},
		)
	}
}

func TestFieldMappingAncestors(t *testing.T) {
	msgMap := getMsgMap()
	tests := []struct {
		name       string
		msg        *mbp.FieldMapping
		projectors map[string]*mbp.ProjectorDefinition
		want       []proto.Message
		wantErrors bool
	}{
		{
			name:       "test constant source",
			msg:        msgMap["mapping"].(*mbp.FieldMapping),
			want:       []proto.Message{msgMap["bool"]},
			wantErrors: false,
		},
		{
			name: "test projector source",
			msg:  msgMap["mapping_proj1"].(*mbp.FieldMapping),
			projectors: map[string]*mbp.ProjectorDefinition{
				"proj1": msgMap["proj1"].(*mbp.ProjectorDefinition),
			},
			want:       []proto.Message{msgMap["proj1"]},
			wantErrors: false,
		},
		{
			name:       "test missing projector",
			msg:        msgMap["mapping_proj1"].(*mbp.FieldMapping),
			projectors: map[string]*mbp.ProjectorDefinition{},
			want:       nil,
			wantErrors: true,
		},
		{
			name:       "nil source",
			msg:        msgMap["mapping_nil"].(*mbp.FieldMapping),
			want:       []proto.Message{},
			wantErrors: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ancestors, err := fieldMappingAncestors(test.msg, test.projectors)
			if test.wantErrors && err == nil {
				t.Errorf("expected errors getting ancestors for msg {%v}", test.msg)
			} else if !test.wantErrors && err != nil {
				t.Errorf("getting ancestors for msg {%v} failed:\n%w", test.msg, err)
			}

			if !test.wantErrors {
				if len(test.want) != len(ancestors) {
					t.Errorf("expected %v ancestors, but got %v: %v", len(test.want), len(ancestors), ancestors)
				}
				for i := range test.want {
					if !cmp.Equal(test.want[i], ancestors[i], protocmp.Transform()) {
						t.Errorf("expected msg %v, but got %v", test.want[i], ancestors[i])
					}
				}
			}
		},
		)
	}
}

func TestValueSourceAncestors(t *testing.T) {
	msgMap := getMsgMap()
	tests := []struct {
		name       string
		e          *env
		msg        *mbp.ValueSource
		want       []proto.Message
		wantErrors bool
	}{
		{
			name:       "test bool",
			msg:        msgMap["bool"].(*mbp.ValueSource),
			want:       []proto.Message{},
			wantErrors: false,
		},
		{
			name:       "test float",
			msg:        msgMap["float"].(*mbp.ValueSource),
			want:       []proto.Message{},
			wantErrors: false,
		},
		{
			name:       "test int",
			msg:        msgMap["int"].(*mbp.ValueSource),
			want:       []proto.Message{},
			wantErrors: false,
		},
		{
			name:       "test string",
			msg:        msgMap["string"].(*mbp.ValueSource),
			want:       []proto.Message{},
			wantErrors: false,
		},
		{
			name: "test Argument 0",
			e: &env{
				name:   "proj1",
				parent: nil,
				args:   []proto.Message{msgMap["bool"]},
			},
			msg:        msgMap["arg0"].(*mbp.ValueSource),
			want:       []proto.Message{msgMap["bool"]},
			wantErrors: false,
		},
		{
			name: "test Argument 1",
			e: &env{
				name:   "proj1",
				parent: nil,
				args:   []proto.Message{msgMap["int"], msgMap["bool"]},
			},
			msg:        msgMap["arg1"].(*mbp.ValueSource),
			want:       []proto.Message{msgMap["bool"]},
			wantErrors: false,
		},
		{
			name: "test Argument too high",
			e: &env{
				name:   "proj1",
				parent: nil,
				args:   []proto.Message{msgMap["int"]},
			},
			msg:        msgMap["arg1"].(*mbp.ValueSource),
			want:       nil,
			wantErrors: true,
		},
		{
			name: "test Argument too low",
			e: &env{
				name:   "proj1",
				parent: nil,
				args:   []proto.Message{msgMap["int"]},
			},
			msg: &mbp.ValueSource{
				Source: &mbp.ValueSource_FromInput{
					FromInput: &mbp.ValueSource_InputSource{
						Arg: 0,
					},
				},
			},
			want:       nil,
			wantErrors: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ancestors, err := valueSourceAncestors(test.e, test.msg)
			if test.wantErrors && err == nil {
				t.Errorf("expected errors getting ancestors for msg {%v}", test.msg)
			} else if !test.wantErrors && err != nil {
				t.Errorf("getting ancestors for msg {%v} failed:\n%w", test.msg, err)
			}

			if !test.wantErrors {
				if len(test.want) != len(ancestors) {
					t.Errorf("expected %v ancestors, but got %v: %v", len(test.want), len(ancestors), ancestors)
				}
				for i := range test.want {
					if !cmp.Equal(test.want[i], ancestors[i], protocmp.Transform()) {
						t.Errorf("expected msg %v, but got %v", test.want[i], ancestors[i])
					}
				}
			}
		},
		)
	}
}

func TestProjectorAncestors(t *testing.T) {
	msgMap := getMsgMap()
	tests := []struct {
		name string
		msg  *mbp.ProjectorDefinition
		want []proto.Message
	}{
		{
			name: "test with ancestors",
			msg:  msgMap["proj1"].(*mbp.ProjectorDefinition),
			want: []proto.Message{msgMap["mapping"]},
		},
		{
			name: "test no mappings",
			msg:  msgMap["proj_empty"].(*mbp.ProjectorDefinition),
			want: []proto.Message{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ancestors := projectorAncestors(test.msg)
			if len(test.want) != len(ancestors) {
				t.Errorf("expected %v ancestors, but got %v: %v", len(test.want), len(ancestors), ancestors)
			}
			for i := range test.want {
				if !cmp.Equal(test.want[i], ancestors[i], protocmp.Transform()) {
					t.Errorf("expected msg %v, but got %v", test.want[i], ancestors[i])
				}
			}
		},
		)
	}
}

func TestProjectorArgs(t *testing.T) {
	msgMap := getMsgMap()
	tests := []struct {
		name       string
		msg        proto.Message
		want       []proto.Message
		wantErrors bool
	}{
		{
			name:       "test no args fieldmapping",
			msg:        msgMap["mapping_proj1"],
			want:       []proto.Message{},
			wantErrors: false,
		},
		{
			name:       "test args fieldmapping",
			msg:        msgMap["mapping_proj2"],
			want:       []proto.Message{msgMap["source_args"], msgMap["int"]},
			wantErrors: false,
		},
		{
			name:       "test no args valuesource",
			msg:        msgMap["source_noargs"],
			want:       []proto.Message{},
			wantErrors: false,
		},
		{
			name:       "test args valuesource",
			msg:        msgMap["source_args"],
			want:       []proto.Message{msgMap["source_args"], msgMap["int"]},
			wantErrors: false,
		},
		{
			name:       "test wrong message type",
			msg:        msgMap["arg0"],
			wantErrors: true,
		},
		{
			name:       "test no projector",
			msg:        msgMap["bool"],
			wantErrors: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ancestors, err := projectorArgs(test.msg)
			if test.wantErrors && err == nil {
				t.Errorf("expected errors getting args for msg {%v}", test.msg)
			} else if !test.wantErrors && err != nil {
				t.Errorf("getting args for msg {%v} failed:\n%w", test.msg, err)
			}

			if !test.wantErrors {
				if len(test.want) != len(ancestors) {
					t.Errorf("expected %v args, but got %v: %v", len(test.want), len(ancestors), ancestors)
				}
				for i := range test.want {
					if !cmp.Equal(test.want[i], ancestors[i], protocmp.Transform()) {
						t.Errorf("expected msg %v, but got %v", test.want[i], ancestors[i])
					}
				}
			}
		},
		)
	}
}
