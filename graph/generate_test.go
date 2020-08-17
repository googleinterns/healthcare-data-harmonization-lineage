package graph

import (
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

func TestNew_Whistle(t *testing.T) {
	setIDfactory(&autoIncFactory{currentID: 0})

	nodeList := [][]Node{
		[]Node{
			&TargetNode{
				id:   idFactory.New(),
				Name: "x",
			},
			&ConstBoolNode{
				id:    idFactory.New(),
				Value: true,
			},
		},
		[]Node{},
		[]Node{
			&TargetNode{
				id:   idFactory.New(),
				Name: "x",
			},
			&ProjectorNode{
				id:   idFactory.New(),
				Name: "projector",
			},
			&TargetNode{
				id:   idFactory.New(),
				Name: "y",
			},
			&ConstFloatNode{
				id:    idFactory.New(),
				Value: 5,
			},
		},
		[]Node{
			&TargetNode{
				id:   idFactory.New(),
				Name: "x",
			},
			&ProjectorNode{
				id:   idFactory.New(),
				Name: "projector",
			},
			&TargetNode{
				id:   idFactory.New(),
				Name: "y",
			},
			&ArgumentNode{
				id:    idFactory.New(),
				Index: 1,
				Field: ".x",
			},
		},
		[]Node{
			&TargetNode{
				id:   idFactory.New(),
				Name: "x",
			},
		},
	}

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
				Graph: map[IsID][]IsID{
					nodeList[0][0].ID(): []IsID{nodeList[0][1].ID()},
					nodeList[0][1].ID(): []IsID{},
				},
				Nodes: map[IsID]Node{
					nodeList[0][0].ID(): nodeList[0][0],
					nodeList[0][1].ID(): nodeList[0][1],
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
			x: projector()
			def projector() {
				y: 5
			}
			`,
			want: Graph{
				Graph: map[IsID][]IsID{
					nodeList[2][0].ID(): []IsID{nodeList[2][1].ID()},
					nodeList[2][1].ID(): []IsID{nodeList[2][2].ID()},
					nodeList[2][2].ID(): []IsID{nodeList[2][3].ID()},
					nodeList[2][3].ID(): []IsID{},
				},
				Nodes: map[IsID]Node{
					nodeList[2][0].ID(): nodeList[2][0],
					nodeList[2][1].ID(): nodeList[2][1],
					nodeList[2][2].ID(): nodeList[2][2],
					nodeList[2][3].ID(): nodeList[2][3],
				},
			},
			wantErrors: false,
		},
		{
			name: "test projector arguments",
			whistle: `
			x: projector(true, 1.0)
			def projector(arg1, arg2) {
				y: arg1.x
			}
			`,
			want: Graph{
				Graph: map[IsID][]IsID{
					nodeList[3][0].ID(): []IsID{nodeList[3][1].ID()},
					nodeList[3][1].ID(): []IsID{nodeList[3][2].ID()},
					nodeList[3][2].ID(): []IsID{nodeList[3][3].ID()},
					nodeList[3][3].ID(): []IsID{},
				},
				Nodes: map[IsID]Node{
					nodeList[3][0].ID(): nodeList[3][0],
					nodeList[3][1].ID(): nodeList[3][1],
					nodeList[3][2].ID(): nodeList[3][2],
					nodeList[3][3].ID(): nodeList[3][3],
				},
			},
			wantErrors: false,
		},
	}

	setIDfactory(&autoIncFactory{currentID: 0})
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

				for wantID, wantNode := range test.want.Nodes {
					node, ok := g.Nodes[wantID]
					if !ok {
						t.Errorf("expected node %v to be in the graph. got %v", wantNode, g.Nodes)
					}
					if !node.Equals(wantNode) {
						t.Errorf("expected node %v but got %v", wantNode, node)
					}
					for _, wantAncestorID := range test.want.Graph[wantID] {
						wantAncestor := test.want.Nodes[wantAncestorID]
						ancestor := g.Nodes[wantAncestorID]
						if !wantAncestor.Equals(ancestor) {
							t.Errorf("expected ancestor %v but got %v", wantAncestor, ancestor)
						}
					}
				}
			}
		},
		)
	}
}

func TestNew_Whistler(t *testing.T) {
	setIDfactory(&autoIncFactory{currentID: 0})

	nodeList := [][]Node{
		[]Node{
			&TargetNode{
				id:   idFactory.New(),
				Name: "x",
			},
			&ConstBoolNode{
				id:    idFactory.New(),
				Value: true,
			},
		},
		[]Node{},
		[]Node{
			&TargetNode{
				id:   idFactory.New(),
				Name: "x",
			},
			&ProjectorNode{
				id:   idFactory.New(),
				Name: "projector",
			},
			&TargetNode{
				id:   idFactory.New(),
				Name: "y",
			},
			&ConstIntNode{
				id:    idFactory.New(),
				Value: 5,
			},
		},
		[]Node{
			&TargetNode{
				id:   idFactory.New(),
				Name: "x",
			},
			&ProjectorNode{
				id:   idFactory.New(),
				Name: "projector",
			},
			&TargetNode{
				id:   idFactory.New(),
				Name: "y",
			},
			&ArgumentNode{
				id:    idFactory.New(),
				Index: 1,
				Field: ".x",
			},
		},
		[]Node{
			&TargetNode{
				id:   idFactory.New(),
				Name: "x",
			},
		},
	}

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
				Graph: map[IsID][]IsID{
					nodeList[0][0].ID(): []IsID{nodeList[0][1].ID()},
					nodeList[0][1].ID(): []IsID{},
				},
				Nodes: map[IsID]Node{
					nodeList[0][0].ID(): nodeList[0][0],
					nodeList[0][1].ID(): nodeList[0][1],
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
							Projector: "projector",
						},
						Target: &mbp.FieldMapping_TargetField{
							TargetField: "x",
						},
					},
				},
				Projector: []*mbp.ProjectorDefinition{
					&mbp.ProjectorDefinition{
						Name: "projector",
						Mapping: []*mbp.FieldMapping{
							&mbp.FieldMapping{
								ValueSource: &mbp.ValueSource{
									Source: &mbp.ValueSource_ConstInt{
										ConstInt: 5,
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
				Graph: map[IsID][]IsID{
					nodeList[2][0].ID(): []IsID{nodeList[2][1].ID()},
					nodeList[2][1].ID(): []IsID{nodeList[2][2].ID()},
					nodeList[2][2].ID(): []IsID{nodeList[2][3].ID()},
					nodeList[2][3].ID(): []IsID{},
				},
				Nodes: map[IsID]Node{
					nodeList[2][0].ID(): nodeList[2][0],
					nodeList[2][1].ID(): nodeList[2][1],
					nodeList[2][2].ID(): nodeList[2][2],
					nodeList[2][3].ID(): nodeList[2][3],
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
										ConstFloat: 1.0,
									},
								},
							},
							Projector: "projector",
						},
						Target: &mbp.FieldMapping_TargetField{
							TargetField: "x",
						},
					},
				},
				Projector: []*mbp.ProjectorDefinition{
					&mbp.ProjectorDefinition{
						Name: "projector",
						Mapping: []*mbp.FieldMapping{
							&mbp.FieldMapping{
								ValueSource: &mbp.ValueSource{
									Source: &mbp.ValueSource_FromInput{
										FromInput: &mbp.ValueSource_InputSource{
											Arg:   1,
											Field: ".x",
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
				Graph: map[IsID][]IsID{
					nodeList[3][0].ID(): []IsID{nodeList[3][1].ID()},
					nodeList[3][1].ID(): []IsID{nodeList[3][2].ID()},
					nodeList[3][2].ID(): []IsID{nodeList[3][3].ID()},
					nodeList[3][3].ID(): []IsID{},
				},
				Nodes: map[IsID]Node{
					nodeList[3][0].ID(): nodeList[3][0],
					nodeList[3][1].ID(): nodeList[3][1],
					nodeList[3][2].ID(): nodeList[3][2],
					nodeList[3][3].ID(): nodeList[3][3],
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
	}

	setIDfactory(&autoIncFactory{currentID: 0})
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

				for wantID, wantNode := range test.want.Nodes {
					node, ok := g.Nodes[wantID]
					if !ok {
						t.Errorf("expected node %v to be in the graph. got %v", wantNode, g.Nodes)
					}
					if !node.Equals(wantNode) {
						t.Errorf("expected node %v but got %v", wantNode, node)
					}
					for _, wantAncestorID := range test.want.Graph[wantID] {
						wantAncestor := test.want.Nodes[wantAncestorID]
						ancestor := g.Nodes[wantAncestorID]
						if !wantAncestor.Equals(ancestor) {
							t.Errorf("expected ancestor %v but got %v", wantAncestor, ancestor)
						}
					}
				}
			}
		},
		)
	}
}

func TestDFSgraphBuild(t *testing.T) {
	setIDfactory(&autoIncFactory{currentID: 0})

	nodeList := [][]Node{
		[]Node{
			&TargetNode{
				id:   idFactory.New(),
				Name: "x",
			},
			&ConstBoolNode{
				id:    idFactory.New(),
				Value: true,
			},
		},
		[]Node{},
		[]Node{
			&TargetNode{
				id:   idFactory.New(),
				Name: "x",
			},
			&ProjectorNode{
				id:   idFactory.New(),
				Name: "projector",
			},
			&TargetNode{
				id:   idFactory.New(),
				Name: "y",
			},
			&ConstIntNode{
				id:    idFactory.New(),
				Value: 5,
			},
		},
		[]Node{
			&TargetNode{
				id:   idFactory.New(),
				Name: "x",
			},
			&ProjectorNode{
				id:   idFactory.New(),
				Name: "projector",
			},
			&TargetNode{
				id:   idFactory.New(),
				Name: "y",
			},
			&ArgumentNode{
				id:    idFactory.New(),
				Index: 1,
				Field: ".x",
			},
		},
		[]Node{
			&TargetNode{
				id:   idFactory.New(),
				Name: "x",
			},
		},
	}

	tests := []struct {
		name       string
		frontier   *dfsBuildStack
		projectors map[string]*mbp.ProjectorDefinition
		want       Graph
		wantErrors bool
	}{
		{
			name: "test constant mapping",
			frontier: &dfsBuildStack{
				stack: []msgContext{
					msgContext{
						msg: &mbp.FieldMapping{
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
			},
			want: Graph{
				Graph: map[IsID][]IsID{
					nodeList[0][0].ID(): []IsID{nodeList[0][1].ID()},
					nodeList[0][1].ID(): []IsID{},
				},
				Nodes: map[IsID]Node{
					nodeList[0][0].ID(): nodeList[0][0],
					nodeList[0][1].ID(): nodeList[0][1],
				},
			},
			wantErrors: false,
		},
		{
			name:       "test empty mapping",
			frontier:   &dfsBuildStack{},
			want:       Graph{},
			wantErrors: false,
		},
		{
			name: "test projector",
			frontier: &dfsBuildStack{
				stack: []msgContext{
					msgContext{
						msg: &mbp.FieldMapping{
							ValueSource: &mbp.ValueSource{
								Projector: "projector",
							},
							Target: &mbp.FieldMapping_TargetField{
								TargetField: "x",
							},
						},
					},
				},
			},
			projectors: map[string]*mbp.ProjectorDefinition{
				"projector": &mbp.ProjectorDefinition{
					Name: "projector",
					Mapping: []*mbp.FieldMapping{
						&mbp.FieldMapping{
							ValueSource: &mbp.ValueSource{
								Source: &mbp.ValueSource_ConstInt{
									ConstInt: 5,
								},
							},
							Target: &mbp.FieldMapping_TargetField{
								TargetField: "y",
							},
						},
					},
				},
			},
			want: Graph{
				Graph: map[IsID][]IsID{
					nodeList[2][0].ID(): []IsID{nodeList[2][1].ID()},
					nodeList[2][1].ID(): []IsID{nodeList[2][2].ID()},
					nodeList[2][2].ID(): []IsID{nodeList[2][3].ID()},
					nodeList[2][3].ID(): []IsID{},
				},
				Nodes: map[IsID]Node{
					nodeList[2][0].ID(): nodeList[2][0],
					nodeList[2][1].ID(): nodeList[2][1],
					nodeList[2][2].ID(): nodeList[2][2],
					nodeList[2][3].ID(): nodeList[2][3],
				},
			},
			wantErrors: false,
		},
		{
			name: "test projector arguments",
			frontier: &dfsBuildStack{
				stack: []msgContext{
					msgContext{
						msg: &mbp.FieldMapping{
							ValueSource: &mbp.ValueSource{
								Source: &mbp.ValueSource_ConstBool{
									ConstBool: true,
								},
								AdditionalArg: []*mbp.ValueSource{
									&mbp.ValueSource{
										Source: &mbp.ValueSource_ConstFloat{
											ConstFloat: 1.0,
										},
									},
								},
								Projector: "projector",
							},
							Target: &mbp.FieldMapping_TargetField{
								TargetField: "x",
							},
						},
					},
				},
			},
			projectors: map[string]*mbp.ProjectorDefinition{
				"projector": &mbp.ProjectorDefinition{
					Name: "projector",
					Mapping: []*mbp.FieldMapping{
						&mbp.FieldMapping{
							ValueSource: &mbp.ValueSource{
								Source: &mbp.ValueSource_FromInput{
									FromInput: &mbp.ValueSource_InputSource{
										Arg:   1,
										Field: ".x",
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
			want: Graph{
				Graph: map[IsID][]IsID{
					nodeList[3][0].ID(): []IsID{nodeList[3][1].ID()},
					nodeList[3][1].ID(): []IsID{nodeList[3][2].ID()},
					nodeList[3][2].ID(): []IsID{nodeList[3][3].ID()},
					nodeList[3][3].ID(): []IsID{},
				},
				Nodes: map[IsID]Node{
					nodeList[3][0].ID(): nodeList[3][0],
					nodeList[3][1].ID(): nodeList[3][1],
					nodeList[3][2].ID(): nodeList[3][2],
					nodeList[3][3].ID(): nodeList[3][3],
				},
			},
			wantErrors: false,
		},
		{
			name: "test no projector",
			frontier: &dfsBuildStack{
				stack: []msgContext{
					msgContext{
						msg: &mbp.FieldMapping{
							ValueSource: &mbp.ValueSource{
								Projector: "projector",
							},
							Target: &mbp.FieldMapping_TargetField{
								TargetField: "x",
							},
						},
					},
				},
			},
			projectors: map[string]*mbp.ProjectorDefinition{},
			wantErrors: true,
		},
	}

	setIDfactory(&autoIncFactory{currentID: 0})
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			g, err := buildGraphDFS(test.frontier, test.projectors)
			if test.wantErrors && err == nil {
				t.Errorf("expected error building graph")
			} else if !test.wantErrors && err != nil {
				t.Errorf("building graph for %v failed: %w", test.frontier, err)
			}

			if !test.wantErrors {
				if len(test.want.Nodes) != len(g.Nodes) {
					t.Errorf("expected %v nodes, but got %v; %v", len(test.want.Nodes), len(g.Nodes), g.Nodes)
				}

				for wantID, wantNode := range test.want.Nodes {
					node, ok := g.Nodes[wantID]
					if !ok {
						t.Errorf("expected node %v to be in the graph. got %v", wantNode, g.Nodes)
					}
					if !node.Equals(wantNode) {
						t.Errorf("expected node %v but got %v", wantNode, node)
					}
					for _, wantAncestorID := range test.want.Graph[wantID] {
						wantAncestor := test.want.Nodes[wantAncestorID]
						ancestor := g.Nodes[wantAncestorID]
						if !wantAncestor.Equals(ancestor) {
							t.Errorf("expected ancestor %v but got %v", wantAncestor, ancestor)
						}
					}
				}
			}
		},
		)
	}
}

func TestNewNode(t *testing.T) {
	setIDfactory(mockIDfactory(0))
	tests := []struct {
		name       string
		msg        proto.Message
		want       Node
		wantErrors bool
	}{
		{
			name: "test constant bool",
			msg: &mbp.ValueSource{
				Source: &mbp.ValueSource_ConstBool{
					ConstBool: true,
				},
			},
			want: &ConstBoolNode{
				id:    intID(0),
				Value: true,
			},
			wantErrors: false,
		},
		{
			name: "test constant int",
			msg: &mbp.ValueSource{
				Source: &mbp.ValueSource_ConstInt{
					ConstInt: 1,
				},
			},
			want: &ConstIntNode{
				id:    intID(0),
				Value: 1,
			},
			wantErrors: false,
		},
		{
			name: "test constant float",
			msg: &mbp.ValueSource{
				Source: &mbp.ValueSource_ConstFloat{
					ConstFloat: 1.0,
				},
			},
			want: &ConstFloatNode{
				id:    intID(0),
				Value: 1.0,
			},
			wantErrors: false,
		},
		{
			name: "test constant string",
			msg: &mbp.ValueSource{
				Source: &mbp.ValueSource_ConstString{
					ConstString: "str",
				},
			},
			want: &ConstStringNode{
				id:    intID(0),
				Value: "str",
			},
			wantErrors: false,
		},
		{
			name: "test field mapping",
			msg: &mbp.FieldMapping{
				Target: &mbp.FieldMapping_TargetField{
					TargetField: "x",
				},
			},
			want: &TargetNode{
				id:   intID(0),
				Name: "x",
			},
			wantErrors: false,
		},
		{
			name: "test projector",
			msg: &mbp.ProjectorDefinition{
				Name:    "projector",
				Mapping: []*mbp.FieldMapping{},
			},
			want: &ProjectorNode{
				id:   intID(0),
				Name: "projector",
			},
			wantErrors: false,
		},
		{
			name: "test argument",
			msg: &mbp.ValueSource{
				Source: &mbp.ValueSource_FromInput{
					FromInput: &mbp.ValueSource_InputSource{
						Arg:   1,
						Field: ".x",
					},
				},
			},
			want: &ArgumentNode{
				id:    intID(0),
				Index: 1,
				Field: ".x",
			},
			wantErrors: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			node, err := newNode(test.msg)

			if test.wantErrors && err == nil {
				t.Errorf("expected errors making node %v", test.msg)
			} else if !test.wantErrors && err != nil {
				t.Errorf("expected node %v but got error %w", test.want, err)
			}
			if !test.wantErrors && !test.want.Equals(node) {
				t.Errorf("expected node %v but got %v", test.want, node)
			}
		},
		)
	}
}

func TestAncestors(t *testing.T) {
	tests := []struct {
		name             string
		msg              proto.Message
		projectors       map[string]*mbp.ProjectorDefinition
		want             proto.Message
		wantNumAncestors int
		wantErrors       bool
	}{
		{
			name: "test constant source",
			msg: &mbp.ValueSource{
				Source: &mbp.ValueSource_ConstBool{
					ConstBool: true,
				},
			},
			want:       nil,
			wantErrors: false,
		},
		{
			name: "test field mapping",
			msg: &mbp.FieldMapping{
				ValueSource: &mbp.ValueSource{
					Source: &mbp.ValueSource_ConstBool{
						ConstBool: true,
					},
				},
			},
			want: &mbp.ValueSource{
				Source: &mbp.ValueSource_ConstBool{
					ConstBool: true,
				},
			},
			wantErrors: false,
		},
		{
			name: "test field mapping error",
			msg: &mbp.FieldMapping{
				ValueSource: &mbp.ValueSource{
					Projector: "no_projector",
				},
			},
			projectors: map[string]*mbp.ProjectorDefinition{},
			wantErrors: true,
		},
		{
			name: "test projector",
			msg: &mbp.ProjectorDefinition{
				Name: "projector",
				Mapping: []*mbp.FieldMapping{
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
			want: &mbp.FieldMapping{
				ValueSource: &mbp.ValueSource{
					Source: &mbp.ValueSource_ConstBool{
						ConstBool: true,
					},
				},
				Target: &mbp.FieldMapping_TargetField{
					TargetField: "x",
				},
			},
			wantErrors: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			msgs, err := ancestors(test.msg, test.projectors)

			if test.wantErrors && err == nil {
				t.Errorf("expected error getting ancestors for %v", test.msg)
			} else if !test.wantErrors && err != nil {
				t.Errorf("getting ancestors for %v failed: %w", test.msg, err)
			}

			if !test.wantErrors {
				if test.want == nil && len(msgs) != 0 {
					t.Errorf("expected 0 ancestors but got %v: %v", len(msgs), msgs)
				}
				if test.want != nil {
					if len(msgs) != 1 {
						t.Errorf("expected 1 ancestor but got %v: %v", len(msgs), msgs)
					} else if cmp.Diff(test.want, msgs[0], protocmp.Transform()) != "" {
						t.Errorf("expected ancestor %v but got %v", test.want, msgs[0])
					}
				}
			}
		},
		)
	}
}

func TestFieldMappingAncestors(t *testing.T) {
	tests := []struct {
		name       string
		msg        *mbp.FieldMapping
		projectors map[string]*mbp.ProjectorDefinition
		want       []proto.Message
		wantErrors bool
	}{
		{
			name: "test constant source",
			msg: &mbp.FieldMapping{
				ValueSource: &mbp.ValueSource{
					Source: &mbp.ValueSource_ConstBool{
						ConstBool: true,
					},
				},
			},
			want: []proto.Message{
				&mbp.ValueSource{
					Source: &mbp.ValueSource_ConstBool{
						ConstBool: true,
					},
				},
			},
			wantErrors: false,
		},
		{
			name:       "test empty mapping",
			msg:        &mbp.FieldMapping{},
			want:       []proto.Message{},
			wantErrors: false,
		},
		{
			name: "test projector source",
			msg: &mbp.FieldMapping{
				ValueSource: &mbp.ValueSource{
					Projector: "projector",
				},
			},
			projectors: map[string]*mbp.ProjectorDefinition{
				"projector": &mbp.ProjectorDefinition{
					Mapping: []*mbp.FieldMapping{&mbp.FieldMapping{
						ValueSource: &mbp.ValueSource{
							Source: &mbp.ValueSource_ConstBool{
								ConstBool: true,
							},
						},
					}},
				},
			},
			want: []proto.Message{
				&mbp.ProjectorDefinition{
					Mapping: []*mbp.FieldMapping{&mbp.FieldMapping{
						ValueSource: &mbp.ValueSource{
							Source: &mbp.ValueSource_ConstBool{
								ConstBool: true,
							},
						},
					}},
				},
			},
			wantErrors: false,
		},
		{
			name: "test invalid projector source",
			msg: &mbp.FieldMapping{
				ValueSource: &mbp.ValueSource{
					Projector: "no_projector",
				},
			},
			projectors: map[string]*mbp.ProjectorDefinition{
				"projector": &mbp.ProjectorDefinition{
					Mapping: []*mbp.FieldMapping{&mbp.FieldMapping{
						ValueSource: &mbp.ValueSource{
							Source: &mbp.ValueSource_ConstBool{
								ConstBool: true,
							},
						},
					}},
				},
			},
			wantErrors: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			msgs, err := fieldMappingAncestors(test.msg, test.projectors)

			if test.wantErrors && err == nil {
				t.Errorf("expected error getting ancestors for %v", test.msg)
			} else if !test.wantErrors && err != nil {
				t.Errorf("getting ancestors for %v failed: %w", test.msg, err)
			}

			if !test.wantErrors {
				if len(msgs) != len(test.want) {
					t.Fatalf("expected %v ancestors but got %v: %v", len(test.want), len(msgs), msgs)
				}
				for i := range msgs {
					if cmp.Diff(msgs[i], test.want[i], protocmp.Transform()) != "" {
						t.Errorf("expected ancestor %v but got %v", test.want[i], msgs[i])
					}
				}
			}
		},
		)
	}
}

func TestValueSourceAncestors(t *testing.T) {
	tests := []struct {
		name       string
		msg        *mbp.ValueSource
		want       []proto.Message
		wantErrors bool
	}{
		{
			name: "test ConstBool",
			msg: &mbp.ValueSource{
				Source: &mbp.ValueSource_ConstBool{
					ConstBool: true,
				},
			},
			want:       []proto.Message{},
			wantErrors: false,
		},
		{
			name: "test ConstInt",
			msg: &mbp.ValueSource{
				Source: &mbp.ValueSource_ConstInt{
					ConstInt: 1,
				},
			},
			want:       []proto.Message{},
			wantErrors: false,
		},
		{
			name: "test ConstBool",
			msg: &mbp.ValueSource{
				Source: &mbp.ValueSource_ConstFloat{
					ConstFloat: 0.0,
				},
			},
			want:       []proto.Message{},
			wantErrors: false,
		},
		{
			name: "test ConstBool",
			msg: &mbp.ValueSource{
				Source: &mbp.ValueSource_ConstString{
					ConstString: "str",
				},
			},
			want:       []proto.Message{},
			wantErrors: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			msgs, err := valueSourceAncestors(test.msg)
			if !test.wantErrors && err != nil {
				t.Errorf("getting valueSource ancestors failed; %w", err)
			}
			if len(msgs) != len(test.want) {
				t.Fatalf("expected %v ancestors, but got %v", len(msgs), len(test.want))
			}
		},
		)
	}
}

func TestProjectorAncestors(t *testing.T) {
	tests := []struct {
		name string
		msg  *mbp.ProjectorDefinition
		want []*mbp.FieldMapping
	}{
		{
			name: "test with mappings",
			msg: &mbp.ProjectorDefinition{
				Name: "projector",
				Mapping: []*mbp.FieldMapping{
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
					&mbp.FieldMapping{
						ValueSource: &mbp.ValueSource{
							Source: &mbp.ValueSource_ConstInt{
								ConstInt: 5,
							},
						},
						Target: &mbp.FieldMapping_TargetField{
							TargetField: "y",
						},
					},
				},
			},
			want: []*mbp.FieldMapping{
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
				&mbp.FieldMapping{
					ValueSource: &mbp.ValueSource{
						Source: &mbp.ValueSource_ConstInt{
							ConstInt: 5,
						},
					},
					Target: &mbp.FieldMapping_TargetField{
						TargetField: "y",
					},
				},
			},
		},
		{
			name: "test without mappings",
			msg: &mbp.ProjectorDefinition{
				Name:    "projector",
				Mapping: []*mbp.FieldMapping{},
			},
			want: []*mbp.FieldMapping{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			msgs := projectorAncestors(test.msg)
			if len(msgs) != len(test.want) {
				t.Fatalf("expected %v ancestors, but got %v", len(msgs), len(test.want))
			}
			for i := range msgs {
				wantMapping, msg := test.want[i], msgs[i]
				var mapping *mbp.FieldMapping
				var ok bool
				if mapping, ok = msg.(*mbp.FieldMapping); !ok {
					t.Fatalf("expected ancestor of type FieldMapping, but got %v", msg)
				}
				if cmp.Diff(wantMapping.GetValueSource(), mapping.GetValueSource(), protocmp.Transform()) != "" {
					t.Fatalf("expected ValueSource %v but got %v", wantMapping.GetValueSource(), mapping.GetValueSource())
				}
				if wantMapping.GetTargetField() != mapping.GetTargetField() {
					t.Errorf("expected Target %v but got %v", wantMapping.GetTarget(), mapping.GetTarget())
				}
			}
		},
		)
	}
}

func TestDFSbuildStack_Push(t *testing.T) {
	tests := []struct {
		name string
		s    *dfsBuildStack
		mcs  []msgContext
		want *dfsBuildStack
	}{
		{
			name: "push one to empty",
			s:    &dfsBuildStack{},
			mcs: []msgContext{
				msgContext{
					msg: &mbp.ValueSource{
						Source: &mbp.ValueSource_ConstBool{
							ConstBool: true,
						},
					},
				},
			},
			want: &dfsBuildStack{
				stack: []msgContext{
					msgContext{
						msg: &mbp.ValueSource{
							Source: &mbp.ValueSource_ConstBool{
								ConstBool: true,
							},
						},
					},
				},
			},
		},
		{
			name: "push one to nonempty",
			s: &dfsBuildStack{
				stack: []msgContext{
					msgContext{
						msg: &mbp.ValueSource{
							Source: &mbp.ValueSource_ConstInt{
								ConstInt: 1,
							},
						},
					},
					msgContext{
						msg: &mbp.ValueSource{
							Source: &mbp.ValueSource_ConstInt{
								ConstInt: 2,
							},
						},
					},
				},
			},
			mcs: []msgContext{
				msgContext{
					msg: &mbp.ValueSource{
						Source: &mbp.ValueSource_ConstInt{
							ConstInt: 3,
						},
					},
				},
			},
			want: &dfsBuildStack{
				stack: []msgContext{
					msgContext{
						msg: &mbp.ValueSource{
							Source: &mbp.ValueSource_ConstInt{
								ConstInt: 1,
							},
						},
					},
					msgContext{
						msg: &mbp.ValueSource{
							Source: &mbp.ValueSource_ConstInt{
								ConstInt: 2,
							},
						},
					},
					msgContext{
						msg: &mbp.ValueSource{
							Source: &mbp.ValueSource_ConstInt{
								ConstInt: 3,
							},
						},
					},
				},
			},
		},
		{
			name: "push many",
			s:    &dfsBuildStack{},
			mcs: []msgContext{
				msgContext{
					msg: &mbp.ValueSource{
						Source: &mbp.ValueSource_ConstInt{
							ConstInt: 1,
						},
					},
				},
				msgContext{
					msg: &mbp.ValueSource{
						Source: &mbp.ValueSource_ConstInt{
							ConstInt: 2,
						},
					},
				},
			},
			want: &dfsBuildStack{
				stack: []msgContext{
					msgContext{
						msg: &mbp.ValueSource{
							Source: &mbp.ValueSource_ConstInt{
								ConstInt: 1,
							},
						},
					},
					msgContext{
						msg: &mbp.ValueSource{
							Source: &mbp.ValueSource_ConstInt{
								ConstInt: 2,
							},
						},
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, mc := range test.mcs {
				test.s.push(mc)
			}
			if test.s.len() != test.want.len() {
				t.Errorf("expected length %v, but got length %v", test.s.len(), test.want.len())
			}
			for i := range test.s.stack {
				if cmp.Diff(test.s.stack[i].msg, test.want.stack[i].msg, protocmp.Transform()) != "" {
					t.Errorf("expected %v but got %v", test.want.stack[i].msg, test.s.stack[i].msg)
				}
			}
		},
		)
	}
}

func TestDFSbuildStack_Pop(t *testing.T) {
	tests := []struct {
		name        string
		s           *dfsBuildStack
		wantStack   *dfsBuildStack
		wantPopped  []msgContext
		wantSuccess bool
	}{
		{
			name:        "pop empty",
			s:           &dfsBuildStack{},
			wantSuccess: false,
		},
		{
			name: "pop one-stack",
			s: &dfsBuildStack{
				stack: []msgContext{
					msgContext{
						msg: &mbp.ValueSource{
							Source: &mbp.ValueSource_ConstInt{
								ConstInt: 1,
							},
						},
					},
				},
			},
			wantPopped: []msgContext{
				msgContext{
					msg: &mbp.ValueSource{
						Source: &mbp.ValueSource_ConstInt{
							ConstInt: 1,
						},
					},
				},
			},
			wantStack:   &dfsBuildStack{stack: []msgContext{}},
			wantSuccess: true,
		},
		{
			name: "pop many from many",
			s: &dfsBuildStack{
				stack: []msgContext{
					msgContext{
						msg: &mbp.ValueSource{
							Source: &mbp.ValueSource_ConstInt{
								ConstInt: 1,
							},
						},
					},
					msgContext{
						msg: &mbp.ValueSource{
							Source: &mbp.ValueSource_ConstInt{
								ConstInt: 2,
							},
						},
					},
					msgContext{
						msg: &mbp.ValueSource{
							Source: &mbp.ValueSource_ConstInt{
								ConstInt: 3,
							},
						},
					},
					msgContext{
						msg: &mbp.ValueSource{
							Source: &mbp.ValueSource_ConstInt{
								ConstInt: 4,
							},
						},
					},
				},
			},
			wantPopped: []msgContext{
				msgContext{
					msg: &mbp.ValueSource{
						Source: &mbp.ValueSource_ConstInt{
							ConstInt: 4,
						},
					},
				},
				msgContext{
					msg: &mbp.ValueSource{
						Source: &mbp.ValueSource_ConstInt{
							ConstInt: 3,
						},
					},
				},
			},
			wantStack: &dfsBuildStack{
				stack: []msgContext{
					msgContext{
						msg: &mbp.ValueSource{
							Source: &mbp.ValueSource_ConstInt{
								ConstInt: 1,
							},
						},
					},
					msgContext{
						msg: &mbp.ValueSource{
							Source: &mbp.ValueSource_ConstInt{
								ConstInt: 2,
							},
						},
					},
				},
			},
			wantSuccess: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if !test.wantSuccess {
				if _, ok := test.s.pop(); ok {
					t.Errorf("expected popping on stack %v to fail", test.s)
				}
			} else {
				for _, mc := range test.wantPopped {
					poppedMC, ok := test.s.pop()
					if !ok {
						t.Errorf("expected to pop %v, but popping failed", mc)
					}
					if cmp.Diff(mc.msg, poppedMC.msg, protocmp.Transform()) != "" {
						t.Errorf("expected msg %v but got msg %v", mc.msg, poppedMC.msg)
					}
				}
				if test.s.len() != test.wantStack.len() {
					t.Errorf("expected %v messages after popping, but got %v", test.wantStack.len(), test.s.len())
				}
				for i, wantMC := range test.wantStack.stack {
					msg := test.s.stack[i].msg
					if cmp.Diff(msg, wantMC.msg, protocmp.Transform()) != "" {
						t.Errorf("expected msg %v in stack, but got %v", wantMC.msg, msg)
					}
				}
			}
		},
		)
	}
}
