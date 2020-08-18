package graph

import (
	"testing"

	mbp "github.com/GoogleCloudPlatform/healthcare-data-harmonization/mapping_engine/proto"
	"github.com/GoogleCloudPlatform/healthcare-data-harmonization/mapping_language/transpiler"
	proto "github.com/golang/protobuf/proto"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestNew_Whistle(t *testing.T) {
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
					makeTargetNode("x", 0).ID(): []IsID{makeBoolNode(true, 1).ID()},
					makeBoolNode(true, 1).ID():  []IsID{},
				},
				ArgumentEdges: map[IsID][]IsID{},
				ConditionEdges: map[IsID][]IsID{
					intID(0): ids0(),
				},
				Nodes: map[IsID]Node{
					makeTargetNode("x", 0).ID(): makeTargetNode("x", 0),
					makeBoolNode(true, 1).ID():  makeBoolNode(true, 1),
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
					intID(0): ids1(1),
					intID(1): ids1(2),
					intID(2): ids1(3),
					intID(3): ids0(),
				},
				ArgumentEdges: map[IsID][]IsID{
					intID(1): ids0(),
				},
				ConditionEdges: map[IsID][]IsID{
					intID(0): ids0(),
					intID(2): ids0(),
				},
				Nodes: map[IsID]Node{
					intID(0): makeTargetNode("x", 0),
					intID(1): makeProjNode("proj1", 1),
					intID(2): makeTargetNode("y", 2),
					intID(3): makeFloatNode(5.0, 3),
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
					intID(0): ids1(1),
					intID(1): ids1(2),
					intID(2): ids1(3),
					intID(3): ids1(4),
					intID(4): ids0(),
					intID(5): ids0(),
				},
				ArgumentEdges: map[IsID][]IsID{
					intID(1): ids2(4, 5),
				},
				ConditionEdges: map[IsID][]IsID{
					intID(0): ids0(),
					intID(2): ids0(),
				},
				Nodes: map[IsID]Node{
					intID(0): makeTargetNode("x", 0),
					intID(1): makeProjNode("proj1", 1),
					intID(2): makeTargetNode("y", 2),
					intID(3): makeArgNode(1, "", 3),
					intID(4): makeBoolNode(true, 4),
					intID(5): makeFloatNode(5.0, 5),
				},
			},
			wantErrors: false,
		},
		{
			name: "test projector fields",
			whistle: `
			x: proj(foo())

			def foo() {
				a: bar()
			}

			def bar() {
				b: "b"
			}

			def proj(arg) {
				y: arg.a.b
			}`,
			want: Graph{
				Edges: map[IsID][]IsID{
					intID(0): ids1(1), // x -> proj
					intID(1): ids1(2), // proj -> y
					intID(3): ids1(4), // foo -> a
					intID(4): ids1(5), // a -> bar
					intID(5): ids1(6), // bar -> b
					intID(6): ids1(7), // b -> string
					intID(7): ids0(),  // string
					intID(2): ids1(8), // y -> arg.a.b
					intID(8): ids1(6), // arg.a.b -> b
				},
				ArgumentEdges: map[IsID][]IsID{
					intID(1): ids1(3), // proj -> foo
					intID(3): ids0(),  // foo
					intID(5): ids0(),  // bar
				},
				ConditionEdges: map[IsID][]IsID{
					intID(0): ids0(),
					intID(2): ids0(),
					intID(4): ids0(),
					intID(6): ids0(),
				},
				Nodes: map[IsID]Node{
					intID(0): makeTargetNode("x", 0),
					intID(1): makeProjNode("proj", 1),
					intID(2): makeTargetNode("y", 2),
					intID(3): makeProjNode("foo", 3),
					intID(4): makeTargetNode("a", 4),
					intID(5): makeProjNode("bar", 5),
					intID(6): makeTargetNode("b", 6),
					intID(7): makeStringNode("b", 7),
					intID(8): makeArgNode(1, ".a.b", 8),
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
					makeTargetNode("x", 0).ID():   []IsID{makeProjNode("proj1", 1).ID()},
					makeProjNode("proj1", 1).ID(): []IsID{makeTargetNode("y", 2).ID()},
					makeTargetNode("y", 2).ID():   []IsID{makeArgNode(1, "", 3).ID()},
					makeArgNode(1, "", 3).ID():    []IsID{makeProjNode("proj2", 4).ID()},
					makeProjNode("proj2", 4).ID(): []IsID{makeTargetNode("z", 5).ID()},
					makeTargetNode("z", 5).ID():   []IsID{makeStringNode("foo", 6).ID()},
					makeStringNode("foo", 6).ID(): []IsID{},
				},
				ArgumentEdges: map[IsID][]IsID{
					makeProjNode("proj1", 1).ID(): []IsID{makeProjNode("proj2", 4).ID()},
					makeProjNode("proj2", 4).ID(): []IsID{},
				},
				ConditionEdges: map[IsID][]IsID{
					intID(0): ids0(),
					intID(2): ids0(),
					intID(5): ids0(),
				},
				Nodes: map[IsID]Node{
					makeTargetNode("x", 0).ID():   makeTargetNode("x", 0),
					makeProjNode("proj1", 1).ID(): makeProjNode("proj1", 1),
					makeTargetNode("y", 2).ID():   makeTargetNode("y", 2),
					makeArgNode(1, "", 3).ID():    makeArgNode(1, "", 3),
					makeProjNode("proj2", 4).ID(): makeProjNode("proj2", 4),
					makeTargetNode("z", 5).ID():   makeTargetNode("z", 5),
					makeStringNode("foo", 6).ID(): makeStringNode("foo", 6),
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
		{
			name: "test local variables",
			whistle: `
			var a: "a"
			b: a`,
			want: Graph{
				Edges: map[IsID][]IsID{
					intID(0): ids1(2),
					intID(1): ids0(),
					intID(2): ids1(1),
				},
				ConditionEdges: map[IsID][]IsID{
					intID(0): ids0(),
					intID(2): ids0(),
				},
				Nodes: map[IsID]Node{
					intID(0): makeTargetNode("b", 0),
					intID(1): makeStringNode("a", 1),
					intID(2): makeVarNode("a", 2),
				},
			},
			wantErrors: false,
		},
		{
			name: "test dest keyword",
			whistle: `
			a: "a"
			b: dest a`,
			want: Graph{
				Edges: map[IsID][]IsID{
					intID(0): ids1(1), // a -> "a"
					intID(1): ids0(),  // "a"
					intID(2): ids1(0), // b -> a
				},
				ConditionEdges: map[IsID][]IsID{
					intID(0): ids0(),
					intID(2): ids0(),
				},
				Nodes: map[IsID]Node{
					intID(0): makeTargetNode("a", 0),
					intID(1): makeStringNode("a", 1),
					intID(2): makeTargetNode("b", 2),
				},
			},
			wantErrors: false,
		},
		{
			name: "test simple condition",
			whistle: `
			x (if true): 5`,
			want: Graph{
				Edges: map[IsID][]IsID{
					intID(0): ids1(1), // a -> 5
					intID(1): ids0(),  // 5
					intID(2): ids0(),  // true
				},
				ConditionEdges: map[IsID][]IsID{
					intID(0): ids1(2),
				},
				Nodes: map[IsID]Node{
					intID(0): makeTargetNode("x", 0),
					intID(1): makeFloatNode(5, 1),
					intID(2): makeBoolNode(true, 2),
				},
			},
			wantErrors: false,
		},
		{
			name: "test simple projected condition",
			whistle: `
			x (if $Eq(4, 2)): 5`,
			want: Graph{
				Edges: map[IsID][]IsID{
					intID(0): ids1(1), // x -> 5
					intID(1): ids0(),  // 5
					intID(2): ids0(),  // $Eq
					intID(3): ids0(),  // 4
					intID(4): ids0(),  // 2
				},
				ArgumentEdges: map[IsID][]IsID{
					intID(2): ids2(3, 4),
				},
				ConditionEdges: map[IsID][]IsID{
					intID(0): ids1(2), // x -> $Eq
				},
				Nodes: map[IsID]Node{
					intID(0): makeTargetNode("x", 0),
					intID(1): makeFloatNode(5, 1),
					intID(2): makeProjNode("$Eq", 2),
					intID(3): makeFloatNode(4, 3),
					intID(4): makeFloatNode(2, 4),
				},
			},
			wantErrors: false,
		},
		{
			name: "test conditional block",
			whistle: `
			x: foo()
			def foo() {
				if true {
					a: 1
				} else {
					b: 2
				}
			}`,
			want: Graph{
				Edges: map[IsID][]IsID{
					intID(0): ids1(1),    // x -> foo()
					intID(1): ids2(2, 3), // foo() -> a, b
					intID(2): ids1(4),    // a -> 1
					intID(3): ids1(5),    // b -> 2
					intID(4): ids0(),     // 1
					intID(5): ids0(),     // 2
					intID(6): ids0(),     // true
					intID(7): ids0(),     // $Not
					intID(8): ids0(),     // true
				},
				ArgumentEdges: map[IsID][]IsID{
					intID(1): ids0(),  // foo()
					intID(7): ids1(8), // $Not -> true
				},
				ConditionEdges: map[IsID][]IsID{
					intID(0): ids0(),  // x
					intID(2): ids1(6), // a -> true
					intID(3): ids1(7), // b -> $Not
				},
				Nodes: map[IsID]Node{
					intID(0): makeTargetNode("x", 0),
					intID(1): makeProjNode("foo", 1),
					intID(2): makeTargetNode("a", 2),
					intID(3): makeTargetNode("b", 3),
					intID(4): makeFloatNode(1, 4),
					intID(5): makeFloatNode(2, 5),
					intID(6): makeBoolNode(true, 6),
					intID(7): makeProjNode("$Not", 7),
					intID(8): makeBoolNode(true, 8),
				},
			},
			wantErrors: false,
		},
		{
			name: "test conditional block with projected value",
			whistle: `
			x: foo()
			def foo() {
				if bar() {
					a: 1
				} else {
					b: 2
				}
			}

			def bar() {

			}`,
			want: Graph{
				Edges: map[IsID][]IsID{
					intID(0): ids1(1),    // x -> foo()
					intID(1): ids2(2, 3), // foo() -> a, b
					intID(2): ids1(4),    // a -> 1
					intID(3): ids1(5),    // b -> 2
					intID(4): ids0(),     // 1
					intID(5): ids0(),     // 2
					intID(6): ids0(),     // bar()
					intID(7): ids0(),     // $Not
					intID(8): ids0(),     // bar()
				},
				ArgumentEdges: map[IsID][]IsID{
					intID(1): ids0(),  // foo()
					intID(7): ids1(8), // $Not -> bar()
					intID(6): ids0(),  // bar()
					intID(8): ids0(),  // bar()
				},
				ConditionEdges: map[IsID][]IsID{
					intID(0): ids0(),  // x
					intID(2): ids1(6), // a -> true
					intID(3): ids1(7), // b -> $Not
				},
				Nodes: map[IsID]Node{
					intID(0): makeTargetNode("x", 0),
					intID(1): makeProjNode("foo", 1),
					intID(2): makeTargetNode("a", 2),
					intID(3): makeTargetNode("b", 3),
					intID(4): makeFloatNode(1, 4),
					intID(5): makeFloatNode(2, 5),
					intID(6): makeProjNode("bar", 6),
					intID(7): makeProjNode("$Not", 7),
					intID(8): makeProjNode("bar", 8),
				},
			},
			wantErrors: false,
		},
		{
			name: "test dest with conditions",
			whistle: `
			a (if true): "a1"
			a (if false): "a2"
			x: dest a`,
			want: Graph{
				Edges: map[IsID][]IsID{
					intID(0): ids1(1),    // a -> "a1"
					intID(1): ids0(),     // "a1"
					intID(2): ids1(3),    // a' -> "a2"
					intID(3): ids0(),     // "a2"
					intID(4): ids2(0, 2), // x -> a, a'
					intID(5): ids0(),     // true
					intID(6): ids0(),     // false
				},
				ConditionEdges: map[IsID][]IsID{
					intID(0): ids1(5), // a -> true
					intID(2): ids1(6), // a' -> false
					intID(4): ids0(),
				},
				Nodes: map[IsID]Node{
					intID(0): makeTargetNode("a", 0),
					intID(1): makeStringNode("a1", 1),
					intID(2): makeTargetNode("a", 2),
					intID(3): makeStringNode("a2", 3),
					intID(4): makeTargetNode("x", 4),
					intID(5): makeBoolNode(true, 5),
					intID(6): makeBoolNode(false, 6),
				},
			},
			wantErrors: false,
		},
		{
			name: "test local var with conditions",
			whistle: `
			var a (if true): "a1"
			var a (if false): "a2"
			x: a`,
			want: Graph{
				Edges: map[IsID][]IsID{
					intID(0): ids1(1),    // a -> "a1"
					intID(1): ids0(),     // "a1"
					intID(2): ids1(3),    // a' -> "a2"
					intID(3): ids0(),     // "a2"
					intID(4): ids2(0, 2), // x -> a, a'
					intID(5): ids0(),     // true
					intID(6): ids0(),     // false
				},
				ConditionEdges: map[IsID][]IsID{
					intID(0): ids1(5), // a -> true
					intID(2): ids1(6), // a' -> false
					intID(4): ids0(),
				},
				Nodes: map[IsID]Node{
					intID(0): makeVarNode("a", 0),
					intID(1): makeStringNode("a1", 1),
					intID(2): makeVarNode("a", 2),
					intID(3): makeStringNode("a2", 3),
					intID(4): makeTargetNode("x", 4),
					intID(5): makeBoolNode(true, 5),
					intID(6): makeBoolNode(false, 6),
				},
			},
			wantErrors: false,
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

			if !test.wantErrors && err == nil {
				if len(test.want.Nodes) != len(g.Nodes) {
					t.Errorf("expected %v nodes, but got %v; %v", len(test.want.Nodes), len(g.Nodes), g.Nodes)
				}
				for _, wantNode := range test.want.Nodes {
					if matches := findNodesInMap(wantNode, g.Nodes); len(matches) == 0 {
						t.Errorf("expected node {%v} to be in the graph, but it was not. Graph:\n%v", wantNode, g)
					}
				}
				if equal, errStr := compareGraphs(test.want.Edges, g.Edges, test.want.Nodes, g.Nodes); !equal {
					t.Errorf("the graph edges are not as expected:\n%v\nThe graph was:\n%v", errStr, g)
				}
				if equal, errStr := compareGraphs(test.want.ArgumentEdges, g.ArgumentEdges, test.want.Nodes, g.Nodes); !equal {
					t.Errorf("the graph argument edges are not as expected:\n%v\nThe graph was:\n%v", errStr, g)
				}
				if equal, errStr := compareGraphs(test.want.ConditionEdges, g.ConditionEdges, test.want.Nodes, g.Nodes); !equal {
					t.Errorf("the graph condition edges are not as expected:\n%v\nThe graph was:\n%v", errStr, g)
				}
			}
		},
		)
	}
}

func TestNew_WhistlerProto(t *testing.T) {
	tests := []struct {
		name       string
		mpc        *mbp.MappingConfig
		want       Graph
		wantErrors bool
	}{
		{
			name: "test constant mapping",
			mpc:  makeMappingConfigMsg(nil, []*mbp.FieldMapping{makeMappingMsg("x", makeBoolMsg(true), nil)}),
			want: Graph{
				Edges: map[IsID][]IsID{
					makeTargetNode("x", 0).ID(): []IsID{makeBoolNode(true, 1).ID()},
					makeBoolNode(true, 1).ID():  []IsID{},
				},
				ArgumentEdges: map[IsID][]IsID{},
				ConditionEdges: map[IsID][]IsID{
					intID(0): ids0(),
				},
				Nodes: map[IsID]Node{
					makeTargetNode("x", 0).ID(): makeTargetNode("x", 0),
					makeBoolNode(true, 1).ID():  makeBoolNode(true, 1),
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
			mpc: makeMappingConfigMsg(
				[]*mbp.ProjectorDefinition{
					makeProjDefMsg("proj1", []*mbp.FieldMapping{
						makeMappingMsg("y", makeIntMsg(0), nil),
					}),
				},
				[]*mbp.FieldMapping{
					makeMappingMsg("x", makeProjSourceMsg("proj1", nil, nil), nil),
				}),
			want: Graph{
				Edges: map[IsID][]IsID{
					intID(0): ids1(1),
					intID(1): ids1(2),
					intID(2): ids1(3),
					intID(3): ids0(),
				},
				ArgumentEdges: map[IsID][]IsID{
					intID(1): ids0(),
				},
				ConditionEdges: map[IsID][]IsID{
					intID(0): ids0(),
					intID(2): ids0(),
				},
				Nodes: map[IsID]Node{
					intID(0): makeTargetNode("x", 0),
					intID(1): makeProjNode("proj1", 1),
					intID(2): makeTargetNode("y", 2),
					intID(3): makeIntNode(0, 3),
				},
			},
			wantErrors: false,
		},
		{
			name: "test projector arguments",
			mpc: makeMappingConfigMsg(
				[]*mbp.ProjectorDefinition{
					makeProjDefMsg("proj1", []*mbp.FieldMapping{
						makeMappingMsg("y", makeArgMsg(1, ""), nil),
					}),
				},
				[]*mbp.FieldMapping{
					makeMappingMsg(
						"x",
						makeProjSourceMsg("proj1", makeBoolMsg(true), []*mbp.ValueSource{makeFloatMsg(5.0)}),
						nil),
				}),
			want: Graph{
				Edges: map[IsID][]IsID{
					intID(0): ids1(1),
					intID(1): ids1(2),
					intID(2): ids1(3),
					intID(3): ids1(4),
					intID(4): ids0(),
					intID(5): ids0(),
				},
				ArgumentEdges: map[IsID][]IsID{
					intID(1): ids2(4, 5),
				},
				ConditionEdges: map[IsID][]IsID{
					intID(0): ids0(),
					intID(2): ids0(),
				},
				Nodes: map[IsID]Node{
					intID(0): makeTargetNode("x", 0),
					intID(1): makeProjNode("proj1", 1),
					intID(2): makeTargetNode("y", 2),
					intID(3): makeArgNode(1, "", 3),
					intID(4): makeBoolNode(true, 4),
					intID(5): makeFloatNode(5.0, 5),
				},
			},
			wantErrors: false,
		},
		{
			name: "test no projector",
			mpc: makeMappingConfigMsg([]*mbp.ProjectorDefinition{}, []*mbp.FieldMapping{
				makeMappingMsg("x", makeProjSourceMsg("projector", nil, nil), nil),
			}),
			wantErrors: true,
		},
		{
			name: "test nested projectors",
			mpc: makeMappingConfigMsg(
				[]*mbp.ProjectorDefinition{
					makeProjDefMsg("proj1", []*mbp.FieldMapping{
						makeMappingMsg("y", makeArgMsg(1, ""), nil),
					}),
					makeProjDefMsg("proj2", []*mbp.FieldMapping{
						makeMappingMsg("z", makeStringMsg("foo"), nil),
					}),
				},
				[]*mbp.FieldMapping{
					makeMappingMsg(
						"x",
						makeProjSourceMsg(
							"proj1",
							makeProjectedSourceMsg(
								makeProjSourceMsg("proj2", nil, nil),
								"",
								nil),
							nil,
						),
						nil,
					),
				}),
			want: Graph{
				Edges: map[IsID][]IsID{
					makeTargetNode("x", 0).ID():   []IsID{makeProjNode("proj1", 1).ID()},
					makeProjNode("proj1", 1).ID(): []IsID{makeTargetNode("y", 2).ID()},
					makeTargetNode("y", 2).ID():   []IsID{makeArgNode(1, "", 3).ID()},
					makeArgNode(1, "", 3).ID():    []IsID{makeProjNode("proj2", 4).ID()},
					makeProjNode("proj2", 4).ID(): []IsID{makeTargetNode("z", 5).ID()},
					makeTargetNode("z", 5).ID():   []IsID{makeStringNode("foo", 6).ID()},
					makeStringNode("foo", 6).ID(): []IsID{},
				},
				ArgumentEdges: map[IsID][]IsID{
					makeProjNode("proj1", 1).ID(): []IsID{makeProjNode("proj2", 4).ID()},
					makeProjNode("proj2", 4).ID(): []IsID{},
				},
				ConditionEdges: map[IsID][]IsID{
					intID(0): ids0(),
					intID(2): ids0(),
					intID(5): ids0(),
				},
				Nodes: map[IsID]Node{
					makeTargetNode("x", 0).ID():   makeTargetNode("x", 0),
					makeProjNode("proj1", 1).ID(): makeProjNode("proj1", 1),
					makeTargetNode("y", 2).ID():   makeTargetNode("y", 2),
					makeArgNode(1, "", 3).ID():    makeArgNode(1, "", 3),
					makeProjNode("proj2", 4).ID(): makeProjNode("proj2", 4),
					makeTargetNode("z", 5).ID():   makeTargetNode("z", 5),
					makeStringNode("foo", 6).ID(): makeStringNode("foo", 6),
				},
			},
			wantErrors: false,
		},
		{
			name: "test recursive projector",
			mpc: makeMappingConfigMsg(
				[]*mbp.ProjectorDefinition{
					makeProjDefMsg("proj1", []*mbp.FieldMapping{
						makeMappingMsg("z", makeProjSourceMsg("proj1", nil, nil), nil),
					}),
				},
				[]*mbp.FieldMapping{
					makeMappingMsg(
						"x",
						makeProjSourceMsg("proj1", nil, nil),
						nil),
				}),
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
					t.Errorf("expected %v nodes, but got %v; %v", len(test.want.Nodes), len(g.Nodes), g)
				}
				for _, wantNode := range test.want.Nodes {
					if matches := findNodesInMap(wantNode, g.Nodes); len(matches) == 0 {
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

func TestAddArgLineages(t *testing.T) {
	tests := []struct {
		name       string
		args       [][]whistlerNode
		e          *env
		graph      Graph
		projNode   *ProjectorNode
		projectors map[string]*mbp.ProjectorDefinition
		want       *env
		wantErrors bool
	}{
		{
			name: "add argument",
			args: [][]whistlerNode{
				[]whistlerNode{
					whistlerNode{
						msg: makeBoolMsg(true),
					},
					whistlerNode{
						msg: makeIntMsg(1),
					},
				},
				[]whistlerNode{
					whistlerNode{
						msg: makeIntMsg(2),
					},
				},
			},
			e: &env{
				name: "root",
			},
			graph: Graph{
				Edges: map[IsID][]IsID{
					intID(0): ids0(),
				},
				ArgumentEdges: map[IsID][]IsID{
					intID(0): ids0(),
				},
				Nodes: map[IsID]Node{
					intID(0): makeProjNode("proj1", 0),
				},
			},
			projNode: makeProjNode("proj1", 0),
			projectors: map[string]*mbp.ProjectorDefinition{
				"proj1": makeProjDefMsg("proj1", nil), // the projector mappings don't matter for this test
			},
			want: &env{
				name: "proj1",
				args: [][]whistlerNode{
					[]whistlerNode{
						whistlerNode{
							msg:         makeBoolMsg(true),
							nodeInGraph: makeBoolNode(true, 0),
						},
						whistlerNode{
							msg:         makeIntMsg(1),
							nodeInGraph: makeIntNode(1, 1),
						},
					},
					[]whistlerNode{
						whistlerNode{
							msg:         makeIntMsg(2),
							nodeInGraph: makeIntNode(2, 2),
						},
					},
				},
			},
			wantErrors: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			e, err := test.graph.addArgLineages(test.args, test.e, test.projNode, test.projectors)
			if test.wantErrors && err == nil {
				t.Errorf("expected error getting argument lineages")
			} else if !test.wantErrors && err != nil {
				t.Errorf("adding argument lineages for %v failed:\n%w", test.args, err)
			}

			if !test.wantErrors {
				if test.want.name != e.name {
					t.Errorf("expected env named %v, but got %v", test.want.name, e.name)
				}
				if len(test.want.args) != len(e.args) {
					t.Errorf("expected %v arguments, but got %v; %v", len(test.want.args), len(e.args), e.args)
				}
				for i, wantMsgList := range test.want.args {
					for j, wantMsg := range wantMsgList {
						if !cmp.Equal(wantMsg.msg, e.args[i][j].msg, protocmp.Transform()) {
							t.Errorf("expected msg {%v}, but got msg {%v}", wantMsg.msg, e.args[i][j].msg)
						}
						if !equalsIgnoreID(wantMsg.nodeInGraph, e.args[i][j].nodeInGraph) {
							t.Errorf("expected node {%v}, but got {%v}", wantMsg.nodeInGraph, e.args[i][j].nodeInGraph)
						}
					}
				}
			}
		},
		)
	}
}

func TestAddNode(t *testing.T) {
	tests := []struct {
		name        string
		graph       Graph
		node        Node
		descendant  Node
		isArg       bool
		isCondition bool
		isNew       bool
		want        Graph
		wantErrors  bool
	}{
		{
			name: "new no descendant",
			graph: Graph{
				Edges: map[IsID][]IsID{},
				Nodes: map[IsID]Node{},
			},
			node:        makeBoolNode(true, 0),
			isArg:       false,
			isCondition: false,
			isNew:       true,
			want: Graph{
				Edges: map[IsID][]IsID{
					intID(0): ids0(),
				},
				ConditionEdges: map[IsID][]IsID{
					intID(0): ids0(),
				},
				Nodes: map[IsID]Node{
					intID(0): makeBoolNode(true, 0),
				},
			},
			wantErrors: false,
		},
		{
			name: "new ProjectorNode no descendant",
			graph: Graph{
				Edges:         map[IsID][]IsID{},
				ArgumentEdges: map[IsID][]IsID{},
				Nodes:         map[IsID]Node{},
			},
			node:        makeProjNode("proj", 0),
			isArg:       false,
			isCondition: false,
			isNew:       true,
			want: Graph{
				Edges: map[IsID][]IsID{
					intID(0): ids0(),
				},
				ArgumentEdges: map[IsID][]IsID{
					intID(0): ids0(),
				},
				Nodes: map[IsID]Node{
					intID(0): makeProjNode("proj", 0),
				},
			},
			wantErrors: false,
		},
		{
			name: "new already in graph",
			graph: Graph{
				Edges: map[IsID][]IsID{
					intID(0): ids0(),
				},
				Nodes: map[IsID]Node{
					intID(0): makeBoolNode(true, 0),
				},
			},
			node:        makeBoolNode(true, 0),
			isArg:       false,
			isCondition: false,
			isNew:       true,
			wantErrors:  true,
		},
		{
			name: "node already in graph with descendant",
			graph: Graph{
				Edges: map[IsID][]IsID{
					intID(1): ids0(),
					intID(0): ids0(),
				},
				ConditionEdges: map[IsID][]IsID{
					intID(1): ids0(),
				},
				Nodes: map[IsID]Node{
					intID(1): makeTargetNode("x", 1),
					intID(0): makeBoolNode(true, 0),
				},
			},
			node:        makeBoolNode(true, 0),
			isArg:       false,
			isCondition: false,
			isNew:       false,
			descendant:  makeTargetNode("x", 1),
			want: Graph{
				Edges: map[IsID][]IsID{
					intID(1): ids1(0),
					intID(0): ids0(),
				},
				ConditionEdges: map[IsID][]IsID{
					intID(1): ids0(),
				},
				Nodes: map[IsID]Node{
					intID(1): makeTargetNode("x", 1),
					intID(0): makeBoolNode(true, 0),
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
			node:       makeBoolNode(true, 1),
			descendant: makeTargetNode("x", 0),
			isArg:      false,
			isNew:      true,
			want:       Graph{},
			wantErrors: true,
		},
		{
			name: "isArg descendant",
			graph: Graph{
				Edges: map[IsID][]IsID{
					intID(0): ids0(),
				},
				ArgumentEdges: map[IsID][]IsID{
					intID(0): ids0(),
				},
				Nodes: map[IsID]Node{
					intID(0): makeProjNode("proj", 0),
				},
			},
			node:       makeBoolNode(true, 1),
			descendant: makeProjNode("proj", 0),
			isArg:      true,
			isNew:      true,
			want: Graph{
				Edges: map[IsID][]IsID{
					intID(0): ids0(),
					intID(1): ids0(),
				},
				ArgumentEdges: map[IsID][]IsID{
					intID(0): ids1(1),
				},
				Nodes: map[IsID]Node{
					intID(0): makeProjNode("proj", 0),
					intID(1): makeBoolNode(true, 1),
				},
			},
			wantErrors: false,
		},
		{
			name: "isCondition descendant",
			graph: Graph{
				Edges: map[IsID][]IsID{
					intID(0): ids0(),
				},
				ConditionEdges: map[IsID][]IsID{
					intID(0): ids0(),
				},
				Nodes: map[IsID]Node{
					intID(0): makeTargetNode("x", 0),
				},
			},
			node:        makeBoolNode(true, 1),
			descendant:  makeTargetNode("x", 0),
			isArg:       false,
			isCondition: true,
			isNew:       true,
			want: Graph{
				Edges: map[IsID][]IsID{
					intID(0): ids0(),
					intID(1): ids0(),
				},
				ConditionEdges: map[IsID][]IsID{
					intID(0): ids1(1),
				},
				Nodes: map[IsID]Node{
					intID(0): makeTargetNode("x", 0),
					intID(1): makeBoolNode(true, 1),
				},
			},
			wantErrors: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := addNode(test.graph, test.node, test.descendant, test.isArg, test.isCondition, test.isNew)
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
					if matches := findNodesInMap(wantNode, graph.Nodes); len(matches) == 0 {
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

func TestNewNode(t *testing.T) {
	tests := []struct {
		name       string
		msg        proto.Message
		numArgs    int
		wantErrors bool
		want       Node
	}{
		{
			name: "test new constant bool",
			msg:  makeBoolMsg(true),
			want: &ConstBoolNode{
				id:    intID(0),
				Value: true,
			},
			wantErrors: false,
		},
		{
			name: "test new constant int",
			msg:  makeIntMsg(0),
			want: &ConstIntNode{
				id:    intID(0),
				Value: 0,
			},
			wantErrors: false,
		},
		{
			name: "test new constant float",
			msg:  makeFloatMsg(5.0),
			want: &ConstFloatNode{
				id:    intID(0),
				Value: 5.0,
			},
			wantErrors: false,
		},
		{
			name: "test new constant string",
			msg:  makeStringMsg("foo"),
			want: &ConstStringNode{
				id:    intID(0),
				Value: "foo",
			},
			wantErrors: false,
		},
		{
			name: "test new field mapping",
			msg:  makeMappingMsg("x", nil, nil),
			want: &TargetNode{
				id:   intID(0),
				Name: "x",
			},
			wantErrors: false,
		},
		{
			name: "test new local variable",
			msg:  makeVarMappingMsg("x", nil, nil),
			want: &TargetNode{
				id:         intID(0),
				Name:       "x",
				IsVariable: true,
			},
		},
		{
			name: "test new projector",
			msg:  makeProjDefMsg("proj1", nil),
			want: &ProjectorNode{
				id:   intID(0),
				Name: "proj1",
			},
			wantErrors: false,
		},
		{
			name:    "test new argument",
			msg:     makeArgMsg(1, ""),
			numArgs: 1,
			want: &ArgumentNode{
				id:    intID(0),
				Index: 1,
			},
			wantErrors: false,
		},
		{
			name:    "test new $root",
			msg:     makeArgMsg(1, ""),
			numArgs: 0,
			want: &RootNode{
				id: intID(0),
			},
			wantErrors: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			node, err := newNode(test.msg, test.numArgs)
			if test.wantErrors && err == nil {
				t.Errorf("expected errors getting node for msg {%v}. Got node %v", test.msg, node)
			} else if !test.wantErrors && err != nil {
				t.Errorf("getting node for msg {%v} failed:\n%w", test.msg, err)
			}

			if !test.wantErrors {
				if !equalsIgnoreID(test.want, node) {
					t.Errorf("wanted node %v but got node %v", test.want, node)
				}
			}
		},
		)
	}
}

func TestFindNodeInGraph(t *testing.T) {
	tests := []struct {
		name       string
		startNode  Node
		graph      Graph
		path       []string
		want       Node
		wantErrors bool
	}{
		{
			name:       "test base case",
			startNode:  makeBoolNode(true, 0),
			graph:      Graph{},
			path:       nil,
			want:       makeBoolNode(true, 0),
			wantErrors: false,
		},
		{
			name:      "test find node only targets",
			startNode: makeTargetNode("a", 0),
			path:      []string{"b", "c"},
			graph: Graph{
				Edges: map[IsID][]IsID{
					intID(0): ids1(1),
					intID(1): ids1(2),
					intID(2): ids0(),
				},
				Nodes: map[IsID]Node{
					intID(0): makeTargetNode("a", 0),
					intID(1): makeTargetNode("b", 1),
					intID(2): makeTargetNode("c", 2),
				},
			},
			want:       makeTargetNode("c", 2),
			wantErrors: false,
		},
		{
			name:      "test find node with projectors",
			startNode: makeTargetNode("a", 0),
			path:      []string{"b"},
			graph: Graph{
				Edges: map[IsID][]IsID{
					intID(0): ids1(1),
					intID(1): ids1(2),
					intID(2): ids0(),
				},
				Nodes: map[IsID]Node{
					intID(0): makeTargetNode("a", 0),
					intID(1): makeProjNode("foo", 1),
					intID(2): makeTargetNode("b", 2),
				},
			},
			want:       makeTargetNode("b", 2),
			wantErrors: false,
		},
		{
			name:      "test find node; bad path",
			startNode: makeTargetNode("a", 0),
			path:      []string{"b", "c"},
			graph: Graph{
				Edges: map[IsID][]IsID{
					intID(0): ids1(1),
					intID(1): ids1(2),
					intID(2): ids0(),
				},
				Nodes: map[IsID]Node{
					intID(0): makeTargetNode("a", 0),
					intID(1): makeTargetNode("d", 1),
					intID(2): makeTargetNode("e", 2),
				},
			},
			wantErrors: true,
		},
		{
			name:      "test find node composite target",
			startNode: makeTargetNode("a", 0),
			path:      []string{"b", "c"},
			graph: Graph{
				Edges: map[IsID][]IsID{
					intID(0): ids1(1),
					intID(1): ids0(),
				},
				Nodes: map[IsID]Node{
					intID(0): makeTargetNode("a", 0),
					intID(1): makeTargetNode("b.c", 1),
				},
			},
			want:       makeTargetNode("b.c", 1),
			wantErrors: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			node, err := findNodeInGraph(test.startNode, test.path, test.graph)
			if test.wantErrors && err == nil {
				t.Errorf("expected errors finding node %v with path %v. Got node %v", test.startNode, test.path, node)
			} else if !test.wantErrors && err != nil {
				t.Errorf("finding node %v with path %v failed:\n%w", test.startNode, test.path, err)
			}

			if !test.wantErrors {
				if !equalsIgnoreID(test.want, node) {
					t.Errorf("wanted node %v but got node %v", test.want, node)
				}
			}
		},
		)
	}
}

func TestGetAllAncestors(t *testing.T) {
	tests := []struct {
		name       string
		wstlrNode  whistlerNode
		wantErrors bool
	}{
		{
			name:       "test known field",
			wstlrNode:  whistlerNode{msg: makeBoolMsg(true)},
			wantErrors: false,
		},
		{
			name:       "test unsupported field",
			wstlrNode:  whistlerNode{msg: makeMappingConfigMsg(nil, nil)},
			wantErrors: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := getAllAncestors(test.wstlrNode, nil, nil)
			if test.wantErrors && err == nil {
				t.Errorf("expected errors getting ancestors for %v", test.wstlrNode)
			} else if !test.wantErrors && err != nil {
				t.Errorf("finding ancestors for %v failed:\n%w", test.wstlrNode, err)
			}
		},
		)
	}
}

func TestFieldMappingAncestors(t *testing.T) {
	tests := []struct {
		name       string
		msg        *mbp.FieldMapping
		wstlrEnv   *env
		projectors map[string]*mbp.ProjectorDefinition
		want       ancestorCollection
		wantErrors bool
	}{
		{
			name: "test constant source",
			msg:  makeMappingMsg("x", makeBoolMsg(true), nil),
			want: ancestorCollection{mainAncestors: []whistlerNode{
				whistlerNode{msg: makeBoolMsg(true)},
			}},
			wantErrors: false,
		},
		{
			name:       "test nil source",
			msg:        makeMappingMsg("x", nil, nil),
			wantErrors: true,
		},
		{
			name: "test conditional mapping",
			msg:  makeMappingMsg("x", makeBoolMsg(true), makeBoolMsg(false)),
			want: ancestorCollection{
				mainAncestors: []whistlerNode{whistlerNode{msg: makeBoolMsg(true)}},
				conditions:    []whistlerNode{whistlerNode{msg: makeBoolMsg(false)}},
			},
			wantErrors: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ancestors, err := fieldMappingAncestors(test.msg, test.wstlrEnv, test.projectors)
			if test.wantErrors && err == nil {
				t.Errorf("expected errors getting ancestors for msg {%v}", test.msg)
			} else if !test.wantErrors && err != nil {
				t.Errorf("getting ancestors for msg {%v} failed:\n%w", test.msg, err)
			}

			if !test.wantErrors {
				mainAncestors := test.want.mainAncestors
				if len(mainAncestors) != len(ancestors.mainAncestors) {
					t.Errorf("expected %v main ancestors, but got %v: %v", len(mainAncestors), len(ancestors.mainAncestors), ancestors.mainAncestors)
				}
				for i := range mainAncestors {
					if !cmp.Equal(mainAncestors[i].msg, ancestors.mainAncestors[i].msg, protocmp.Transform()) {
						t.Errorf("expected msg %v, but got %v", mainAncestors[i], ancestors.mainAncestors[i])
					}
				}
				conditionAncestors := test.want.conditions
				if len(conditionAncestors) != len(ancestors.conditions) {
					t.Errorf("expected %v condition ancestors, but got %v: %v", len(conditionAncestors), len(ancestors.conditions), ancestors)
				}
				for i := range conditionAncestors {
					if !cmp.Equal(conditionAncestors[i].msg, ancestors.conditions[i].msg, protocmp.Transform()) {
						t.Errorf("expected msg %v, but got %v", conditionAncestors[i], ancestors.conditions[i])
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
		e          *env
		msg        *mbp.ValueSource
		want       []whistlerNode
		wantErrors bool
	}{
		{
			name:       "test constant",
			msg:        makeBoolMsg(true),
			want:       nil,
			wantErrors: false,
		},
		{
			name: "test Argument 0",
			e: &env{
				name:   "proj1",
				parent: nil,
				args: [][]whistlerNode{
					[]whistlerNode{
						whistlerNode{
							msg:         makeBoolMsg(true),
							nodeInGraph: makeBoolNode(true, 0),
						},
					},
				},
			},
			msg: makeArgMsg(1, ""),
			want: []whistlerNode{whistlerNode{
				msg:         makeBoolMsg(true),
				nodeInGraph: makeBoolNode(true, 0)}},
			wantErrors: false,
		},
		{
			name: "test Argument too high",
			e: &env{
				name:   "proj1",
				parent: nil,
				args:   [][]whistlerNode{},
			},
			msg:        makeArgMsg(2, ""),
			want:       nil,
			wantErrors: true,
		},
		{
			name: "test $root ancestors",
			e: &env{
				name:   "proj1",
				parent: nil,
				args:   [][]whistlerNode{},
			},
			msg:        makeArgMsg(1, ""),
			want:       nil,
			wantErrors: false,
		},
		{
			name:       "test unsupported msg",
			want:       nil,
			wantErrors: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ancestors, err := valueSourceAncestors(test.msg, test.e)
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
					if !cmp.Equal(test.want[i].msg, ancestors[i].msg, protocmp.Transform()) {
						t.Errorf("expected msg %v, but got %v", test.want[i].msg, ancestors[i].msg)
					}
				}
				for i := range test.want {
					if !equalsIgnoreID(test.want[i].nodeInGraph, ancestors[i].nodeInGraph) {
						t.Errorf("expected node %v, but got %v", test.want[i].nodeInGraph, ancestors[i].nodeInGraph)
					}
				}

			}
		},
		)
	}
}

func TestProjectorAncestors(t *testing.T) {
	tests := []struct {
		name            string
		msg             *mbp.ProjectorDefinition
		projValueSource *mbp.ValueSource
		want            ancestorCollection
		wantErrors      bool
	}{
		{
			name: "test mappings",
			msg: makeProjDefMsg("proj", []*mbp.FieldMapping{makeMappingMsg(
				"x",
				makeBoolMsg(true),
				nil)}),
			projValueSource: nil,
			want: ancestorCollection{
				mainAncestors: []whistlerNode{whistlerNode{msg: makeMappingMsg(
					"x",
					makeBoolMsg(true),
					nil)}},
			},
			wantErrors: false,
		},
		{
			name:            "test arguments",
			msg:             makeProjDefMsg("proj", []*mbp.FieldMapping{}),
			projValueSource: makeProjSourceMsg("proj", makeBoolMsg(true), []*mbp.ValueSource{makeIntMsg(0)}),
			want: ancestorCollection{
				projectorArgs: [][]whistlerNode{
					[]whistlerNode{
						whistlerNode{msg: makeProjSourceMsg("proj", makeBoolMsg(true), []*mbp.ValueSource{makeIntMsg(0)})},
					},
					[]whistlerNode{
						whistlerNode{msg: makeIntMsg(0)},
					},
				}},
			wantErrors: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			allAncestors, err := projectorAncestors(test.msg, test.projValueSource, nil, nil)
			if test.wantErrors && err == nil {
				t.Errorf("expected errors getting ancestors for msg {%v}. Got %v", test.msg, allAncestors)
			} else if !test.wantErrors && err != nil {
				t.Errorf("getting ancestors for msg {%v} failed:\n%w", test.msg, err)
			}

			if !test.wantErrors {
				wantMsgs := test.want.mainAncestors
				wantArgs := test.want.projectorArgs
				if len(wantMsgs) != len(allAncestors.mainAncestors) {
					t.Errorf("expected %v ancestors, but got %v: %v", len(wantMsgs), len(allAncestors.mainAncestors), allAncestors)
				}
				for i := range wantMsgs {
					if !cmp.Equal(wantMsgs[i].msg, allAncestors.mainAncestors[i].msg, protocmp.Transform()) {
						t.Errorf("expected msg %v, but got %v", wantMsgs[i].msg, allAncestors.mainAncestors[i].msg)
					}
				}

				if len(wantArgs) != len(allAncestors.projectorArgs) {
					t.Errorf("expected %v args, but got %v: %v", len(wantArgs), len(allAncestors.projectorArgs), allAncestors)
				}
				for i, wantArgList := range wantArgs {
					if len(wantArgList) != len(allAncestors.projectorArgs[i]) {
						t.Errorf("expected %v msgs for arg %v, but got %v: %v", len(wantArgList), i, len(allAncestors.projectorArgs[i]), allAncestors.projectorArgs[i])
					}
					for j, wantArg := range wantArgList {
						if !cmp.Equal(wantArg.msg, allAncestors.projectorArgs[i][j].msg, protocmp.Transform()) {
							t.Errorf("expected arg %v, but got %v", wantArg.msg, allAncestors.projectorArgs[i][j].msg)
						}
					}
				}

			}
		},
		)
	}
}

func TestWhistlerNodesFromValueSource(t *testing.T) {
	tests := []struct {
		name        string
		source      *mbp.ValueSource
		wstlrEnv    *env
		fromMapping bool
		projectors  map[string]*mbp.ProjectorDefinition
		want        []whistlerNode
		wantErrors  bool
	}{
		{
			name:        "test constant msg",
			source:      makeBoolMsg(true),
			wstlrEnv:    nil,
			fromMapping: false,
			projectors:  nil,
			want:        []whistlerNode{whistlerNode{msg: makeBoolMsg(true)}},
			wantErrors:  false,
		},
		{
			name:   "test projector",
			source: makeProjSourceMsg("proj", nil, nil),
			projectors: map[string]*mbp.ProjectorDefinition{
				"proj": makeProjDefMsg("proj", nil),
			},
			fromMapping: true,
			want: []whistlerNode{whistlerNode{
				msg:        makeProjDefMsg("proj", nil),
				projSource: makeProjSourceMsg("proj", nil, nil),
			}},
			wantErrors: false,
		},
		{
			name:        "test bad projector",
			source:      makeProjSourceMsg("proj", nil, nil),
			projectors:  map[string]*mbp.ProjectorDefinition{},
			fromMapping: true,
			wantErrors:  true,
		},
		{
			name:   "test dest value source",
			source: makeDestValSourceMsg("x.y"),
			wstlrEnv: &env{
				targets: map[string][]Node{
					"x": []Node{makeTargetNode("x", 0)},
				},
			},
			want: []whistlerNode{whistlerNode{
				msg:         makeDestValSourceMsg("x.y"),
				nodeInGraph: makeTargetNode("x", 0),
				pathInGraph: "y"}},
			fromMapping: false,
			wantErrors:  false,
		},
		{
			name:   "test bad dest",
			source: makeDestValSourceMsg("x.y"),
			wstlrEnv: &env{
				targets: map[string][]Node{
					"notx": []Node{makeTargetNode("notx", 0)},
				},
			},
			fromMapping: false,
			wantErrors:  true,
		},
		{
			name:   "test no dest",
			source: makeDestValSourceMsg(""),
			wstlrEnv: &env{
				targets: map[string][]Node{
					"notx": []Node{makeTargetNode("notx", 0)},
				},
			},
			fromMapping: false,
			wantErrors:  true,
		},
		{
			name:   "test local var",
			source: makeLocalVarMsg("x.y"),
			wstlrEnv: &env{
				vars: map[string][]Node{
					"x": []Node{makeTargetNode("x", 0)},
				},
			},
			fromMapping: false,
			want: []whistlerNode{whistlerNode{
				msg:         makeLocalVarMsg("x.y"),
				nodeInGraph: makeTargetNode("x", 0),
				pathInGraph: "y",
			}},
			wantErrors: false,
		},
		{
			name: "test projected value",
			source: makeProjectedSourceMsg(
				makeProjSourceMsg("bar", makeBoolMsg(true), nil),
				"foo",
				nil),
			wstlrEnv: nil,
			projectors: map[string]*mbp.ProjectorDefinition{
				"bar": makeProjDefMsg("bar", nil),
			},
			want: []whistlerNode{whistlerNode{
				msg:        makeProjDefMsg("bar", nil),
				projSource: makeProjSourceMsg("bar", makeBoolMsg(true), nil),
			}},
			wantErrors: false,
		},
		{
			name: "test projected value $Not from else",
			source: makeProjectedSourceMsg(
				makeProjSourceMsg("", makeBoolMsg(true), nil),
				"$Not",
				nil),
			wstlrEnv:   nil,
			projectors: nil,
			want: []whistlerNode{whistlerNode{
				msg: makeBoolMsg(true),
			}},
			wantErrors: false,
		},
		{
			name: "test projected value $Not",
			source: makeProjectedSourceMsg(
				makeProjSourceMsg("bar", makeBoolMsg(true), nil),
				"$Not",
				nil),
			wstlrEnv: nil,
			projectors: map[string]*mbp.ProjectorDefinition{
				"bar": makeProjDefMsg("bar", nil),
			},
			want: []whistlerNode{whistlerNode{
				msg:        makeProjDefMsg("bar", nil),
				projSource: makeProjSourceMsg("bar", makeBoolMsg(true), nil),
			}},
			wantErrors: false,
		},
		{
			name: "test no ProjectedValue",
			source: makeProjectedSourceMsg(
				nil,
				"foo",
				nil),
			wstlrEnv:   nil,
			wantErrors: true,
		},
		{
			name: "test bad projector",
			source: makeProjectedSourceMsg(
				makeProjSourceMsg("bar", makeBoolMsg(true), nil),
				"foo",
				nil),
			wstlrEnv: nil,
			projectors: map[string]*mbp.ProjectorDefinition{
				"notbar": makeProjDefMsg("notbar", nil),
			},
			wantErrors: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			wstlrNodes, err := whistlerNodesFromValueSource(test.source, test.wstlrEnv, test.fromMapping, test.projectors)
			if test.wantErrors && err == nil {
				t.Errorf("expected errors getting wstlrNodes for msg {%v}. Got %v", test.source, wstlrNodes)
			} else if !test.wantErrors && err != nil {
				t.Errorf("getting wstlrNodes for msg {%v} failed:\n%w", test.source, err)
			}

			if !test.wantErrors {
				for i, wantWstlrNode := range test.want {
					if !cmp.Equal(wantWstlrNode.msg, wstlrNodes[i].msg, protocmp.Transform()) {
						t.Errorf("expected message %v, but got %v", wantWstlrNode.msg, wstlrNodes[i].msg)
					}
					if !cmp.Equal(wantWstlrNode.projSource, wstlrNodes[i].projSource, protocmp.Transform()) {
						t.Errorf("expected projector source %v, but got projector source %v", wantWstlrNode.projSource, wstlrNodes[i].projSource)
					}
					if !equalsIgnoreID(wantWstlrNode.nodeInGraph, wstlrNodes[i].nodeInGraph) {
						t.Errorf("expected node %v, but got %v", wantWstlrNode.nodeInGraph, wstlrNodes[i].nodeInGraph)
					}
					if wantWstlrNode.pathInGraph != wstlrNodes[i].pathInGraph {
						t.Errorf("expected path %v, but got path %v", wantWstlrNode.pathInGraph, wstlrNodes[i].pathInGraph)
					}
				}
			}
		},
		)
	}
}

func TestReadVarFromEnv(t *testing.T) {
	tests := []struct {
		name       string
		varName    string
		e          *env
		want       Node
		wantCount  int
		wantErrors bool
	}{
		{
			name:    "local lookup",
			varName: "x",
			e: &env{
				vars: map[string][]Node{
					"x": []Node{makeBoolNode(true, 0)},
				},
			},
			want:       makeBoolNode(true, 0),
			wantCount:  1,
			wantErrors: false,
		},
		{
			name:    "parent lookup",
			varName: "x",
			e: &env{
				parent: &env{
					vars: map[string][]Node{
						"x": []Node{makeBoolNode(true, 0)},
					},
				},
			},
			want:       makeBoolNode(true, 0),
			wantCount:  1,
			wantErrors: false,
		},
		{
			name:    "var not in env",
			varName: "y",
			e: &env{
				parent: &env{
					vars: map[string][]Node{
						"x": []Node{makeBoolNode(true, 0)},
					},
				},
			},
			wantErrors: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			nodes, err := readVarFromEnv(test.varName, test.e)
			if test.wantErrors && err == nil {
				t.Errorf("expected errors finding node %v in the environment. Got %v", test.varName, nodes)
			} else if !test.wantErrors && err != nil {
				t.Errorf("finding node %v failed:\n%w", test.varName, err)
			}

			if !test.wantErrors {
				if test.wantCount != len(nodes) {
					t.Errorf("expected to find %v matching nodes in the graph, but found %v. %v", test.wantCount, len(nodes), nodes)
				}
				for _, node := range nodes {
					if !equalsIgnoreID(test.want, node) {
						t.Errorf("expected node %v, but got %v", test.want, node)
					}
				}
			}
		},
		)
	}
}
