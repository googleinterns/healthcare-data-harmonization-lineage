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
				Edges: map[int][]int{
					makeTargetNode("x", "root", 0).ID(): []int{makeBoolNode(true, "root", 1).ID()},
					makeBoolNode(true, "root", 1).ID():  []int{},
				},
				ArgumentEdges: map[int][]int{},
				ConditionEdges: map[int][]int{
					0: ids0(),
				},
				Nodes: map[int]Node{
					makeTargetNode("x", "root", 0).ID(): makeTargetNode("x", "root", 0),
					makeBoolNode(true, "root", 1).ID():  makeBoolNode(true, "root", 1),
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
				Edges: map[int][]int{
					0: ids1(1),
					1: ids1(2),
					2: ids1(3),
					3: ids0(),
				},
				ArgumentEdges: map[int][]int{
					1: ids0(),
				},
				ConditionEdges: map[int][]int{
					0: ids0(),
					2: ids0(),
				},
				Nodes: map[int]Node{
					0: makeTargetNode("x", "root", 0),
					1: makeProjNode("proj1", "root", 1),
					2: makeTargetNode("y", "proj1", 2),
					3: makeFloatNode(5.0, "proj1", 3),
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
				Edges: map[int][]int{
					0: ids1(1),
					1: ids1(2),
					2: ids1(3),
					3: ids1(4),
					4: ids0(),
					5: ids0(),
				},
				ArgumentEdges: map[int][]int{
					1: ids2(4, 5),
				},
				ConditionEdges: map[int][]int{
					0: ids0(),
					2: ids0(),
				},
				Nodes: map[int]Node{
					0: makeTargetNode("x", "root", 0),
					1: makeProjNode("proj1", "root", 1),
					2: makeTargetNode("y", "proj1", 2),
					3: makeArgNode(1, "", "proj1", 3),
					4: makeBoolNode(true, "root", 4),
					5: makeFloatNode(5.0, "root", 5),
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
				Edges: map[int][]int{
					0: ids1(1), // x -> proj
					1: ids1(2), // proj -> y
					3: ids1(4), // foo -> a
					4: ids1(5), // a -> bar
					5: ids1(6), // bar -> b
					6: ids1(7), // b -> string
					7: ids0(),  // string
					2: ids1(8), // y -> arg.a.b
					8: ids1(6), // arg.a.b -> b
				},
				ArgumentEdges: map[int][]int{
					1: ids1(3), // proj -> foo
					3: ids0(),  // foo
					5: ids0(),  // bar
				},
				ConditionEdges: map[int][]int{
					0: ids0(),
					2: ids0(),
					4: ids0(),
					6: ids0(),
				},
				Nodes: map[int]Node{
					0: makeTargetNode("x", "root", 0),
					1: makeProjNode("proj", "root", 1),
					2: makeTargetNode("y", "proj", 2),
					3: makeProjNode("foo", "root", 3),
					4: makeTargetNode("a", "foo", 4),
					5: makeProjNode("bar", "foo", 5),
					6: makeTargetNode("b", "bar", 6),
					7: makeStringNode("b", "bar", 7),
					8: makeArgNode(1, ".a.b", "proj", 8),
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
				Edges: map[int][]int{
					0: ids1(1), // x -> proj1
					1: ids1(2), // proj1 -> y
					2: ids1(3), // y -> arg 1
					3: ids1(4), // arg 1 -> proj2
					4: ids1(5), // proj2 -> z
					5: ids1(6), // z -> "foo"
					6: ids0(),  // "foo"
				},
				ArgumentEdges: map[int][]int{
					1: ids1(4), // proj1 -> proj2
					4: ids0(),  // proj2
				},
				ConditionEdges: map[int][]int{
					0: ids0(), // x
					2: ids0(), // y
					5: ids0(), /// z
				},
				Nodes: map[int]Node{
					0: makeTargetNode("x", "root", 0),
					1: makeProjNode("proj1", "root", 1),
					2: makeTargetNode("y", "proj1", 2),
					3: makeArgNode(1, "", "proj1", 3),
					4: makeProjNode("proj2", "root", 4),
					5: makeTargetNode("z", "proj2", 5),
					6: makeStringNode("foo", "proj2", 6),
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
				Edges: map[int][]int{
					0: ids1(2),
					1: ids0(),
					2: ids1(1),
				},
				ConditionEdges: map[int][]int{
					0: ids0(),
					2: ids0(),
				},
				Nodes: map[int]Node{
					0: makeTargetNode("b", "root", 0),
					1: makeStringNode("a", "root", 1),
					2: makeVarNode("a", "root", 2),
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
				Edges: map[int][]int{
					0: ids1(1), // a -> "a"
					1: ids0(),  // "a"
					2: ids1(0), // b -> a
				},
				ConditionEdges: map[int][]int{
					0: ids0(),
					2: ids0(),
				},
				Nodes: map[int]Node{
					0: makeTargetNode("a", "root", 0),
					1: makeStringNode("a", "root", 1),
					2: makeTargetNode("b", "root", 2),
				},
			},
			wantErrors: false,
		},
		{
			name: "test simple condition",
			whistle: `
			x (if true): 5`,
			want: Graph{
				Edges: map[int][]int{
					0: ids1(1), // a -> 5
					1: ids0(),  // 5
					2: ids0(),  // true
				},
				ConditionEdges: map[int][]int{
					0: ids1(2),
				},
				Nodes: map[int]Node{
					0: makeTargetNode("x", "root", 0),
					1: makeFloatNode(5, "root", 1),
					2: makeBoolNode(true, "root", 2),
				},
			},
			wantErrors: false,
		},
		{
			name: "test simple projected condition",
			whistle: `
			x (if $Eq(4, 2)): 5`,
			want: Graph{
				Edges: map[int][]int{
					0: ids1(1), // x -> 5
					1: ids0(),  // 5
					2: ids0(),  // $Eq
					3: ids0(),  // 4
					4: ids0(),  // 2
				},
				ArgumentEdges: map[int][]int{
					2: ids2(3, 4),
				},
				ConditionEdges: map[int][]int{
					0: ids1(2), // x -> $Eq
				},
				Nodes: map[int]Node{
					0: makeTargetNode("x", "root", 0),
					1: makeFloatNode(5, "root", 1),
					2: makeProjNode("$Eq", "root", 2),
					3: makeFloatNode(4, "root", 3),
					4: makeFloatNode(2, "root", 4),
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
				Edges: map[int][]int{
					0: ids1(1),    // x -> foo()
					1: ids2(2, 3), // foo() -> a, b
					2: ids1(4),    // a -> 1
					3: ids1(5),    // b -> 2
					4: ids0(),     // 1
					5: ids0(),     // 2
					6: ids0(),     // true
					7: ids0(),     // $Not
					8: ids0(),     // true
				},
				ArgumentEdges: map[int][]int{
					1: ids0(),  // foo()
					7: ids1(8), // $Not -> true
				},
				ConditionEdges: map[int][]int{
					0: ids0(),  // x
					2: ids1(6), // a -> true
					3: ids1(7), // b -> $Not
				},
				Nodes: map[int]Node{
					0: makeTargetNode("x", "root", 0),
					1: makeProjNode("foo", "root", 1),
					2: makeTargetNode("a", "foo", 2),
					3: makeTargetNode("b", "foo", 3),
					4: makeFloatNode(1, "foo", 4),
					5: makeFloatNode(2, "foo", 5),
					6: makeBoolNode(true, "foo", 6),
					7: makeProjNode("$Not", "foo", 7),
					8: makeBoolNode(true, "foo", 8),
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
				Edges: map[int][]int{
					0: ids1(1),    // x -> foo()
					1: ids2(2, 3), // foo() -> a, b
					2: ids1(4),    // a -> 1
					3: ids1(5),    // b -> 2
					4: ids0(),     // 1
					5: ids0(),     // 2
					6: ids0(),     // bar()
					7: ids0(),     // $Not
					8: ids0(),     // bar()
				},
				ArgumentEdges: map[int][]int{
					1: ids0(),  // foo()
					7: ids1(8), // $Not -> bar()
					6: ids0(),  // bar()
					8: ids0(),  // bar()
				},
				ConditionEdges: map[int][]int{
					0: ids0(),  // x
					2: ids1(6), // a -> true
					3: ids1(7), // b -> $Not
				},
				Nodes: map[int]Node{
					0: makeTargetNode("x", "root", 0),
					1: makeProjNode("foo", "root", 1),
					2: makeTargetNode("a", "foo", 2),
					3: makeTargetNode("b", "foo", 3),
					4: makeFloatNode(1, "foo", 4),
					5: makeFloatNode(2, "foo", 5),
					6: makeProjNode("bar", "foo", 6),
					7: makeProjNode("$Not", "foo", 7),
					8: makeProjNode("bar", "foo", 8),
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
				Edges: map[int][]int{
					0: ids1(1),    // a -> "a1"
					1: ids0(),     // "a1"
					2: ids1(3),    // a' -> "a2"
					3: ids0(),     // "a2"
					4: ids2(0, 2), // x -> a, a'
					5: ids0(),     // true
					6: ids0(),     // false
				},
				ConditionEdges: map[int][]int{
					0: ids1(5), // a -> true
					2: ids1(6), // a' -> false
					4: ids0(),
				},
				Nodes: map[int]Node{
					0: makeTargetNode("a", "root", 0),
					1: makeStringNode("a1", "root", 1),
					2: makeTargetNode("a", "root", 2),
					3: makeStringNode("a2", "root", 3),
					4: makeTargetNode("x", "root", 4),
					5: makeBoolNode(true, "root", 5),
					6: makeBoolNode(false, "root", 6),
				},
			},
			wantErrors: false,
		},
		{
			name: "test dest with conditional field",
			whistle: `
			a: foo()
			def foo() {
				b (if true): "b true"
				b (if false): "b false"
			}
			x: dest a.b`,
			want: Graph{
				Edges: map[int][]int{
					0: ids1(1),    // a -> foo()
					1: ids2(2, 3), // foo -> b1, b2
					2: ids1(4),    // b1 -> "b true"
					3: ids1(5),    // b2 -> "b false"
					4: ids0(),     // "b true"
					5: ids0(),     // "b false"
					6: ids2(2, 3), // x -> b1, b2
					7: ids0(),     // true
					8: ids0(),     // false
				},
				ConditionEdges: map[int][]int{
					0: ids0(),  // a
					2: ids1(7), // b1 -> true
					3: ids1(8), // b2 -> false
					6: ids0(),  // x
				},
				ArgumentEdges: map[int][]int{
					1: ids0(), // foo()
				},
				Nodes: map[int]Node{
					0: makeTargetNode("a", "root", 0),
					1: makeProjNode("foo", "root", 1),
					2: makeTargetNode("b", "foo", 2),
					3: makeTargetNode("b", "foo", 3),
					4: makeStringNode("b true", "foo", 4),
					5: makeStringNode("b false", "foo", 5),
					6: makeTargetNode("x", "root", 6),
					7: makeBoolNode(true, "foo", 7),
					8: makeBoolNode(false, "foo", 8),
				},
			},
			wantErrors: false,
		},
		{
			name: "test recursive",
			whistle: `
			x: foo()
			def foo() {
				y: bar()
			}

			def bar() {
				z: foo()
			}`,
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

/*
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
				Edges: map[int][]int{
					makeTargetNode("x", 0).ID(): []int{makeBoolNode(true, 1).ID()},
					makeBoolNode(true, 1).ID():  []int{},
				},
				ArgumentEdges: map[int][]int{},
				ConditionEdges: map[int][]int{
					0: ids0(),
				},
				Nodes: map[int]Node{
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
				Edges: map[int][]int{
					0: ids1(1),
					1: ids1(2),
					2: ids1(3),
					3: ids0(),
				},
				ArgumentEdges: map[int][]int{
					1: ids0(),
				},
				ConditionEdges: map[int][]int{
					0: ids0(),
					2: ids0(),
				},
				Nodes: map[int]Node{
					0: makeTargetNode("x", 0),
					1: makeProjNode("proj1", 1),
					2: makeTargetNode("y", 2),
					3: makeIntNode(0, 3),
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
				Edges: map[int][]int{
					0: ids1(1),
					1: ids1(2),
					2: ids1(3),
					3: ids1(4),
					4: ids0(),
					5: ids0(),
				},
				ArgumentEdges: map[int][]int{
					1: ids2(4, 5),
				},
				ConditionEdges: map[int][]int{
					0: ids0(),
					2: ids0(),
				},
				Nodes: map[int]Node{
					0: makeTargetNode("x", 0),
					1: makeProjNode("proj1", 1),
					2: makeTargetNode("y", 2),
					3: makeArgNode(1, "", 3),
					4: makeBoolNode(true, 4),
					5: makeFloatNode(5.0, 5),
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
				Edges: map[int][]int{
					makeTargetNode("x", 0).ID():   []int{makeProjNode("proj1", 1).ID()},
					makeProjNode("proj1", 1).ID(): []int{makeTargetNode("y", 2).ID()},
					makeTargetNode("y", 2).ID():   []int{makeArgNode(1, "", 3).ID()},
					makeArgNode(1, "", 3).ID():    []int{makeProjNode("proj2", 4).ID()},
					makeProjNode("proj2", 4).ID(): []int{makeTargetNode("z", 5).ID()},
					makeTargetNode("z", 5).ID():   []int{makeStringNode("foo", 6).ID()},
					makeStringNode("foo", 6).ID(): []int{},
				},
				ArgumentEdges: map[int][]int{
					makeProjNode("proj1", 1).ID(): []int{makeProjNode("proj2", 4).ID()},
					makeProjNode("proj2", 4).ID(): []int{},
				},
				ConditionEdges: map[int][]int{
					0: ids0(),
					2: ids0(),
					5: ids0(),
				},
				Nodes: map[int]Node{
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
*/

func TestAddArgLineages(t *testing.T) {
	tests := []struct {
		name       string
		args       [][]whistlerNode
		e          *env
		projNode   *ProjectorNode
		projectors map[string]*mbp.ProjectorDefinition
		graph      Graph
		startID    int
		want       *env
		wantErrors bool
	}{
		{
			name: "add arguments",
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
				Edges: map[int][]int{
					0: ids0(),
				},
				ArgumentEdges: map[int][]int{
					0: ids0(),
				},
				Nodes: map[int]Node{
					0: makeProjNode("proj1", "root", 0),
				},
				targetLineages: map[int]targetLineage{},
			},
			projNode: makeProjNode("proj1", "root", 0),
			projectors: map[string]*mbp.ProjectorDefinition{
				"proj1": makeProjDefMsg("proj1", nil), // the projector mappings don't matter for this test
			},
			startID: 1,
			want: &env{
				name: "proj1",
				args: [][]argLineage{
					[]argLineage{
						argLineage{
							node: makeBoolNode(true, "root", 1),
						},
						argLineage{
							node: makeIntNode(1, "root", 2),
						},
					},
					[]argLineage{
						argLineage{
							node: makeIntNode(2, "root", 3),
						},
					},
				},
			},
			wantErrors: false,
		},
		{
			name: "test add target argument",
			args: [][]whistlerNode{
				[]whistlerNode{
					whistlerNode{
						msg: makeMappingMsg("x", makeBoolMsg(true), nil),
					},
				},
			},
			e: &env{
				name: "root",
			},
			graph: Graph{
				Edges: map[int][]int{
					0: ids0(),
				},
				ArgumentEdges: map[int][]int{
					0: ids0(),
				},
				ConditionEdges: map[int][]int{},
				Nodes: map[int]Node{
					0: makeProjNode("proj1", "root", 0),
				},
				targetLineages: map[int]targetLineage{
					1: targetLineage{
						node: makeTargetNode("x", "root", 1),
						childTargets: map[string][]targetLineage{
							"y": []targetLineage{
								targetLineage{
									node: makeTargetNode("y", "root", 2),
								},
							},
						},
					},
				},
			},
			projNode: makeProjNode("proj1", "root", 0),
			projectors: map[string]*mbp.ProjectorDefinition{
				"proj1": makeProjDefMsg("proj1", nil), // the projector mappings don't matter for this test
			},
			startID: 1,
			want: &env{
				name: "proj1",
				args: [][]argLineage{
					[]argLineage{
						argLineage{
							node: makeTargetNode("x", "root", 1),
							childTargets: map[string][]targetLineage{
								"y": []targetLineage{
									targetLineage{
										node: makeTargetNode("y", "root", 2),
									},
								},
							},
						},
					},
				},
			},
			wantErrors: false,
		},
		{
			name: "test lineage not cached",
			args: [][]whistlerNode{
				[]whistlerNode{
					whistlerNode{
						msg: makeMappingMsg("x", makeBoolMsg(true), nil),
					},
				},
			},
			e: &env{
				name: "root",
			},
			graph: Graph{
				Edges: map[int][]int{
					0: ids0(),
				},
				ArgumentEdges: map[int][]int{
					0: ids0(),
				},
				ConditionEdges: map[int][]int{},
				Nodes: map[int]Node{
					0: makeProjNode("proj1", "root", 0),
				},
				targetLineages: map[int]targetLineage{},
			},
			projNode: makeProjNode("proj1", "root", 0),
			projectors: map[string]*mbp.ProjectorDefinition{
				"proj1": makeProjDefMsg("proj1", nil), // the projector mappings don't matter for this test
			},
			startID:    1,
			wantErrors: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setIncID(test.startID)
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
						if !equalsIgnoreID(wantMsg.node, e.args[i][j].node) {
							t.Errorf("expected node {%v}, but got {%v}", wantMsg.node, e.args[i][j].node)
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
				Edges: map[int][]int{},
				Nodes: map[int]Node{},
			},
			node:        makeBoolNode(true, "root", 0),
			isArg:       false,
			isCondition: false,
			isNew:       true,
			want: Graph{
				Edges: map[int][]int{
					0: ids0(),
				},
				ConditionEdges: map[int][]int{
					0: ids0(),
				},
				Nodes: map[int]Node{
					0: makeBoolNode(true, "root", 0),
				},
			},
			wantErrors: false,
		},
		{
			name: "new ProjectorNode no descendant",
			graph: Graph{
				Edges:         map[int][]int{},
				ArgumentEdges: map[int][]int{},
				Nodes:         map[int]Node{},
			},
			node:        makeProjNode("proj", "root", 0),
			isArg:       false,
			isCondition: false,
			isNew:       true,
			want: Graph{
				Edges: map[int][]int{
					0: ids0(),
				},
				ArgumentEdges: map[int][]int{
					0: ids0(),
				},
				Nodes: map[int]Node{
					0: makeProjNode("proj", "root", 0),
				},
			},
			wantErrors: false,
		},
		{
			name: "new already in graph",
			graph: Graph{
				Edges: map[int][]int{
					0: ids0(),
				},
				Nodes: map[int]Node{
					0: makeBoolNode(true, "root", 0),
				},
			},
			node:        makeBoolNode(true, "root", 0),
			isArg:       false,
			isCondition: false,
			isNew:       true,
			wantErrors:  true,
		},
		{
			name: "node already in graph with descendant",
			graph: Graph{
				Edges: map[int][]int{
					1: ids0(),
					0: ids0(),
				},
				ConditionEdges: map[int][]int{
					1: ids0(),
				},
				Nodes: map[int]Node{
					1: makeTargetNode("x", "root", 1),
					0: makeBoolNode(true, "root", 0),
				},
			},
			node:        makeBoolNode(true, "root", 0),
			isArg:       false,
			isCondition: false,
			isNew:       false,
			descendant:  makeTargetNode("x", "root", 1),
			want: Graph{
				Edges: map[int][]int{
					1: ids1(0),
					0: ids0(),
				},
				ConditionEdges: map[int][]int{
					1: ids0(),
				},
				Nodes: map[int]Node{
					1: makeTargetNode("x", "root", 1),
					0: makeBoolNode(true, "root", 0),
				},
			},
			wantErrors: false,
		},
		{
			name: "new descendant not in graph",
			graph: Graph{
				Edges: map[int][]int{},
				Nodes: map[int]Node{},
			},
			node:       makeBoolNode(true, "root", 1),
			descendant: makeTargetNode("x", "root", 0),
			isArg:      false,
			isNew:      true,
			want:       Graph{},
			wantErrors: true,
		},
		{
			name: "isArg descendant",
			graph: Graph{
				Edges: map[int][]int{
					0: ids0(),
				},
				ArgumentEdges: map[int][]int{
					0: ids0(),
				},
				Nodes: map[int]Node{
					0: makeProjNode("proj", "root", 0),
				},
			},
			node:       makeBoolNode(true, "root", 1),
			descendant: makeProjNode("proj", "root", 0),
			isArg:      true,
			isNew:      true,
			want: Graph{
				Edges: map[int][]int{
					0: ids0(),
					1: ids0(),
				},
				ArgumentEdges: map[int][]int{
					0: ids1(1),
				},
				Nodes: map[int]Node{
					0: makeProjNode("proj", "root", 0),
					1: makeBoolNode(true, "root", 1),
				},
			},
			wantErrors: false,
		},
		{
			name: "isCondition descendant",
			graph: Graph{
				Edges: map[int][]int{
					0: ids0(),
				},
				ConditionEdges: map[int][]int{
					0: ids0(),
				},
				Nodes: map[int]Node{
					0: makeTargetNode("x", "root", 0),
				},
			},
			node:        makeBoolNode(true, "root", 1),
			descendant:  makeTargetNode("x", "root", 0),
			isArg:       false,
			isCondition: true,
			isNew:       true,
			want: Graph{
				Edges: map[int][]int{
					0: ids0(),
					1: ids0(),
				},
				ConditionEdges: map[int][]int{
					0: ids1(1),
				},
				Nodes: map[int]Node{
					0: makeTargetNode("x", "root", 0),
					1: makeBoolNode(true, "root", 1),
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

func TestIsRecursive(t *testing.T) {
	tests := []struct {
		name       string
		graph      Graph
		node       Node
		want       bool
		wantErrors bool
	}{
		{
			name: "not recursive",
			graph: Graph{
				Edges: map[int][]int{
					0: ids1(1),
					1: ids0(),
				},
				Nodes: map[int]Node{
					0: makeTargetNode("x", "root", 0),
					1: makeBoolNode(true, "root", 1),
				},
			},
			node:       makeBoolNode(true, "root", 1),
			want:       false,
			wantErrors: false,
		},
		{
			name: "recursive",
			graph: Graph{
				Edges: map[int][]int{
					0: ids1(1), // foo() -> x
					1: ids1(2), // x -> bar()
					2: ids1(3), // bar() -> yn
					3: ids1(1), // y -> foo'()
					4: ids1(0), // foo'()
				},
				Nodes: map[int]Node{
					0: makeProjNode("foo", "root", 0),
					1: makeTargetNode("x", "foo", 1),
					2: makeProjNode("bar", "foo", 2),
					3: makeTargetNode("y", "bar", 3),
					4: makeProjNode("foo", "bar", 4),
				},
			},
			node:       makeProjNode("foo", "bar", 4),
			want:       true,
			wantErrors: false,
		},
		{
			name: "badly-formed graph",
			graph: Graph{
				Edges: map[int][]int{},
				Nodes: map[int]Node{
					0: makeBoolNode(true, "root", 0),
				},
			},
			node:       makeBoolNode(true, "root", 0),
			wantErrors: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recursive, err := isRecursive(test.graph, test.node)
			if test.wantErrors && err == nil {
				t.Errorf("expected errors checking graph {%v}", test.graph)
			} else if !test.wantErrors && err != nil {
				t.Errorf("checking graph {%v} failed:\n%w", test.graph, err)
			}

			if !test.wantErrors {
				if !test.want && recursive {
					t.Errorf("expected graph %v to be recursive", test.graph)
				}
				if test.want && !recursive {
					t.Errorf("didn't expect graph %v to be recursive", test.graph)
				}
			}
		},
		)
	}
}

func TestGetNode(t *testing.T) {
	tests := []struct {
		name       string
		wstlrNode  whistlerNode
		wstlrEnv   *env
		wantNode   Node
		wantIsNew  bool
		wantErrors bool
	}{
		{
			name: "test not new node",
			wstlrNode: whistlerNode{
				nodeInGraph: makeBoolNode(true, "root", 0),
			},
			wantNode:   makeBoolNode(true, "root", 0),
			wantIsNew:  false,
			wantErrors: false,
		},
		{
			name: "test new constant bool",
			wstlrNode: whistlerNode{
				msg: makeBoolMsg(true),
			},
			wstlrEnv: &env{
				name: "root",
			},
			wantNode:   makeBoolNode(true, "root", 0),
			wantIsNew:  true,
			wantErrors: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			node, isNew, err := getNode(test.wstlrNode, test.wstlrEnv)
			if test.wantErrors && err == nil {
				t.Errorf("expected errors getting node for msg {%v}. Got node %v", test.wstlrNode, node)
			} else if !test.wantErrors && err != nil {
				t.Errorf("getting node for msg {%v} failed:\n%w", test.wstlrNode, err)
			}

			if !test.wantErrors {
				if test.wantIsNew != isNew {
					t.Errorf("Wanted a new node: %v. Got a new node: %v", test.wantIsNew, isNew)
				}
				if !equalsIgnoreID(test.wantNode, node) {
					t.Errorf("wanted node %v but got node %v", test.wantNode, node)
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
		wstlrEnv   *env
		wantErrors bool
		want       Node
	}{
		{
			name: "test new field mapping",
			msg:  makeMappingMsg("x", nil, nil),
			wstlrEnv: &env{
				name: "root",
			},
			want: &TargetNode{
				id:      0,
				Name:    "x",
				Context: "root",
			},
			wantErrors: false,
		},
		{
			name: "test new projector",
			msg:  makeProjDefMsg("proj1", nil),
			wstlrEnv: &env{
				name: "root",
			},
			want: &ProjectorNode{
				id:      0,
				Name:    "proj1",
				Context: "root",
			},
			wantErrors: false,
		},
		{
			name: "test new argument",
			msg:  makeArgMsg(1, ""),
			wstlrEnv: &env{
				name: "root",
				args: make([][]argLineage, 1),
			},
			want: &ArgumentNode{
				id:      0,
				Index:   1,
				Context: "root",
			},
			wantErrors: false,
		},
		{
			name: "test new $root",
			msg:  makeArgMsg(1, ""),
			wstlrEnv: &env{
				name: "root",
				args: make([][]argLineage, 0),
			},
			want: &RootNode{
				id:      0,
				Context: "root",
			},
			wantErrors: false,
		},
		{
			name:       "test unsupported message",
			msg:        makeMappingConfigMsg(nil, nil),
			wantErrors: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			node, err := newNode(test.msg, test.wstlrEnv)
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

func TestFieldMappingConditions(t *testing.T) {
	tests := []struct {
		name       string
		msg        *mbp.FieldMapping
		wstlrEnv   *env
		projectors map[string]*mbp.ProjectorDefinition
		want       []whistlerNode
	}{
		{
			name: "test conditional $And",
			msg: makeMappingMsg("x", makeBoolMsg(true), makeProjSourceMsg(
				"$And",
				makeBoolMsg(false),
				nil)),
			want: []whistlerNode{whistlerNode{msg: makeProjSourceMsg("$And", makeBoolMsg(false), nil)}},
		},
		{
			name: "test conditional mapping",
			msg:  makeMappingMsg("x", makeBoolMsg(true), makeBoolMsg(false)),
			want: []whistlerNode{whistlerNode{msg: makeBoolMsg(false)}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ancestors, err := fieldMappingConditions(test.msg, test.wstlrEnv, test.projectors)
			if err != nil {
				t.Errorf("getting conditions for msg {%v} failed:\n%w", test.msg, err)
			}

			conditionAncestors := test.want
			if len(conditionAncestors) != len(ancestors) {
				t.Errorf("expected %v condition ancestors, but got %v: %v", len(conditionAncestors), len(ancestors), ancestors)
			}
			for i := range conditionAncestors {
				if !cmp.Equal(conditionAncestors[i].msg, ancestors[i].msg, protocmp.Transform()) {
					t.Errorf("expected msg %v, but got %v", conditionAncestors[i].msg, ancestors[i].msg)
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
			name: "test $root ancestors",
			e: &env{
				name:   "proj1",
				parent: nil,
				args:   [][]argLineage{},
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

func TestFromInputAncestor(t *testing.T) {
	tests := []struct {
		name       string
		msg        *mbp.ValueSource
		wstlrEnv   *env
		want       []whistlerNode
		wantErrors bool
	}{
		{
			name: "test Argument 0",
			wstlrEnv: &env{
				name:   "proj1",
				parent: nil,
				args: [][]argLineage{
					[]argLineage{
						argLineage{
							node: makeBoolNode(true, "root", 0),
						},
					},
				},
			},
			msg: makeArgMsg(1, ""),
			want: []whistlerNode{whistlerNode{
				nodeInGraph: makeBoolNode(true, "root", 0)}},
			wantErrors: false,
		},
		{
			name: "test $root ancestors",
			wstlrEnv: &env{
				name:   "proj1",
				parent: nil,
				args:   [][]argLineage{},
			},
			msg:        makeArgMsg(1, ""),
			want:       nil,
			wantErrors: false,
		},
		{
			name: "test arg too high",
			wstlrEnv: &env{
				args: [][]argLineage{},
			},
			msg:        makeArgMsg(2, ""),
			wantErrors: true,
		},
		{
			name: "test arg fields",
			wstlrEnv: &env{
				args: [][]argLineage{
					[]argLineage{
						argLineage{
							node: makeProjNode("foo", "root", 0),
							childTargets: map[string][]targetLineage{
								"x": []targetLineage{
									targetLineage{node: makeTargetNode("x", "root", 1)},
								},
							},
						},
					},
				},
			},
			msg:        makeArgMsg(1, ".x"),
			want:       []whistlerNode{whistlerNode{nodeInGraph: makeTargetNode("x", "root", 1)}},
			wantErrors: false,
		},
		{
			name: "test no childTargets",
			wstlrEnv: &env{
				args: [][]argLineage{
					[]argLineage{
						argLineage{
							node:         makeProjNode("foo", "root", 0),
							childTargets: nil,
						},
					},
				},
			},
			msg:        makeArgMsg(1, ".x"),
			wantErrors: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ancestors, err := fromInputAncestor(test.msg.Source.(*mbp.ValueSource_FromInput).FromInput, test.wstlrEnv)
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
		},
		{
			name:            "test nil source",
			msg:             makeProjDefMsg("proj", nil),
			projValueSource: makeProjSourceMsg("proj", nil, nil),
			want:            ancestorCollection{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			allAncestors, err := projectorAncestors(test.msg, test.projValueSource, nil, nil)
			if err != nil {
				t.Errorf("getting ancestors for msg {%v} failed:\n%w", test.msg, err)
			}

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
				targets: map[string][]targetLineage{
					"x": []targetLineage{targetLineage{
						node: makeTargetNode("x", "root", 0),
						childTargets: map[string][]targetLineage{
							"y": []targetLineage{targetLineage{
								node: makeTargetNode("y", "root", 1),
							}},
						}}},
				},
			},
			want: []whistlerNode{whistlerNode{
				msg:         makeDestValSourceMsg("x.y"),
				nodeInGraph: makeTargetNode("y", "root", 1)}},
			fromMapping: false,
			wantErrors:  false,
		},
		{
			name:   "test bad dest",
			source: makeDestValSourceMsg("x.y"),
			wstlrEnv: &env{
				targets: map[string][]targetLineage{
					"notx": []targetLineage{targetLineage{node: makeTargetNode("notx", "root", 0)}},
				},
			},
			fromMapping: false,
			wantErrors:  true,
		},
		{
			name:        "test no dest",
			source:      makeDestValSourceMsg(""),
			wstlrEnv:    &env{},
			fromMapping: false,
			wantErrors:  true,
		},
		{
			name:   "test local var",
			source: makeLocalVarMsg("x.y"),
			wstlrEnv: &env{
				vars: map[string][]targetLineage{
					"x": []targetLineage{targetLineage{
						node: makeTargetNode("x", "root", 0),
						childTargets: map[string][]targetLineage{
							"y": []targetLineage{targetLineage{
								node: makeTargetNode("y", "root", 1),
							}},
						},
					}},
				},
			},
			fromMapping: false,
			want: []whistlerNode{whistlerNode{
				msg:         makeLocalVarMsg("x.y"),
				nodeInGraph: makeTargetNode("y", "root", 1),
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
				}
			}
		},
		)
	}
}

func TestFindNodesInGraph(t *testing.T) {
	tests := []struct {
		name       string
		currNode   Node
		path       []string
		lineages   map[string][]targetLineage
		want       []Node
		wantErrors bool
	}{
		{
			name:       "test base case",
			currNode:   makeBoolNode(true, "root", 0),
			path:       nil,
			want:       []Node{makeBoolNode(true, "root", 0)},
			wantErrors: false,
		},
		{
			name:     "test find node only targets",
			currNode: makeTargetNode("a", "root", 0),
			path:     []string{"b", "c"},
			lineages: map[string][]targetLineage{
				"b": []targetLineage{targetLineage{
					node: makeTargetNode("b", "root", 1),
					childTargets: map[string][]targetLineage{
						"c": []targetLineage{
							targetLineage{
								node: makeTargetNode("c", "root", 2),
							},
						},
					},
				}},
			},
			want:       []Node{makeTargetNode("c", "root", 2)},
			wantErrors: false,
		},
		{
			name:     "test find node; bad path",
			currNode: makeTargetNode("a", "root", 0),
			path:     []string{"b", "c"},
			lineages: map[string][]targetLineage{
				"b": []targetLineage{targetLineage{
					node: makeTargetNode("b", "root", 1),
					childTargets: map[string][]targetLineage{
						"not_c": []targetLineage{
							targetLineage{
								node: makeTargetNode("not_c", "root", 2),
							},
						},
					},
				}},
			},
			wantErrors: true,
		},
		{
			name:     "test find node composite target",
			currNode: makeTargetNode("a", "root", 0),
			path:     []string{"b", "c"},
			lineages: map[string][]targetLineage{
				"b.c": []targetLineage{targetLineage{
					node: makeTargetNode("b.c", "root", 1),
				}},
			},
			want:       []Node{makeTargetNode("b.c", "root", 1)},
			wantErrors: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			nodes, err := findNodesInGraph(test.path, test.currNode, test.lineages)
			if test.wantErrors && err == nil {
				t.Errorf("expected errors finding node %v with path %v. Got nodes %v", test.currNode, test.path, nodes)
			} else if !test.wantErrors && err != nil {
				t.Errorf("finding nodes %v with path %v failed:\n%w", test.currNode, test.path, err)
			}

			if !test.wantErrors {
				if len(test.want) != len(nodes) {
					t.Errorf("wanted %v nodes but got %v", len(test.want), len(nodes))
				}
				for i, node := range nodes {
					if !equalsIgnoreID(test.want[i], node) {
						t.Errorf("wanted node %v but got node %v", test.want, node)
					}
				}
			}
		},
		)
	}
}

/*
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
				vars: map[string][]nodeEnv{
					"x": []nodeEnv{nodeEnv{node: makeTargetNode("x", "root", 0)}},
				},
			},
			want:       makeTargetNode("x", "root", 0),
			wantCount:  1,
			wantErrors: false,
		},
		{
			name:    "parent lookup",
			varName: "x",
			e: &env{
				parent: &env{
					vars: map[string][]nodeEnv{
						"x": []nodeEnv{nodeEnv{node: makeTargetNode("x", "root", 0)}},
					},
				},
			},
			want:       makeTargetNode("x", "root", 0),
			wantCount:  1,
			wantErrors: false,
		},
		{
			name:    "var not in env",
			varName: "y",
			e: &env{
				parent: &env{
					vars: map[string][]nodeEnv{
						"x": []nodeEnv{nodeEnv{node: makeTargetNode("x", "root", 0)}},
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
				for _, nodeEnv := range nodes {
					if !equalsIgnoreID(test.want, nodeEnv.node) {
						t.Errorf("expected node %v, but got %v", test.want, nodeEnv.node)
					}
				}
			}
		},
		)
	}
}
*/
