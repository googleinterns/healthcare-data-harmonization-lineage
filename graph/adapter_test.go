package graph

import (
	"io/ioutil"
	"testing"

	"github.com/google/go-cmp/cmp"
	gpb "github.com/googleinterns/healthcare-data-harmonization-lineage/graph/proto"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestWriteProtobuf(t *testing.T) {
	tests := []struct {
		name       string
		graph      Graph
		want       string
		wantErrors bool
	}{
		{
			name: "test writing",
			graph: Graph{
				Edges: map[int][]int{
					0: ids1(2), // x -> foo
					1: ids0(),  // foo()
					2: ids0(),  // foo()
					3: ids0(),  // 1
				},
				ConditionEdges: map[int][]int{
					0: ids1(1), // x -> true
				},
				ArgumentEdges: map[int][]int{
					2: ids1(3), // foo() -> 1
				},
				Nodes: map[int]Node{
					0: makeTargetNode("x", "root", 0),
					1: makeBoolNode(true, "root", 1),
					2: makeProjNode("foo", "root", 2),
					3: makeFloatNode(1, "root", 3),
				},
			},
			want:       "write_protobuf_1.pb.go",
			wantErrors: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pbGraph, err := WriteProtobuf(test.graph)
			if test.wantErrors && err == nil {
				t.Errorf("expected error writing protobuf")
			} else if !test.wantErrors && err != nil {
				t.Errorf("writing protobuf for %v failed: %w", test.graph, err)
			}

			if !test.wantErrors && err == nil {
				in, err := ioutil.ReadFile("./test_files/" + test.want)
				if err != nil {
					t.Fatalf("couldn't read file %v:\n%v", test.want, err)
				}

				wantGraph := &gpb.Graph{}
				if err := proto.Unmarshal(in, wantGraph); err != nil {
					t.Fatalf("couldn't unmarshal protobuf graph from reference:\n%v", err)
				}
				if !cmp.Equal(wantGraph, pbGraph, protocmp.Transform()) {
					t.Errorf("expected protobuf:\n%v\n, but got:\n%v\n", wantGraph, pbGraph)
				}
			}
		},
		)
	}
}
