// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package main runs the lineage graph generation program
package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"

	fileutil "github.com/GoogleCloudPlatform/healthcare-data-harmonization/mapping_engine/util/ioutil"
	"github.com/GoogleCloudPlatform/healthcare-data-harmonization/mapping_language/transpiler"
	"github.com/googleinterns/healthcare-data-harmonization-lineage/graph"
	"google.golang.org/protobuf/proto"
)

var (
	mappingFile   = flag.String("mapping_file_spec", "", "Mapping file (DHML file).")
	protobufOut   = flag.String("protobuf_out", "", "Output lineage graph file (textproto file).")
	pngOut        = flag.String("png_out", "", "Output file path and name for the PNG rendering")
	dotOut        = flag.String("dot_out", "", "Output file path for the dot text output")
	writeExamples = flag.Bool("write_examples", false, "Write example files from whistle code in examples/whistle to graphs in examples/graphs")
)

const exampleWhistleDir = "./examples/whistle/"
const examplePNGdir = "./examples/png/"
const exampleDotDir = "./examples/dottext/"

func main() {
	flag.Parse()

	if *writeExamples {
		if err := writeExampleGraphs(); err != nil {
			log.Fatalf("failed to write examples:\n%v", err)
		}
	} else {
		if *mappingFile == "" {
			log.Fatalf("The whistle mapping file is not provided or is an empty string.\n" +
				"Please provide file with -mapping_file_spec=/path/to/your-file.wstl")
		}

		dotString, g, err := makeGraphAndDot(*mappingFile, *pngOut)
		if err != nil {
			log.Fatalf("creating the graph failed:\n%v", err)
		}

		fmt.Println(dotString)

		if *protobufOut != "" {
			pbGraph, err := graph.WriteProtobuf(g)
			if err != nil {
				log.Fatalf("Failed to write graph to protobuf:\n%v", err)
			}

			out, err := proto.Marshal(pbGraph)
			if err != nil {
				log.Fatalf("Failed to marshal the protobuf graph %v:\n%v", pbGraph, err)
			}
			if err := ioutil.WriteFile(*protobufOut, out, 0644); err != nil {
				log.Fatalf("Failed to write the graph:\n%v", err)
			}
		}

		if *dotOut != "" {
			if err := ioutil.WriteFile(*dotOut, []byte(dotString), 0644); err != nil {
				log.Fatalf("Failed to write the dot graph:\n%v", err)
			}
		}

	}
}

func makeGraphAndDot(mappingFile string, pngOut string) (string, graph.Graph, error) {
	mpc, err := transpiler.Transpile(string(fileutil.MustRead(mappingFile, "mapping")))
	if err != nil {
		return "", graph.Graph{}, fmt.Errorf("Transpiling whistle failed:\n%w", err)
	}

	g, err := graph.New(mpc)
	if err != nil {
		return "", graph.Graph{}, fmt.Errorf("Graph construction failed:\n%w", err)
	}

	dotString, err := graph.WriteDOTpng(g, pngOut)
	if err != nil {
		return "", graph.Graph{}, fmt.Errorf("Failed to write graph to DOT:\n%w", err)
	}

	return dotString, g, nil
}

func writeExampleGraphs() error {
	var whistleFiles []string
	var pngFiles []string
	var dotFiles []string
	err := filepath.Walk(exampleWhistleDir, func(path string, info os.FileInfo, err error) error {
		if filepath.Ext(path) == ".wstl" {
			whistleFiles = append(whistleFiles, path)
			name := strings.TrimSuffix(info.Name(), ".wstl")
			pngFiles = append(pngFiles, examplePNGdir+name+".png")
			dotFiles = append(dotFiles, exampleDotDir+name+".dot")
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to find example whistle files:\n:%w", err)
	}

	for i := range whistleFiles {
		fmt.Printf("process file %v\n", whistleFiles[i])
		dotString, _, err := makeGraphAndDot(whistleFiles[i], pngFiles[i])
		if err != nil {
			return fmt.Errorf("failed to make graph for file %v:\n%w", whistleFiles[i], err)
		}
		if err := ioutil.WriteFile(dotFiles[i], []byte(dotString), 0644); err != nil {
			return fmt.Errorf("failed to write dot text for file %v:\n%w", whistleFiles[i], err)
		}
	}
	return nil
}
