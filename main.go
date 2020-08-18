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
	"log"

	mappb "github.com/GoogleCloudPlatform/healthcare-data-harmonization/mapping_engine/proto"
	fileutil "github.com/GoogleCloudPlatform/healthcare-data-harmonization/mapping_engine/util/ioutil"
	"github.com/GoogleCloudPlatform/healthcare-data-harmonization/mapping_language/transpiler"
	"github.com/googleinterns/healthcare-data-harmonization-lineage/graph"
)

var (
	mappingFile = flag.String("mapping_file_spec", "", "Mapping file (DHML file).")
)

func main() {
	flag.Parse()

	var mpc *mappb.MappingConfig
	var err error

	if *mappingFile == "" {
		log.Fatalf("The whistle mapping file is not provided or is an empty string.\n" +
			"Please provide file with -mapping_file_spec=/path/to/your-file.wstl")
	}

	if mpc, err = transpiler.Transpile(string(fileutil.MustRead(*mappingFile, "mapping"))); err != nil {
		log.Fatalf("Transpiling whistle failed:\n%v", err)
	}

	g, err := graph.New(mpc)
	if err != nil {
		log.Fatalf("Graph construction failed:\n%v", err)
	}
	fmt.Println(g)
}
