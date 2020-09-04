# Data Harmonization Lineage

**This is not an officially supported Google product.**

## What is this?
The [whistle mapping language](https://github.com/GoogleCloudPlatform/healthcare-data-harmonization/blob/master/mapping_language/doc/reference.md) is used to map between healtcare schemas such as FHIR and OMOP. This tool reads whistle code and generates a graph representing the lineage of transformations applied to each field.

### example whistle code:

    x: foo()

    def foo() {
      if $Eq(0, 5) {
        y: "true value"
      } else {
        y: "false value"
      }
    }
    
### example lineage graph:

![Alt text](https://g.gravizo.com/source/custom_mark0?https%3A%2F%2Fraw.githubusercontent.com%2Fgoogleinterns%2Fhealthcare-data-harmonization-lineage%2Fprotobuf%2FREADME.md)
<details>
  <summary></summary>
  custom_mark0
digraph G {
	graph [bb="0,0,226,108"];
	node [label="\N"];
	4	 [height=0.5,
		label=C,
		pos="27,90",
		width=0.75];
	5	 [height=0.5,
		label=true,
		pos="27,18",
		width=0.75];
	4 -> 5	 [pos="e,27,36.413 27,71.831 27,64.131 27,54.974 27,46.417"];
	0	 [height=0.5,
		label=A,
		pos="113,90",
		width=0.75];
	1	 [height=0.5,
		label="\"string\"",
		pos="113,18",
		width=1.1364];
	0 -> 1	 [pos="e,113,36.413 113,71.831 113,64.131 113,54.974 113,46.417"];
	2	 [height=0.5,
		label=B,
		pos="199,90",
		width=0.75];
	3	 [height=0.5,
		label=3,
		pos="199,18",
		width=0.75];
	2 -> 3	 [pos="e,199,36.413 199,71.831 199,64.131 199,54.974 199,46.417"];
}
custom_mark0
</details>

## Installation

1. Make sure you have a golang development environment set up
2. Download and build the [healthcare-data-harmonization](https://github.com/GoogleCloudPlatform/healthcare-data-harmonization)
3. Download and build this repository using build.sh. You must provide the path to the healthcare-data-harmonization repository to the build script

## Use

Simply run the healthcare-data-harmonization-lineage executable
Flags:
* `--help`
  - display help on flags
* `-mapping_file_spec=[path/to/your/mapping.wstl]`
  - path and filename of the whistle code you want to generate a graph for
* `-dot_out=[path/to/your/dottext.dot]`
  - if provided, generates a [dot representation](https://en.wikipedia.org/wiki/DOT_(graph_description_language)) of the graph with the given path and file name
* `-png_out=[path/to/your/image.png]`
  - if provided, generates a png image of the graph with the given path and file name
* `-protobuf_out=[path/to/your/protobuf.pb.bin]`
  - if provided, generates a serialized protobuf representation of the graph with the given path and file name
* `-write_examples=[true|false]`
  - if provided, generates images and dot files for the whistle code in examples/. all other flags are ignored if this is activated.
