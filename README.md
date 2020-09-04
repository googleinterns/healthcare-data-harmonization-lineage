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

![rendered lineage graph](https://raw.githubusercontent.com/googleinterns/healthcare-data-harmonization-lineage/protobuf/examples/png/3.png)

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
