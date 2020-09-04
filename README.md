# Data Harmonization Lineage

**This is not an officially supported Google product.**

## What is this?
The whistle mapping language is used to write mappings between healthcare schemas such as FHIR and OMOP. This tool analyzes whistle code and returns a graph representing the lineage of transformations applied to each field.

### whistle code:
   
    x: foo()
    
    def foo() {
      if $Eq(5, 0) {
        y: "true value"
      } else {
        y: "false value"
      }
    }
    
### rendered lineage graph:

![Alt text](https://g.gravizo.com/source/custom_mark0?https%3A%2F%2Fraw.githubusercontent.com%2Fgoogleinterns%2Fhealthcare-data-harmonization-lineage%2Fprotobuf%2FREADME.md)
<details> 
<summary></summary>
custom_mark0
  digraph G {
    size ="4,4";
    main [shape=box];
    main -> parse [weight=8];
    parse -> execute;
    main -> init [style=dotted];
    main -> cleanup;
    execute -> { make_string; printf};
    init -> make_string;
    edge [color=red];
    main -> printf [style=bold,label="100 times"];
    make_string [label="make a string"];
    node [shape=box,style=filled,color=".7 .3 1.0"];
    execute -> compare;
  }
custom_mark0
</details>


## Installation:

1. Make sure your system is set up for Golang development
1. Download and build the https://github.com/GoogleCloudPlatform/healthcare-data-harmonization repository
2. Download and build this repository with build.sh. You must provide the path to the healthcare-data-harmonization repository for it to work

## Use:

Build the program, then run it on the commandline with ./healthcare-data-harmonization-lineage.
Flags:
* `--help`
  - explains how to use the tool
* `-mapping_file_spec=[path/to/your/mapping.wstl]`
  - the path and file name to the whistle code you want to map
* `-png_out=[string]`
  - if provided, the graph will be rendered as a .png file with the given path and file name
* `-protobuf_out`
  - if provided, the graph will be marshalled as protobuf output with the given path and file name
* `-dot_out`
  - if provided, the graph's [dot representation](https://en.wikipedia.org/wiki/DOT_(graph_description_language) will be written with the given path and file name
* `-write_examples`
  - if provided, the examples found in the "examples" directory of this repository will be generated. all other flags are ignored if this is true
