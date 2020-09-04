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
digraph "" {
	graph [bb="0,0,484.47,448.4"];
	node [label="\N"];
	19	 [height=0.5,
		label="def $Not",
		pos="300,195.6",
		width=1.2526];
	20	 [height=0.5,
		label="def $Eq",
		pos="300,106.8",
		width=1.1483];
	19 -> 20	 [label=arg,
		lp="308.94,151.2",
		pos="e,300,124.87 300,177.2 300,165.09 300,149.01 300,135.27",
		style=dashed];
	21	 [height=0.5,
		label=0,
		pos="264,18",
		width=0.75];
	20 -> 21	 [label=arg,
		lp="293.94,62.4",
		pos="e,271.15,35.64 292.71,88.83 287.61,76.23 280.69,59.165 274.92,44.943",
		style=dashed];
	22	 [height=0.5,
		label=5,
		pos="336,18",
		width=0.75];
	20 -> 22	 [label=arg,
		lp="329.94,62.4",
		pos="e,328.85,35.64 307.29,88.83 312.39,76.23 319.31,59.165 325.08,44.943",
		style=dashed];
	12	 [height=0.5,
		label="def foo",
		pos="239,357.4",
		width=1.0737];
	13	 [height=0.5,
		label=y,
		pos="180,284.4",
		width=0.75];
	12 -> 13	 [pos="e,193,300.49 225.32,340.47 217.63,330.96 207.92,318.94 199.46,308.47"];
	18	 [height=0.5,
		label=y,
		pos="300,284.4",
		width=0.75];
	12 -> 18	 [pos="e,286.76,300.24 253.15,340.47 261.23,330.8 271.47,318.54 280.33,307.94"];
	14	 [height=0.5,
		label="def $Eq",
		pos="63,195.6",
		width=1.1483];
	13 -> 14	 [label=cond,
		lp="145.61,240",
		pos="e,83.609,211.24 161.98,270.72 143.22,256.48 113.57,233.98 91.611,217.32",
		style=dotted];
	17	 [height=0.5,
		label="\"true value\"",
		pos="180,195.6",
		width=1.5905];
	13 -> 17	 [pos="e,180,213.67 180,266 180,253.89 180,237.81 180,224.07"];
	16	 [height=0.5,
		label=5,
		pos="27,106.8",
		width=0.75];
	14 -> 16	 [label=arg,
		lp="57.938,151.2",
		pos="e,34.151,124.44 55.715,177.63 50.607,165.03 43.689,147.97 37.923,133.74",
		style=dashed];
	15	 [height=0.5,
		label=0,
		pos="99,106.8",
		width=0.75];
	14 -> 15	 [label=arg,
		lp="93.938,151.2",
		pos="e,91.849,124.44 70.285,177.63 75.393,165.03 82.311,147.97 88.077,133.74",
		style=dashed];
	11	 [height=0.5,
		label=x,
		pos="239,430.4",
		width=0.75];
	11 -> 12	 [pos="e,239,375.49 239,412.36 239,404.28 239,394.58 239,385.58"];
	18 -> 19	 [label=cond,
		lp="313.61,240",
		pos="e,300,213.67 300,266 300,253.89 300,237.81 300,224.07",
		style=dotted];
	23	 [height=0.5,
		label="\"false value\"",
		pos="424,195.6",
		width=1.6797];
	18 -> 23	 [pos="e,400.65,212.32 318.56,271.11 338.06,257.14 369.07,234.94 392.42,218.21"];
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
