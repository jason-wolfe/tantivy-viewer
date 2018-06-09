**tantivy-viewer** is a tool for helping understand, investigate, 
and diagnose any sorts of issues you may encounter while working
with a [tantivy](https://github.com/tantivy-search/tantivy)
search index.

Its creation was inspired by using [luke](http://www.getopt.org/luke/)
to diagnose issues with Lucene indexes. 

WARNING: This is currently frozen to an unmerged tantivy branch 
with some experimental API changes. This situation will be
resolved in the near future by getting the API changed or by
moving back to a normal dependency.

# Features
- Web interface for convenient exploration
- Viewing the index schema in detail
- Exploring fields
    - Top terms per field
    - Which documents have a given term
    - Reconstructing particular documents from the index,
      either by uninverting or reading fast fields
- Command line interface for when a web interface is 
  inconvenient or impractical.
  
# Getting started

These instructions will get you a copy of the project up and 
running on your local machine.

## Prerequisites 

A tantivy index

## Installing

Clone this repo and build it

```
git clone git@github.com:jason-wolfe/tantivy-viewer.git
cd tantivy-viewer
cargo build --release 
```

In the future this will be published to crates.io and available
via `cargo install`.

## Running the Web Server

Run the `tantivy-viewer-web` executable with an argument pointing to 
your tantivy directory, and a server will be started on localhost:3000.

Point your browser to [http://localhost:3000/](http://localhost:3000/)
to start exploring! 

(TODO: Add a parameter for controlling the server port)

## Running the command line interface

Run the `tantivy-viewer` executable.

Thanks to [clap](https://crates.io/crates/clap) the program is somewhat self-documenting.

Running with the "help" subcommand describes all of the options.

Running with "help <subcommand>" will explain the usage of a particular command.

Here is a sample session on a toy index:

```
➜  ./target/debug/tantivy-viewer help   
tantivy-viewer 

USAGE:
    tantivy-viewer <index> [SUBCOMMAND]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

ARGS:
    <index>    

SUBCOMMANDS:
    fields         
    help           Prints this message or the help of the given subcommand(s)
    reconstruct    
    termdocs       
    topterms       

➜  ./target/debug/tantivy-viewer /tmp/my_index fields 
Fields {
    fields: {
        "id": FieldDescriptor {
            name: "id",
            value_type: I64,
            extra_options: Object(
                {
                    "fast": String(
                        "single"
                    ),
                    "indexed": Bool(
                        false
                    ),
                    "stored": Bool(
                        false
                    )
                }
            )
        },
        "body": FieldDescriptor {
            name: "body",
            value_type: Str,
            extra_options: Object(
                {
                    "indexing": Object(
                        {
                            "record": String(
                                "position"
                            ),
                            "tokenizer": String(
                                "default"
                            )
                        }
                    ),
                    "stored": Bool(
                        false
                    )
                }
            )
        }
    }
}

➜  ./target/debug/tantivy-viewer /tmp/my_index topterms body 1
TermCount { count: 6, term: Text("c") }

➜  ./target/debug/tantivy-viewer /tmp/my_index termdocs body c
(1dbb5705, 0)
(334149b7, 0)
(5304ac6f, 0)
(56cbf23e, 0)
(98b35d2d, 0)
(dce0f804, 0)

➜  ./target/debug/tantivy-viewer /tmp/my_index reconstruct 1dbb 0 body
b c %    
```