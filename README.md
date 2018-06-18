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
    - Reconstructing particular documents from the index,
      either by uninverting or reading fast fields
- Searching the index
    - Reconstructed identifying fields alongside search results for readability
  
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

Run the `tantivy-viewer` executable with an argument pointing to 
your tantivy directory, and a server will be started on localhost:3000.

```
➜  ./target/debug/tantivy-viewer /tmp/my_index
```

Point your browser to [http://localhost:3000/](http://localhost:3000/)
to start exploring! 

(TODO: Add a parameter for controlling the server port)