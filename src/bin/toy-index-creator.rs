#[macro_use]
extern crate tantivy;

use tantivy::Result;
use tantivy::Index;
use tantivy::schema::*;
use std::env;
use std::path::Path;

fn main() {
    let args = env::args().collect::<Vec<_>>();
    main_inner(&Path::new(&args[1])).unwrap()
}

fn main_inner(output_path: &Path) -> Result<()> {
    let mut schema_builder = SchemaBuilder::default();
    let id = schema_builder.add_i64_field("id", IntOptions::default().set_fast(Cardinality::SingleValue));
    let body = schema_builder.add_text_field("body", TEXT);

    let schema = schema_builder.build();
    let index = Index::create(output_path, schema.clone())?;

    {
        let mut index_writer = index.writer(50_000_000)?;
        index_writer.add_document(doc!(
            id => 1i64,
            body => "A B C"
        ));
        index_writer.add_document(doc!(
            id => 2i64,
            body => "B C"
        ));
        index_writer.add_document(doc!(
            id => 3i64,
            body => "C"
        ));
        index_writer.commit()?;
    }

    {
        let mut index_writer = index.writer(50_000_000)?;
        index_writer.add_document(doc!(
            id => 4i64,
            body => "B C"
        ));
        index_writer.add_document(doc!(
            id => 5i64,
            body => "A B C"
        ));
        index_writer.add_document(doc!(
            id => 6i64,
            body => "C"
        ));
        index_writer.commit()?;
    }

    Ok(())
}