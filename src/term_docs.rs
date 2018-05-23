use tantivy::Index;
use tantivy::Result;
use tantivy::schema::Type;
use tantivy::Term;
use tantivy::schema::IndexRecordOption;
use tantivy::SegmentId;
use tantivy::DocSet;
use tantivy::DocId;

pub fn term_docs(index: &Index, field: &str, term_str: &str) -> Result<Vec<(SegmentId, DocId)>> {
    let schema = index.schema();
    let field = schema.get_field(field).ok_or("Field not found")?;
    let field_type = schema.get_field_entry(field).field_type().value_type();

    let term = match field_type {
        Type::Str => Term::from_field_text(field, term_str),
        Type::U64 => Term::from_field_u64(field, term_str.parse::<u64>().map_err(|_| "invalid u64 value")?),
        Type::I64 => Term::from_field_i64(field, term_str.parse::<i64>().map_err(|_| "invalid i64 value")?),
        Type::HierarchicalFacet => unimplemented!(),
        Type::Bytes => unimplemented!(),
    };

    let searcher = index.searcher();
    let mut result = Vec::new();
    for segment in searcher.segment_readers() {
        let index = segment.inverted_index(field);
        let segment_id = segment.segment_id();
        if let Some(mut postings) = index.read_postings(&term, IndexRecordOption::Basic) {
            while postings.advance() {
                result.push((segment_id, postings.doc()));
            }
        }
    }

    Ok(result)
}
