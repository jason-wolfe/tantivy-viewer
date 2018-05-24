use tantivy::Index;
use tantivy::Result;
use tantivy::schema::IndexRecordOption;
use tantivy::DocSet;
use tantivy::DocId;
use tantivy::SkipResult;
use tantivy::Postings;
use top_terms::TantivyValue;

pub fn reconstruct(index: &Index, field: &str, target_segment: &str, doc: DocId) -> Result<Vec<Option<TantivyValue>>> {
    let schema = index.schema();
    let field = schema.get_field(field).ok_or("Field not found")?;
    let field_type = schema.get_field_entry(field).field_type();
    let value_type = field_type.value_type();
    let options = field_type.get_index_record_option().unwrap_or(IndexRecordOption::WithFreqsAndPositions);
    let searcher = index.searcher();

    let mut positions_buf = Vec::new();
    let mut reconstructed = Vec::new();

    let mut segment_found = false;
    for segment in searcher.segment_readers() {
        if !segment.segment_id().uuid_string().starts_with(target_segment) {
            continue;
        }
        segment_found = true;
        let index = segment.inverted_index(field);
        let mut term_stream = index.terms().stream();

        while term_stream.advance() {
            let mut segment_postings = index.read_postings_from_terminfo(term_stream.value(), options);
            if let SkipResult::Reached = segment_postings.skip_next(doc) {
                segment_postings.positions(&mut positions_buf);
                let value = TantivyValue::from_term(term_stream.key(), value_type);
                if let Some(last) = positions_buf.pop() {
                    if last as usize >= reconstructed.len() {
                        reconstructed.resize(last as usize + 1, None);
                    }
                    positions_buf.push(last);
                } else {
                    for _ in 0..segment_postings.term_freq() {
                        reconstructed.push(Some(value.clone()));
                    }
                    continue;
                }
                for position in positions_buf.drain(..) {
                    reconstructed[position as usize] = Some(value.clone());
                }
            }
        }
    }
    if ! segment_found {
        println!("No segment found starting with '{}'!)", target_segment);
    }
    Ok(reconstructed)
}
