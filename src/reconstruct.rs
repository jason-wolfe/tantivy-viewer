use tantivy::Index;
use tantivy::Result;
use tantivy::schema::IndexRecordOption;
use tantivy::DocSet;
use tantivy::DocId;
use tantivy::SkipResult;
use tantivy::Postings;
use top_terms::TantivyValue;
use tantivy::schema::FieldType;
use tantivy::schema::Field;
use tantivy::SegmentReader;
use tantivy::fastfield::FastValue;
use tantivy::schema::Cardinality;

trait FieldTypeExt {
    fn is_fast(&self) -> bool;
}

impl FieldTypeExt for FieldType {
    fn is_fast(&self) -> bool {
        match *self {
            FieldType::Str(_) => false,
            FieldType::U64(ref opts) => opts.is_fast(),
            FieldType::I64(ref opts) => opts.is_fast(),
            FieldType::HierarchicalFacet => false, // TODO: What do we do with these?
            FieldType::Bytes => true,
        }
    }
}

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

        if let Some(_record_option) = field_type.get_index_record_option() {
            // Field is indexed
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
        } else if field_type.is_fast() {
            match *field_type {
                FieldType::Str(_) => {},
                FieldType::U64(ref opts) => reconstruct_numeric::<u64>(segment, doc, field, opts.get_fastfield_cardinality(), &mut reconstructed)?,
                FieldType::I64(ref opts) => reconstruct_numeric::<i64>(segment, doc, field, opts.get_fastfield_cardinality(), &mut reconstructed)?,
                FieldType::HierarchicalFacet => unimplemented!(),
                FieldType::Bytes => {
                    let bytes_reader = segment.bytes_fast_field_reader(field)?;
                    let bytes = bytes_reader.get_val(doc).iter().cloned().collect::<Vec<_>>();
                    reconstructed.push(Some(bytes.into()));
                },
            }
        }
    }
    if ! segment_found {
        println!("No segment found starting with '{}'!)", target_segment);
    }
    Ok(reconstructed)
}

fn reconstruct_numeric<T: FastValue + Into<TantivyValue>>(segment: &SegmentReader, doc: DocId, field: Field, cardinality: Option<Cardinality>, output: &mut Vec<Option<TantivyValue>>) -> Result<()> {
    match cardinality {
        Some(Cardinality::SingleValue) => {
            let reader = segment.fast_field_reader::<T>(field)?;
            output.push(Some(reader.get(doc).into()));
        }
        Some(Cardinality::MultiValues) => {
            let reader = segment.multi_fast_field_reader::<T>(field)?;
            let mut tmp = Vec::new();
            reader.get_vals(doc, &mut tmp);
            output.extend(tmp.into_iter().map(|x| Some(x.into())));
        }
        None => {
            panic!("Reconstructing numeric on non-fast field!")
        }
    }

    Ok(())
}
