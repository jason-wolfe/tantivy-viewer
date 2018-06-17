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
use std::collections::HashMap;
use tantivy::SegmentId;
use tantivy::postings::SegmentPostings;
use tantivy::schema::Type;

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

pub fn reconstruct_one(index: &Index, field: &str, segment: SegmentId, doc: DocId) -> Result<Vec<Option<TantivyValue>>> {
    let mut segment_to_doc = HashMap::new();
    segment_to_doc.insert(segment, vec![doc]);
    let mut result_map = reconstruct(index, field, &segment_to_doc)?;
    Ok(result_map.remove(&segment).unwrap().pop().unwrap().1)
}

pub fn reconstruct(index: &Index, field: &str, docs: &HashMap<SegmentId, Vec<DocId>>) -> Result<HashMap<SegmentId, Vec<(DocId, Vec<Option<TantivyValue>>)>>> {
    let schema = index.schema();
    let field = schema.get_field(field).ok_or("Field not found")?;
    let field_type = schema.get_field_entry(field).field_type();
    let value_type = field_type.value_type();
    let options = field_type.get_index_record_option().unwrap_or(IndexRecordOption::WithFreqsAndPositions);
    let searcher = index.searcher();

    let mut positions_buf = Vec::new();
    let mut reconstructed_docs = HashMap::new();

    for segment in searcher.segment_readers() {
        if let Some(segment_docs) = docs.get(&segment.segment_id()) {
            let mut segment_reconstructed_docs = segment_docs.iter()
                .map(|&doc| (doc, Vec::new()))
                .collect::<Vec<_>>();
            if let Some(_record_option) = field_type.get_index_record_option() {
                // Field is indexed
                let index = segment.inverted_index(field);
                let mut term_stream = index.terms().stream();

                while term_stream.advance() {
                    let mut segment_postings = index.read_postings_from_terminfo(term_stream.value(), options);

                    let mut current_doc = None;
                    let mut reached_end = false;

                    for (idx, &doc) in segment_docs.iter().enumerate() {
                        let mut reconstructed_doc = &mut segment_reconstructed_docs.get_mut(idx).unwrap().1;
                        let mut seek = !reached_end;
                        if let Some(current_doc) = current_doc {
                            if current_doc <= doc {
                                seek = false;
                            }
                            if current_doc == doc {
                                reconstruct_doc(&mut reconstructed_doc, &mut positions_buf, &mut segment_postings, term_stream.key(), value_type);
                            }
                        }
                        if seek {
                            match segment_postings.skip_next(doc) {
                                SkipResult::Reached => {
                                    reconstruct_doc(&mut reconstructed_doc, &mut positions_buf, &mut segment_postings, term_stream.key(), value_type);
                                    current_doc = Some(segment_postings.doc());
                                }
                                SkipResult::End => {
                                    reached_end = true;
                                    current_doc = None;
                                }
                                SkipResult::OverStep => {
                                    current_doc = Some(segment_postings.doc());
                                }
                            }
                        }
                    }
                }
            } else if field_type.is_fast() {
                for (idx, &doc) in segment_docs.iter().enumerate() {
                    let mut reconstructed_doc = &mut segment_reconstructed_docs.get_mut(idx).unwrap().1;
                    match *field_type {
                        FieldType::Str(_) => {},
                        FieldType::U64(ref opts) => reconstruct_numeric::<u64>(segment, doc, field, opts.get_fastfield_cardinality(), &mut reconstructed_doc)?,
                        FieldType::I64(ref opts) => reconstruct_numeric::<i64>(segment, doc, field, opts.get_fastfield_cardinality(), &mut reconstructed_doc)?,
                        FieldType::HierarchicalFacet => unimplemented!(),
                        FieldType::Bytes => {
                            let bytes_reader = segment.bytes_fast_field_reader(field)?;
                            let bytes = bytes_reader.get_val(doc).iter().cloned().collect::<Vec<_>>();
                            reconstructed_doc.push(Some(bytes.into()));
                        },
                    }
                }
            }
            reconstructed_docs.insert(segment.segment_id(), segment_reconstructed_docs);
        }
    }

    Ok(reconstructed_docs)
}

fn reconstruct_doc(output: &mut Vec<Option<TantivyValue>>, positions_buf: &mut Vec<u32>, postings: &mut SegmentPostings, term_bytes: &[u8], value_type: Type) {
    postings.positions(positions_buf);
    let value = TantivyValue::from_term(term_bytes, value_type);
    if let Some(last) = positions_buf.pop() {
        if last as usize >= output.len() {
            output.resize(last as usize + 1, None);
        }
        positions_buf.push(last);
    } else {
        for _ in 0..postings.term_freq() {
            output.push(Some(value.clone()));
        }
        return;
    }
    for position in positions_buf.drain(..) {
        output[position as usize] = Some(value.clone());
    }
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
