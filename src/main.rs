extern crate fst;
#[macro_use]
extern crate serde;
extern crate serde_json;
extern crate tantivy;

use fst::Automaton;
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::env;
use std::path::Path;
use tantivy::Index;
use tantivy::Result;
use tantivy::schema::FieldType;
use tantivy::schema::Type;
use tantivy::termdict::TermStreamer;

fn main() {
    let args = env::args().collect::<Vec<_>>();
    let index_path = Path::new(&args[1]);
    main_inner(&index_path).unwrap();
}

fn main_inner(index_path: &Path) -> Result<()> {
    let index = Index::open(index_path)?;
    Ok(())
}

struct Fields {
    fields: HashMap<String, FieldDescriptor>,
}

struct FieldDescriptor {
    name: String,
    value_type: Type,
    extra_options: Value,
}

fn field_options(field_type: &FieldType) -> Result<Value> {
    Ok(match *field_type {
        FieldType::Str(ref options) => serde_json::to_value(options)?,
        FieldType::U64(ref options) => serde_json::to_value(options)?,
        FieldType::I64(ref options) => serde_json::to_value(options)?,
        FieldType::HierarchicalFacet => Value::Null,
        FieldType::Bytes => Value::Null,
    })
}

fn get_fields(index: &Index) -> Result<Fields> {
    let schema = index.schema();

    let mut fields = HashMap::new();

    for field in schema.fields() {
        let name = field.name().to_string();
        fields.insert(name.clone(), FieldDescriptor {
            name,
            value_type: field.field_type().value_type(),
            extra_options: field_options(field.field_type())?,
        });
    }

    Ok(Fields { fields })
}

struct TopTerms {
    terms: Vec<(usize, Vec<u8>)>,
}

struct StreamerWrapper<'a, A: Automaton> {
    streamer: TermStreamer<'a, A>,
}

impl<'a, A: Automaton> Ord for StreamerWrapper<'a, A> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.streamer.key().cmp(&other.streamer.key())
    }
}

impl<'a, A: Automaton> PartialOrd for StreamerWrapper<'a, A> {
    fn partial_cmp(&self, other: &StreamerWrapper<'a, A>) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a, A: Automaton> PartialEq for StreamerWrapper<'a, A> {
    fn eq(&self, other: &StreamerWrapper<'a, A>) -> bool {
        self.streamer.key() == other.streamer.key()
    }
}

impl<'a, A: Automaton> Eq for StreamerWrapper<'a, A> {}

fn top_terms(index: &Index, field: String, k: usize) -> Result<TopTerms> {
    let searcher = index.searcher();
    let field = index.schema().get_field(&field).ok_or("Sorry, that field does not exist!")?;
    let indexes = searcher.segment_readers().iter().map(|x| x.inverted_index(field)).collect::<Vec<_>>();

    let mut streams = indexes.iter().filter_map(|x| {
        let mut stream = x.terms().stream();
        if stream.advance() {
            Some(StreamerWrapper {
                streamer: stream,
            })
        } else {
            None
        }
    }).collect::<BinaryHeap<_>>();

    let mut pq = BinaryHeap::new();

    while !streams.is_empty() {
        let current_key = streams.peek().unwrap().streamer.key().to_owned();
        let mut count = 0;

        while let Some(mut head) = streams.peek_mut() {
            if head.streamer.key() == &current_key[..] {
                count += head.streamer.value().doc_freq as i64;
                if !head.streamer.advance() {
                    PeekMut::pop(head);
                }
            } else {
                break;
            }
        }

        if pq.len() < k {
            pq.push((-count, current_key));
        } else if pq.peek().unwrap().0 < -count {
            *pq.peek_mut().unwrap() = (-count, current_key);
        }
    }

    let mut vec = Vec::new();
    while let Some((neg_count, term)) = pq.pop() {
        vec.push(((-neg_count) as usize, term));
    }
    vec.reverse();

    Ok(TopTerms {
        terms: vec,
    })
}