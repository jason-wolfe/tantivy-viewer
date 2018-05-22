extern crate byteorder;
extern crate clap;
extern crate fst;
extern crate serde_json;
extern crate tantivy;

use byteorder::{ReadBytesExt, LittleEndian};
use fst::Automaton;
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::path::Path;
use tantivy::Index;
use tantivy::Result;
use tantivy::schema::FieldType;
use tantivy::schema::Type;
use tantivy::termdict::TermStreamer;
use clap::App;
use clap::Arg;
use clap::SubCommand;
use tantivy::Term;
use tantivy::schema::Schema;
use tantivy::schema::Field;

fn main() {
    main_inner().unwrap();
}

fn main_inner() -> Result<()> {
    let matches = App::new("tantivy-viewer")
        .arg(Arg::with_name("index")
            .index(1)
            .takes_value(true)
            .required(true))
        .subcommand(SubCommand::with_name("fields"))
        .subcommand(SubCommand::with_name("topterms")
            .arg(Arg::with_name("field")
                .takes_value(true)
                .required(true)
                .index(1))
            .arg(Arg::with_name("k")
                .takes_value(true)
                .default_value("10")))
        .get_matches();
    let index_path = Path::new(matches.value_of("index").unwrap());
    let index = Index::open(index_path)?;
    match matches.subcommand() {
        ("fields", _) => {
            let fields = get_fields(&index)?;
            println!("{:#?}", fields);
        }
        ("topterms", Some(sub_args)) => {
            let field = sub_args.value_of("field").unwrap();
            let k = sub_args.value_of("k").unwrap().parse::<usize>().expect("invalid 'k' value provided.");
            let top_terms = top_terms(&index, field.to_string(), k)?;
            for term in top_terms.terms.into_iter() {
                eprintln!("term = {:?}", term);
            }
        }
        (ref command, _) => {
            println!("Unknown sub-command!: {}", command);
        }
    }
    Ok(())
}

#[derive(Debug)]
struct Fields {
    fields: HashMap<String, FieldDescriptor>,
}

#[derive(Debug)]
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

#[derive(Debug)]
struct TopTerms {
    terms: Vec<TermCount>,
}

#[derive(Eq, PartialEq, Ord, PartialOrd, Debug)]
struct TermCount {
    count: i64,
    term: TantivyValue,
}

#[derive(Eq, PartialEq, PartialOrd, Ord, Debug)]
enum TantivyValue {
    I64(i64),
    U64(u64),
    Text(String),
}

impl TantivyValue {
    fn from_term(key: &[u8], ty: Type) -> TantivyValue {
        let term = Term::from_field_text(Field(0), unsafe { std::str::from_utf8_unchecked(key) });
        match ty {
            Type::Str => TantivyValue::Text(term.text().to_string()),
            Type::U64 => TantivyValue::U64(term.get_u64()),
            Type::I64 => TantivyValue::I64(term.get_i64()),
            Type::HierarchicalFacet => unimplemented!(),
            Type::Bytes => unimplemented!(),
        }
    }
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
    let value_type = index.schema().get_field_entry(field).field_type().value_type();
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
            pq.push(TermCount { count: -count, term: TantivyValue::from_term(&current_key[..], value_type) });
        } else if pq.peek().unwrap().count > -count {
            *pq.peek_mut().unwrap() = TermCount { count: -count, term: TantivyValue::from_term(&current_key[..], value_type) };
        }
    }

    let mut vec = Vec::new();
    while let Some(mut termcount) = pq.pop() {
        termcount.count = -termcount.count;
        vec.push(termcount);
    }
    vec.reverse();

    Ok(TopTerms {
        terms: vec,
    })
}