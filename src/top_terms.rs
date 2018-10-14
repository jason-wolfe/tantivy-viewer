use failure::err_msg;
use failure::Error;
use tantivy::schema::Type;
use tantivy::Term;
use tantivy::schema::Field;
use fst::Automaton;
use tantivy::termdict::TermStreamer;
use std::cmp::Ordering;
use std::fmt;
use std::str;
use tantivy::Index;
use std::collections::BinaryHeap;
use std::collections::binary_heap::PeekMut;

#[derive(Debug)]
pub struct TopTerms {
    pub terms: Vec<TermCount>,
}

#[derive(Eq, PartialEq, Ord, PartialOrd, Debug)]
pub struct TermCount {
    pub count: i64,
    pub term: TantivyValue,
}

#[derive(Eq, PartialEq, PartialOrd, Ord, Debug, Clone)]
pub enum TantivyValue {
    I64(i64),
    U64(u64),
    Text(String),
    Bytes(Vec<u8>),
}

impl From<i64> for TantivyValue {
    fn from(value: i64) -> Self {
        TantivyValue::I64(value)
    }
}

impl From<u64> for TantivyValue {
    fn from(value: u64) -> Self {
        TantivyValue::U64(value)
    }
}

impl From<String> for TantivyValue {
    fn from(value: String) -> Self {
        TantivyValue::Text(value)
    }
}

impl From<Vec<u8>> for TantivyValue {
    fn from(value: Vec<u8>) -> Self {
        TantivyValue::Bytes(value)
    }
}

impl fmt::Display for TantivyValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            TantivyValue::I64(val) => val.fmt(f),
            TantivyValue::U64(val) => val.fmt(f),
            TantivyValue::Text(ref val) => val.fmt(f),
            TantivyValue::Bytes(ref val) => write!(f, "[{} bytes]", val.len()),
        }
    }
}

impl TantivyValue {
    pub fn from_term(key: &[u8], ty: Type) -> TantivyValue {
        let term = Term::from_field_text(Field(0), unsafe { str::from_utf8_unchecked(key) });
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
        self.streamer.key().cmp(&other.streamer.key()).reverse()
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

pub fn top_terms(index: &Index, field: &str, k: usize) -> Result<TopTerms, Error> {
    let searcher = index.searcher();
    let field = index.schema().get_field(field).ok_or(err_msg("Sorry, that field does not exist!"))?;
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
