#![feature(transpose_result)]

extern crate actix_web;
extern crate cookie;
extern crate downcast;
extern crate env_logger;
extern crate failure;
#[macro_use]
extern crate failure_derive;
extern crate fst;
extern crate handlebars;
extern crate itertools;
#[macro_use]
extern crate log;
extern crate pretty_bytes;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate tantivy;
extern crate url;

mod debug;
mod fields;
mod reconstruct;
mod space_usage;
mod top_terms;

use actix_web::App;
use failure::Error;
use tantivy::Index;
use std::env;
use std::sync::Arc;
use tantivy::DocId;
use actix_web::HttpRequest;
use actix_web::server;
use handlebars::Handlebars;
use std::fs;
use serde::Serialize;
use actix_web::HttpResponse;
use actix_web::Query;
use actix_web::http;
use pretty_bytes::converter::convert;
use handlebars::Helper;
use handlebars::RenderContext;
use handlebars::RenderError;
use tantivy::query::QueryParser;
use tantivy::SegmentId;
use tantivy::collector::Collector;
use tantivy::SegmentReader;
use std::collections::HashSet;
use itertools::Itertools;
use std::collections::HashMap;
use tantivy::schema::Schema;
use tantivy::query::BooleanQuery;
use tantivy::query::Occur;
use tantivy::query::TermQuery;
use tantivy::Term;
use tantivy::schema::Type;
use tantivy::query::PhraseQuery;
use tantivy::query::RangeQuery;
use url::form_urlencoded;

use fields::get_fields;
use space_usage::space_usage;
use top_terms::top_terms;
use top_terms::TantivyValue;
use reconstruct::reconstruct;
use tantivy::query::AllQuery;
use std::collections::Bound;
use tantivy::Searcher;
use tantivy::SegmentLocalId;
use tantivy::query::Scorer;
use tantivy::fastfield::DeleteBitSet;
use reconstruct::handle_reconstruct;
use debug::handle_debug;

#[derive(Fail, Debug)]
enum TantivyViewerError {
    #[fail(display="An error occurred in tantivy")]
    TantivyError(tantivy::Error),
    #[fail(display="Query parsing error occurred")]
    QueryParserError(tantivy::query::QueryParserError),
    #[fail(display="Error encountered while rendering page")]
    RenderingError(handlebars::RenderError),
    #[fail(display="Error encountered serializing json")]
    JsonSerializationError,
    #[fail(display="Could not find a segment with the given prefix")]
    SegmentNotFoundError,
    #[fail(display="Could not break down unknown query type")]
    UnknownQueryTypeError,
}

impl actix_web::error::ResponseError for TantivyViewerError {
    fn error_response(&self) -> HttpResponse {
        use TantivyViewerError::*;
        let status = match *self {
            TantivyError(_)
            | RenderingError(_)
            | JsonSerializationError
            | UnknownQueryTypeError => http::StatusCode::INTERNAL_SERVER_ERROR,
            QueryParserError(_)
            | SegmentNotFoundError => http::StatusCode::BAD_REQUEST,
        };

        HttpResponse::Ok()
            .status(status)
            .body(format!("{}", self))
    }
}

impl From<tantivy::Error> for TantivyViewerError {
    fn from(e: tantivy::Error) -> Self {
        TantivyViewerError::TantivyError(e)
    }
}

impl From<UnknownQueryTypeError> for TantivyViewerError {
    fn from(_: UnknownQueryTypeError) -> Self {
        TantivyViewerError::UnknownQueryTypeError
    }
}

#[derive(Serialize)]
struct FieldData {
    name: String,
}

#[derive(Serialize)]
struct IndexData {
    fields: Vec<FieldData>,
    segments: Vec<String>,
    num_fields: usize,
    total_usage: usize,
}

fn handle_index(req: HttpRequest<State>) -> Result<HttpResponse, TantivyViewerError> {
    let state = req.state();
    let index = &state.index;
    let searcher = index.searcher();
    let space_usage = searcher.space_usage();
    let segments = index.searchable_segment_ids().map_err(TantivyViewerError::TantivyError)?;
    let fields: Vec<_> = index.schema().fields().iter().map(|x| FieldData { name: x.name().to_string() }).collect();
    let num_fields = fields.len();
    let data = IndexData {
        fields,
        segments: segments.into_iter().map(|x| x.short_uuid_string()).collect(),
        num_fields,
        total_usage: space_usage.total(),
    };

    state.render_template("index", &data)
}

#[derive(Debug, Serialize)]
struct FieldDetail {
    name: String,
    value_type: String,
    extra_options: String,
}

fn handle_field_details(req: HttpRequest<State>) -> Result<HttpResponse, TantivyViewerError> {
    let state = req.state();
    let fields = get_fields(&state.index)
        .map_err(TantivyViewerError::TantivyError)?;

    let mut field_details = fields
        .fields
        .into_iter()
        .map(|(_k, v)| Ok(FieldDetail {
            name: v.name,
            value_type: format!("{:?}", v.value_type),
            extra_options: serde_json::to_string(&v.extra_options)?
        }))
        .collect::<Result<Vec<_>, std::io::Error>>()
        .map_err(|_e| TantivyViewerError::JsonSerializationError)?;

    field_details.sort_unstable_by_key(|x| x.name.clone());

    state.render_template("field_details", &field_details)
}

fn handle_space_usage(req: HttpRequest<State>) -> Result<HttpResponse, TantivyViewerError> {
    let state = req.state();
    let space_usage = space_usage(&state.index);
    state.render_template("space_usage", &space_usage)
}

fn get_identifying_fields<S>(req: &HttpRequest<S>) -> Vec<String> {
    let mut cookie_fields = Vec::new();

    if let Ok(cookies) = req.cookies() {
        for cookie in cookies {
            if cookie.name() == "selected" {
                for value in cookie.value().split(",") {
                    cookie_fields.push(value.to_string());
                }
            }
        }
    }

    cookie_fields
}

#[derive(Serialize)]
struct ConfigurationField {
    field: String,
    selected: bool,
}

fn handle_configure(req: HttpRequest<State>) -> Result<HttpResponse, TantivyViewerError> {
    let state = req.state();
    let fields = get_fields(&state.index)
        .map_err(TantivyViewerError::TantivyError)?;

    let cookie_fields = get_identifying_fields(&req).into_iter().collect::<HashSet<String>>();

    let fields = fields.fields
        .into_iter()
        .map(|(field,_v)| {
            let selected = cookie_fields.contains(&field);
            ConfigurationField {
                field,
                selected,
            }
        })
        .sorted_by(|x, y| {
            x.selected.cmp(&y.selected).reverse().then_with(|| x.field.cmp(&y.field))
        });

    state.render_template("configure", &fields)
}

#[derive(Deserialize)]
struct TopTermsQuery {
    field: String,
    k: Option<usize>,
}

#[derive(Serialize)]
struct TermCountData {
    term: String,
    count: i64,
}

#[derive(Serialize)]
struct TopTermsData {
    field: String,
    terms: Vec<TermCountData>,
}

fn handle_top_terms(req: (HttpRequest<State>, Query<TopTermsQuery>)) -> Result<HttpResponse, TantivyViewerError>  {
    let (req, params) = req;
    let state = req.state();
    let field = params.field.clone();
    let k = params.k.unwrap_or(100);
    let top_terms = top_terms(&state.index, &field, k).unwrap();
    let data = TopTermsData {
        field,
        terms: top_terms.terms.into_iter().map(|x| TermCountData {
            term: format!("{}", x.term),
            count: x.count
        }).collect()
    };
    state.render_template("top_terms", &data)
}

fn stringify_values(values: Vec<Option<TantivyValue>>) -> String {
    values.into_iter()
        .map(|opt| opt.map(|x| format!("{} ", x)).unwrap_or_default())
        .collect()
}

#[derive(Deserialize)]
struct SearchQuery {
    query: Option<String>,
}

struct DocCollector {
    current_segment: Option<SegmentId>,
    current_segment_docs: Vec<DocId>,
    docs: Vec<(SegmentId, Vec<DocId>)>,
}

impl DocCollector {
    fn new() -> DocCollector {
        DocCollector {
            current_segment: None,
            current_segment_docs: Vec::new(),
            docs: Vec::new(),
        }
    }

    fn finish_segment(&mut self) {
        if let Some(segment_id) = self.current_segment {
            let mut docs = Vec::new();
            std::mem::swap(&mut self.current_segment_docs, &mut docs);
            self.docs.push((segment_id, docs));
        }
    }

    fn into_docs(mut self) -> Vec<(SegmentId, Vec<DocId>)> {
        self.finish_segment();
        self.docs
    }
}

impl Collector for DocCollector {
    fn set_segment(&mut self, _segment_local_id: u32, segment: &SegmentReader) -> Result<(), tantivy::Error> {
        self.finish_segment();
        self.current_segment = Some(segment.segment_id());
        Ok(())
    }

    fn collect(&mut self, doc: u32, _score: f32) {
        self.current_segment_docs.push(doc);
    }

    fn requires_scoring(&self) -> bool {
        false
    }
}

#[derive(Serialize)]
struct SearchData {
    query: String,
    reconstructed_fields: Vec<String>,
    docs: Vec<(String, Vec<(DocId, Vec<String>)>)>,
    truncated: bool,
}

impl SearchData {
    fn empty() -> SearchData {
        SearchData {
            query: String::new(),
            reconstructed_fields: Vec::new(),
            docs: Vec::new(),
            truncated: false,
        }
    }
}

trait QueryExt {
    fn collect_first_k(&self, searcher: &Searcher, collector: &mut Collector, k: usize) -> tantivy::Result<()>;
}

impl<Q: tantivy::query::Query> QueryExt for Q {
    fn collect_first_k(&self, searcher: &Searcher, collector: &mut Collector, k: usize) -> tantivy::Result<()> {
        let scoring_enabled = collector.requires_scoring();
        let weight = self.weight(searcher, scoring_enabled)?;
        let mut remaining = k;
        for (segment_ord, segment_reader) in searcher.segment_readers().iter().enumerate() {
            collector.set_segment(segment_ord as SegmentLocalId, segment_reader)?;
            let mut scorer = weight.scorer(segment_reader)?;
            remaining -= segment_collect_first_k(&mut scorer, collector, segment_reader.delete_bitset(), remaining);
        }
        Ok(())
    }
}

trait CollectorExt : Collector + Sized {
    fn collect_first_k(&mut self, searcher: &Searcher, query: &tantivy::query::Query, k: usize) -> tantivy::Result<()> {
        let scoring_enabled = self.requires_scoring();
        let weight = query.weight(searcher, scoring_enabled)?;
        let mut remaining = k;
        for (segment_ord, segment_reader) in searcher.segment_readers().iter().enumerate() {
            self.set_segment(segment_ord as SegmentLocalId, segment_reader)?;
            let mut scorer = weight.scorer(segment_reader)?;
            remaining -= segment_collect_first_k(&mut scorer, &mut *self, segment_reader.delete_bitset(), remaining);
        }
        Ok(())
    }
}

impl<C: Collector + Sized> CollectorExt for C {

}

fn segment_collect_first_k<S: Scorer>(scorer: &mut S, collector: &mut Collector, delete_bitset_opt: Option<&DeleteBitSet>, k: usize) -> usize {
    let mut remaining = k;
    if let Some(delete_bitset) = delete_bitset_opt {
        while remaining > 0 && scorer.advance() {
            let doc = scorer.doc();
            if !delete_bitset.is_deleted(doc) {
                remaining -= 1;
                collector.collect(doc, scorer.score());
            }
        }
    } else {
        while remaining > 0 && scorer.advance() {
            remaining -= 1;
            collector.collect(scorer.doc(), scorer.score());
        }
    }
    remaining
}

fn handle_search(req: (HttpRequest<State>, Query<SearchQuery>)) -> Result<HttpResponse, Error> {
    let (req, params) = req;
    let state = req.state();
    let raw_query = match params.query {
        None => return Ok(state.render_template("search", &SearchData::empty())?),
        Some(ref query) => query.clone(),
    };

    let limit = 1000;

    let query_parser = QueryParser::for_index(&state.index, vec![]);
    let query = query_parser.parse_query(&raw_query).map_err(TantivyViewerError::QueryParserError)?;

    let searcher = state.index.searcher();
    let mut collector = DocCollector::new();
    collector.collect_first_k(&*searcher, &*query, limit + 1).map_err(TantivyViewerError::TantivyError)?;

    let docs = collector.into_docs();

    let identifying_fields = get_identifying_fields(&req);

    let mut remaining = limit;
    let mut docs_to_reconstruct = HashMap::new();
    let mut truncated = false;
    for (segment, docs) in docs.into_iter() {
        let num_docs = docs.len();
        if remaining == 0 {
            break;
        }

        let take = remaining.min(docs.len());
        if take > 0 {
            docs_to_reconstruct.insert(segment, docs.into_iter().take(take).collect());
        }
        if take < num_docs {
            truncated = true;
        }
        remaining -= take;
    }

    let mut reconstructed_fields = Vec::new();
    for field in identifying_fields.iter() {
        let reconstructed = reconstruct(&state.index, &*field, &docs_to_reconstruct)?;
        let reconstructed = reconstructed
            .into_iter()
            .map(|(segment, docs)| {
                (segment, docs.into_iter().map(|(doc, values)| (doc, stringify_values(values))).collect::<Vec<_>>())
            })
            .collect::<HashMap<_, _>>();
        reconstructed_fields.push(reconstructed);
    }

    let mut result = Vec::new();
    for (segment, docs) in docs_to_reconstruct.into_iter() {
        let mut segment_docs = Vec::new();
        for (idx, doc) in docs.into_iter().enumerate() {
            let mut doc_reconstructed_fields = Vec::new();
            for field in reconstructed_fields.iter_mut() {
                let mut str_swap = String::new();
                std::mem::swap(&mut str_swap, &mut field.get_mut(&segment).unwrap().get_mut(idx).unwrap().1);
                doc_reconstructed_fields.push(str_swap);
            }
            segment_docs.push((doc, doc_reconstructed_fields));
        }
        result.push((segment.short_uuid_string(), segment_docs));
    }


    let data = SearchData {
        query: raw_query,
        reconstructed_fields: identifying_fields,
        docs: result,
        truncated,
    };

    Ok(state.render_template("search", &data)?)
}

struct State {
    index: Arc<Index>,
    handlebars: Arc<Handlebars>,
}

impl Clone for State {
    fn clone(&self) -> Self {
        State {
            index: self.index.clone(),
            handlebars: self.handlebars.clone(),
        }
    }
}

impl State {
    fn render_template<T: Serialize>(&self, name: &str, data: &T) -> Result<HttpResponse, TantivyViewerError> {
        Ok(
            HttpResponse::Ok()
            .content_type("text/html")
            .body(self.handlebars.render(name, &data).map_err(TantivyViewerError::RenderingError)?)
        )
    }
}

fn pretty_bytes(h: &Helper, _: &Handlebars, rc: &mut RenderContext) -> Result<(), RenderError> {
    if let Some(param) = h.param(0) {
        if let Some(param) = param.value().as_f64() {
            rc.writer.write(convert(param).as_bytes())?;
            return Ok(());
        }
    }
    rc.writer.write("<invalid argument>".as_bytes())?;
    Ok(())
}

fn url_encode(h: &Helper, _: &Handlebars, rc: &mut RenderContext) -> Result<(), RenderError> {
    if let Some(param) = h.param(0) {
        if let Some(param) = param.value().as_str() {
            let encoded: String = form_urlencoded::byte_serialize(param.as_bytes()).collect();
            rc.writer.write(encoded.as_bytes())?;
            return Ok(());
        }
    }
    Err(RenderError::new("Invalid argument to url_encode. Expected string."))
}

struct UnknownQueryTypeError;
fn child_queries(query: &tantivy::query::Query) -> Result<Vec<Box<tantivy::query::Query>>, UnknownQueryTypeError> {
    let mut result = Vec::new();
    if let Ok(ref query) = query.downcast_ref::<BooleanQuery>() {
        for (_occur, clause) in query.clauses() {
            result.push(clause.box_clone());
        }
    } else if let Ok(_query) = query.downcast_ref::<TermQuery>() {
    } else if let Ok(_query) = query.downcast_ref::<PhraseQuery>() {
    } else if let Ok(_query) = query.downcast_ref::<RangeQuery>() {
    } else if let Ok(_query) = query.downcast_ref::<AllQuery>() {
    } else {
        return Err(UnknownQueryTypeError);
    }
    Ok(result)
}

fn push_term_str(term: &Term, value_type: &Type, allow_quoting: bool, output: &mut String) {
    match *value_type {
        Type::Str => {
            let term_text = term.text();
            if allow_quoting && term_text.contains(' ') {
                output.push('"');
                output.push_str(term_text);
                output.push('"');
            } else {
                output.push_str(term_text)
            }
        },
        Type::U64 => output.push_str(&format!("{}", term.get_u64())),
        Type::I64 => output.push_str(&format!("{}", term.get_i64())),
        Type::HierarchicalFacet => output.push_str("<cannot write HierarchicalFacet>"),
        Type::Bytes => output.push_str("<cannot search for bytes>"),
    }
}

fn query_to_string(query: &tantivy::query::Query, schema: &Schema) -> String {
    let mut output = String::new();
    push_query_to_string(query, schema, &mut output);
    output
}

fn push_query_to_string(query: &tantivy::query::Query, schema: &Schema, output: &mut String) {
    if let Ok(ref query) = query.downcast_ref::<BooleanQuery>() {
        let was_empty = output.is_empty();
        if !was_empty {
            output.push('(');
        }
        for (idx, (occur, clause)) in query.clauses().iter().enumerate() {
            if idx != 0 {
                output.push(' ');
            }
            let prefix = match occur {
                Occur::Should => "",
                Occur::Must => "+",
                Occur::MustNot => "-",
            };
            output.push_str(prefix);
            push_query_to_string(clause.as_ref(), schema, output);
        }
        if !was_empty {
            output.push(')');
        }
    } else if let Ok(ref query) = query.downcast_ref::<TermQuery>() {
        let term = query.term();
        let field_obj = query.term().field();
        let field = schema.get_field_name(field_obj);
        let value_type = schema.get_field_entry(field_obj).field_type().value_type();
        output.push_str(field);
        output.push(':');
        push_term_str(term, &value_type, true, output);
    } else if let Ok(ref query) = query.downcast_ref::<PhraseQuery>() {
        let field = schema.get_field_name(query.field());
        let value_type = schema.get_field_entry(query.field()).field_type().value_type();
        let terms = query.phrase_terms();
        output.push_str(field);
        output.push_str(":\"");
        for (idx, term) in terms.iter().enumerate() {
            if idx != 0 {
                output.push(' ');
            }
            push_term_str(term, &value_type, false, output);
        }
        output.push('"');
    } else if let Ok(query) = query.downcast_ref::<RangeQuery>() {
        let field = schema.get_field_name(query.field());
        let value_type = schema.get_field_entry(query.field()).field_type().value_type();
        output.push_str(field);
        output.push(':');
        match query.left_bound() {
            Bound::Included(term) => {
                output.push('[');
                push_term_str(&term, &value_type, false, output);
            },
            Bound::Excluded(term) => {
                output.push('{');
                push_term_str(&term, &value_type, false, output);
            },
            Bound::Unbounded => output.push('['),
        }
        output.push_str(" TO ");
        match query.right_bound() {
            Bound::Included(term) => {
                push_term_str(&term, &value_type, false, output);
                output.push(']');
            },
            Bound::Excluded(term) => {
                push_term_str(&term, &value_type, false, output);
                output.push('}');
            },
            Bound::Unbounded => output.push(']'),
        }
    } else if let Ok(_query) = query.downcast_ref::<AllQuery>() {
        output.push_str("*");
    } else {
        output.push_str(&format!("<unknown query type {:?}>", query));
    }
}

fn main() -> Result<(), Error> {
    env_logger::init();

    let args = env::args().collect::<Vec<_>>();
    let index = Arc::new(Index::open_in_dir(&args[1]).unwrap());

    let mut handlebars = Handlebars::new();
    handlebars.register_helper("pretty_bytes", Box::new(pretty_bytes));
    handlebars.register_helper("url_encode", Box::new(url_encode));
    for entry in fs::read_dir("./templates")? {
        let entry = entry?;
        let filename = entry.file_name();
        let filename_string = filename.to_string_lossy();
        if filename_string.ends_with(".hbs") {
            let template_name = &filename_string[..filename_string.len() - ".hbs".len()];
            debug!("Registering template {}", template_name);
            handlebars.register_template_file(template_name, entry.path())?;
        }
    }

    let state = State {
        index: index.clone(),
        handlebars: Arc::new(handlebars),
    };
    server::new(move ||
        App::with_state(state.clone())
            .resource("/", |r| r.f(handle_index))
            .resource("/field_details", |r| r.f(handle_field_details))
            .resource("/space_usage", |r| r.f(handle_space_usage))
            .resource("/configure", |r| r.f(handle_configure))
            .resource("/top_terms", |r| r.method(http::Method::GET).with(handle_top_terms))
            .resource("/reconstruct", |r| r.method(http::Method::GET).with(handle_reconstruct))
            .resource("/search", |r| r.method(http::Method::GET).with(handle_search))
            .resource("/debug", |r| r.method(http::Method::GET).with(handle_debug))
    ).bind("0.0.0.0:3000").unwrap().run();

    Ok(())
}