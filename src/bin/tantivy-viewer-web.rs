#![feature(transpose_result)]

extern crate actix_web;
extern crate cookie;
extern crate env_logger;
extern crate failure;
#[macro_use]
extern crate failure_derive;
extern crate handlebars;
extern crate itertools;
#[macro_use]
extern crate log;
extern crate pretty_bytes;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate tantivy;
extern crate tantivy_viewer;
extern crate serde;

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
use tantivy_viewer::TantivyValue;

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
}

impl actix_web::error::ResponseError for TantivyViewerError {
    fn error_response(&self) -> HttpResponse {
        use TantivyViewerError::*;
        let status = match *self {
            TantivyError(_)
            | RenderingError(_)
            | JsonSerializationError => http::StatusCode::INTERNAL_SERVER_ERROR,
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
        total_usage: space_usage.total().0,
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
    let fields = tantivy_viewer::get_fields(&state.index)
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
    let space_usage = tantivy_viewer::space_usage(&state.index);
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
    let fields = tantivy_viewer::get_fields(&state.index)
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
    let top_terms = tantivy_viewer::top_terms(&state.index, &field, k).unwrap();
    let data = TopTermsData {
        field,
        terms: top_terms.terms.into_iter().map(|x| TermCountData {
            term: format!("{}", x.term),
            count: x.count
        }).collect()
    };
    state.render_template("top_terms", &data)
}

#[derive(Deserialize)]
struct TermDocsQuery {
    field: String,
    term: String,
}

#[derive(Serialize)]
struct DocAddress {
    doc: DocId,
    segment: String,
}

#[derive(Serialize)]
struct TermDocsData {
    field: String,
    term: String,
    term_docs: Vec<DocAddress>,
    truncated: bool,
}

fn handle_term_docs(req: (HttpRequest<State>, Query<TermDocsQuery>)) -> Result<HttpResponse, TantivyViewerError> {
    let (req, params) = req;
    let state = req.state();
    let field = params.field.clone();
    let term = params.term.clone();

    let term_docs = tantivy_viewer::term_docs(&state.index, &field, &term)
        .map_err(TantivyViewerError::TantivyError)?;

    let num_docs = term_docs.len();

    let term_docs = term_docs.into_iter()
        .map(|x| DocAddress { doc: x.1, segment: x.0.short_uuid_string() })
        .take(1000)
        .collect::<Vec<_>>();

    let truncated = num_docs > term_docs.len();

    let term_docs_data = TermDocsData {
        field,
        term,
        term_docs,
        truncated,
    };

    state.render_template("term_docs", &term_docs_data)
}

#[derive(Deserialize)]
struct ReconstructQuery {
    field: Option<String>,
    segment: String,
    doc: DocId,
}

#[derive(Serialize)]
struct ReconstructEntry {
    field: String,
    contents: String,
}

#[derive(Serialize)]
struct ReconstructData {
    segment: String,
    doc: DocId,
    all_fields: bool,
    entries: Vec<ReconstructEntry>,
}

fn find_segment(index: &Index, segment_str: &str) -> Result<Option<SegmentId>, tantivy::Error> {
    for segment_id in index.searchable_segment_ids()?.into_iter() {
        if segment_id.uuid_string().starts_with(segment_str) {
            return Ok(Some(segment_id));
        }
    }
    Ok(None)
}

fn stringify_values(values: Vec<Option<TantivyValue>>) -> String {
    values.into_iter()
        .map(|opt| opt.map(|x| format!("{} ", x)).unwrap_or_default())
        .collect()
}

fn reconstruct_to_string(index: &Index, field: &str, segment: &str, doc: DocId) -> Result<String, TantivyViewerError> {
    let segment = find_segment(index, segment)
        .map_err(TantivyViewerError::TantivyError)?
        .ok_or(TantivyViewerError::SegmentNotFoundError)?;
    Ok(
        stringify_values(
            tantivy_viewer::reconstruct_one(index, field, segment, doc)
            .map_err(TantivyViewerError::TantivyError)?
        )
    )
}

fn handle_reconstruct(req: (HttpRequest<State>, Query<ReconstructQuery>)) -> Result<HttpResponse, TantivyViewerError> {
    let (req, params) = req;
    let state = req.state();
    let field = params.field.clone();
    let segment = params.segment.clone();
    let doc = params.doc;

    let mut fields = Vec::new();
    let all_fields = field.is_none();
    if let Some(field) = field {
        // Reconstruct a specific field
        fields.push(field);
    } else {
        // Reconstruct all fields
        let schema = state.index.schema();
        fields.extend(schema.fields().iter().map(|x| x.name().to_string()));
    }

    fields.sort();

    let mut all_reconstructed = Vec::new();

    for field in fields {
        let contents = reconstruct_to_string(&state.index, &field, &segment, doc)?;

        all_reconstructed.push(ReconstructEntry {
            field,
            contents
        });
    }

    let data = ReconstructData {
        segment,
        doc,
        all_fields,
        entries: all_reconstructed,
    };

    state.render_template("reconstruct", &data)
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

fn handle_search(req: (HttpRequest<State>, Query<SearchQuery>)) -> Result<HttpResponse, TantivyViewerError> {
    let (req, params) = req;
    let state = req.state();
    let raw_query = match params.query {
        None => return state.render_template("search", &SearchData::empty()),
        Some(ref query) => query.clone(),
    };

    let query_parser = QueryParser::for_index(&state.index, vec![]);
    let query = query_parser.parse_query(&raw_query).map_err(TantivyViewerError::QueryParserError)?;

    let searcher = state.index.searcher();
    let mut collector = DocCollector::new();
    query.search(&*searcher, &mut collector).map_err(TantivyViewerError::TantivyError)?;

    let docs = collector.into_docs();

    let identifying_fields = get_identifying_fields(&req);

    let mut remaining = 1000;
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
        let reconstructed = tantivy_viewer::reconstruct(&state.index, &*field, &docs_to_reconstruct)?;
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

    state.render_template("search", &data)
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

fn main() -> Result<(), Error> {
    env_logger::init();

    let args = env::args().collect::<Vec<_>>();
    let index = Arc::new(Index::open_in_dir(&args[1]).unwrap());

    let mut handlebars = Handlebars::new();
    handlebars.register_helper("pretty_bytes", Box::new(pretty_bytes));
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
            .resource("/term_docs", |r| r.method(http::Method::GET).with(handle_term_docs))
            .resource("/reconstruct", |r| r.method(http::Method::GET).with(handle_reconstruct))
            .resource("/search", |r| r.method(http::Method::GET).with(handle_search))
    ).bind("0.0.0.0:3001").unwrap().run();

    Ok(())
}