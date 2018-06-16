#![feature(transpose_result)]

extern crate actix_web;
extern crate env_logger;
extern crate failure;
#[macro_use]
extern crate failure_derive;
extern crate handlebars;
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
use std::fmt;
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

#[derive(Fail, Debug)]
enum TantivyViewerError {
    TantivyError(tantivy::Error),
    QueryParserError(tantivy::query::QueryParserError),
    RenderingError(handlebars::RenderError),
    JsonSerializationError,
}

impl fmt::Display for TantivyViewerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TantivyViewerError::TantivyError(_) => write!(f, "Tantivy error occurred"),
            TantivyViewerError::QueryParserError(_) => write!(f, "Query parsing error occurred"),
            TantivyViewerError::RenderingError(_) => write!(f, "Rendering error occurred"),
            TantivyViewerError::JsonSerializationError => write!(f, "Failed to serialize JSON"),
        }
    }
}

impl actix_web::error::ResponseError for TantivyViewerError {}

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
        let reconstructed =
            tantivy_viewer::reconstruct(&state.index, &field, &segment, doc)
                .map_err(TantivyViewerError::TantivyError)?;

        trace!("reconstructed = {:?}", reconstructed);

        all_reconstructed.push(ReconstructEntry {
            field,
            contents: reconstructed.into_iter()
                .map(|opt| opt.map(|x| format!("{} ", x)).unwrap_or_default())
                .collect::<String>()
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
    query: String,
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
    docs: Vec<(String, Vec<DocId>)>,
    truncated: bool,
}

fn handle_search(req: (HttpRequest<State>, Query<SearchQuery>)) -> Result<HttpResponse, TantivyViewerError> {
    let (req, params) = req;
    let state = req.state();
    let raw_query = params.query.clone();

    let query_parser = QueryParser::for_index(&state.index, vec![]);
    let query = query_parser.parse_query(&raw_query).map_err(TantivyViewerError::QueryParserError)?;

    let searcher = state.index.searcher();
    let mut collector = DocCollector::new();
    query.search(&*searcher, &mut collector).map_err(TantivyViewerError::TantivyError)?;

    let docs = collector.into_docs();

    let mut remaining = 1000;
    let mut result = Vec::new();
    let mut truncated = false;
    for (segment, docs) in docs.into_iter() {
        if remaining == 0 {
            break;
        }

        let take = remaining.min(docs.len());
        if take > 0 {
            result.push((segment.short_uuid_string(), (&docs[..take]).iter().cloned().collect()));
        }
        if take < docs.len() {
            truncated = true;
        }
        remaining -= take;
    }

    let data = SearchData {
        query: raw_query,
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
            .resource("/top_terms", |r| r.method(http::Method::GET).with(handle_top_terms))
            .resource("/term_docs", |r| r.method(http::Method::GET).with(handle_term_docs))
            .resource("/reconstruct", |r| r.method(http::Method::GET).with(handle_reconstruct))
            .resource("/search", |r| r.method(http::Method::GET).with(handle_search))
    ).bind("0.0.0.0:3001").unwrap().run();

    Ok(())
}