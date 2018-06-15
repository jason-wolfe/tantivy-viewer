#![feature(transpose_result)]

extern crate actix_web;
extern crate env_logger;
extern crate failure;
#[macro_use]
extern crate failure_derive;
extern crate handlebars;
#[macro_use]
extern crate log;
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

#[derive(Fail, Debug)]
enum TantivyViewerError {
    TantivyError(tantivy::Error),
    RenderingError(handlebars::RenderError),
    JsonSerializationError,
}

impl fmt::Display for TantivyViewerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TantivyViewerError::TantivyError(_) => write!(f, "Tantivy error occurred"),
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
}

fn handle_index(req: HttpRequest<State>) -> Result<HttpResponse, TantivyViewerError> {
    let state = req.state();
    let index = &state.index;
    let segments = index.searchable_segment_ids().map_err(TantivyViewerError::TantivyError)?;
    let data = IndexData {
        fields: index.schema().fields().iter().map(|x| FieldData { name: x.name().to_string() }).collect(),
        segments: segments.into_iter().map(|x| x.short_uuid_string()).collect(),
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


fn main() -> Result<(), Error> {
    env_logger::init();

    let args = env::args().collect::<Vec<_>>();
    let index = Arc::new(Index::open_in_dir(&args[1]).unwrap());

    let mut handlebars = Handlebars::new();
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
            .resource("/top_terms", |r| r.method(http::Method::GET).with(handle_top_terms))
            .resource("/term_docs", |r| r.method(http::Method::GET).with(handle_term_docs))
            .resource("/reconstruct", |r| r.method(http::Method::GET).with(handle_reconstruct))
    ).bind("0.0.0.0:3001").unwrap().run();

    Ok(())
}