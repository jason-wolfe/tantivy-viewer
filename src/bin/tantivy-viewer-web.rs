extern crate handlebars;
extern crate handlebars_iron;
extern crate iron;
extern crate params;
extern crate router;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate tantivy;
extern crate tantivy_viewer;

use std::io::Result;

use iron::Chain;
use iron::Request;
use iron::Response;
use iron::Set;
use iron::IronResult;
use iron::prelude::Iron;
use handlebars_iron::HandlebarsEngine;
use handlebars_iron::DirectorySource;
use handlebars_iron::Template;
use tantivy::Index;
use iron::Handler;
use iron::prelude::*;
use iron::IronError;
use std::env;
use std::sync::Arc;
use router::Router;
use std::fmt;
use std::error;
use tantivy::DocId;

#[derive(Debug)]
enum UrlParameterError {
    MissingParameter { key: Vec<String> },
    InvalidParameter { key: Vec<String> },
}

impl fmt::Display for UrlParameterError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &UrlParameterError::MissingParameter { ref key } => write!(f, "Missing expected parameter {:?}", key),
            &UrlParameterError::InvalidParameter { ref key } => write!(f, "Invalid value for parameter {:?}", key),
        }
    }
}

impl error::Error for UrlParameterError {

}

fn get_parameter<T: params::FromValue>(params: &params::Map, key: &[&str]) -> IronResult<T> {
    use UrlParameterError::*;
    let param_value = params
        .find(key)
        .ok_or_else(|| IronError::new(MissingParameter { key: key.iter().map(|x| x.to_string()).collect() }, iron::status::BadRequest))?;

    <T>::from_value(param_value).ok_or_else(|| IronError::new(InvalidParameter { key: key.iter().map(|x| x.to_string()).collect() }, iron::status::BadRequest))
}

#[derive(Serialize)]
struct FieldData {
    name: String,
}

struct IndexHandler {
    index: Arc<Index>,
}

#[derive(Serialize)]
struct IndexData {
    fields: Vec<FieldData>,
    segments: Vec<String>,
}

impl Handler for IndexHandler {
    fn handle(&self, _: &mut Request) -> IronResult<Response> {
        let mut response = Response::new();
        let segments = self.index.searchable_segment_ids().map_err(|e| IronError::new(e, iron::status::InternalServerError))?;
        let data = IndexData {
            fields: self.index.schema().fields().iter().map(|x| FieldData { name: x.name().to_string() } ).collect(),
            segments: segments.into_iter().map(|x| x.short_uuid_string()).collect(),
        };
        response.set_mut(Template::new("index", data)).set_mut(iron::status::Ok);
        Ok(response)
    }
}

struct TopTermsHandler {
    index: Arc<Index>,
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

impl Handler for TopTermsHandler {
    fn handle(&self, req: &mut Request) -> IronResult<Response> {
        use params::{Params};
        let params = req.get_ref::<Params>().unwrap();
        let field: String = get_parameter(params, &["field"])?;
        let k = get_parameter(params, &["k"])?;
        let top_terms = tantivy_viewer::top_terms(&*self.index, &field, k).unwrap();
        let data = TopTermsData {
            field,
            terms: top_terms.terms.into_iter().map(|x| TermCountData {
                term: format!("{}", x.term),
                count: x.count
            }).collect()
        };
        let mut response = Response::new();
        response.set_mut(Template::new("top_terms", data)).set_mut(iron::status::Ok);
        Ok(response)
    }
}

struct TermDocsHandler {
    index: Arc<Index>,
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

impl Handler for TermDocsHandler {
    fn handle(&self, req: &mut Request) -> IronResult<Response> {
        use params::{Params};
        let params = req.get_ref::<Params>().unwrap();

        let field: String = get_parameter(params, &["field"])?;
        let term: String = get_parameter(params, &["term"])?;

        let term_docs = tantivy_viewer::term_docs(&*self.index, &field, &term)
            .map_err(|e| IronError::new(e, iron::status::InternalServerError))?;

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

        let mut response = Response::new();
        response.set_mut(Template::new("term_docs", term_docs_data)).set_mut(iron::status::Ok);
        Ok(response)
    }
}

struct ReconstructHandler {
    index: Arc<Index>,
}

#[derive(Serialize)]
struct ReconstructData {
    field: String,
    segment: String,
    doc: DocId,
    contents: String,
}

impl Handler for ReconstructHandler {
    fn handle(&self, req: &mut Request) -> IronResult<Response> {
        use params::{Params};
        let params = req.get_ref::<Params>().unwrap();

        let field: String = get_parameter(params, &["field"])?;
        let segment: String = get_parameter(params, &["segment"])?;
        let doc: DocId = get_parameter(params, &["doc"])?;

        let reconstructed =
            tantivy_viewer::reconstruct(&*self.index, &field, &segment, doc)
                .map_err(|e| IronError::new(e, iron::status::InternalServerError))?;

        let contents = reconstructed.into_iter()
            .map(|opt| opt.map(|x| format!("{} ", x)).unwrap_or_default())
            .collect::<String>();

        let reconstructed = ReconstructData {
            field,
            segment,
            doc,
            contents,
        };

        let mut response = Response::new();
        response.set_mut(Template::new("reconstruct", reconstructed)).set_mut(iron::status::Ok);
        Ok(response)
    }
}


fn main() -> Result<()> {
    let args = env::args().collect::<Vec<_>>();
    let index = Arc::new(Index::open(&args[1]).unwrap());

    let mut hbse = HandlebarsEngine::new();
    hbse.add(Box::new(DirectorySource::new("./templates", ".hbs")));

    hbse.reload().expect("failed to load templates");

    let mut router = Router::new();
    router.get("/", IndexHandler { index: index.clone() }, "index");
    router.get("/top_terms", TopTermsHandler { index: index.clone() }, "top_terms");
    router.get("/term_docs", TermDocsHandler { index: index.clone() }, "term_docs");
    router.get("/reconstruct", ReconstructHandler { index: index.clone() }, "reconstruct");

    let mut chain = Chain::new(router);

    chain.link_after(hbse);

    Iron::new(chain).http("0.0.0.0:3000").unwrap();

    Ok(())
}