use tantivy::Index;
use actix_web::HttpRequest;
use actix_web::HttpResponse;
use actix_web::Query;
use tantivy::query::QueryParser;
use tantivy::query::BooleanQuery;
use tantivy::query::Occur;

use TantivyViewerError;
use child_queries;
use query_to_string;
use State;

#[derive(Deserialize)]
pub struct DebugQuery {
    query: Option<String>,
    salient_docs_query: Option<String>,
}

#[derive(Serialize)]
pub struct DebugTree  {
    count: usize,
    query_string: String,
    search_string: String,
    salient_docs_query_string: Option<String>,
    children: Vec<DebugTree>,
}

impl DebugTree {
    fn empty() -> DebugTree {
        DebugTree {
            count: 0,
            query_string: String::new(),
            search_string: String::new(),
            salient_docs_query_string: None,
            children: Vec::new(),
        }
    }
}

fn debug_query(index: &Index, query: &tantivy::query::Query, salient_docs_query: &Option<Box<tantivy::query::Query>>) -> Result<DebugTree, TantivyViewerError> {
    let search_query = if let Some(ref salient_docs_query) = salient_docs_query {
        Box::new(BooleanQuery::from(
            vec![(Occur::Must, query.box_clone()), (Occur::Must, salient_docs_query.box_clone())]
        ))
    } else {
        query.box_clone()
    };

    let searcher = index.searcher();
    let count = search_query.count(&*searcher).map_err(TantivyViewerError::TantivyError)?;

    let children = child_queries(query)?;
    let children = children.into_iter()
        .map(|q| debug_query(index, &*q, salient_docs_query))
        .collect::<Result<Vec<_>, TantivyViewerError>>()?;

    Ok(DebugTree {
        count,
        query_string: query_to_string(query, &index.schema()),
        search_string: query_to_string(&*search_query, &index.schema()),
        salient_docs_query_string: None,
        children,
    })
}

pub(crate) fn handle_debug(req: (HttpRequest<State>, Query<DebugQuery>)) -> Result<HttpResponse, TantivyViewerError> {
    let (req, params) = req;
    let state = req.state();
    let raw_query = match params.query {
        None => return state.render_template("debug", &DebugTree::empty()),
        Some(ref query) => query.clone(),
    };

    let query_parser = QueryParser::for_index(&state.index, vec![]);
    let query = query_parser.parse_query(&raw_query).map_err(TantivyViewerError::QueryParserError)?;

    let raw_salient_docs_query = params.salient_docs_query.clone();
    let salient_docs_query = raw_salient_docs_query
        .filter(|x| !x.is_empty())
        .map(|q| query_parser.parse_query(&q).map_err(TantivyViewerError::QueryParserError))
        .transpose()?;

    let mut data = debug_query(&state.index, &*query, &salient_docs_query)?;
    data.salient_docs_query_string = params.salient_docs_query.clone();

    state.render_template("debug", &data)
}
