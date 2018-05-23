extern crate byteorder;
extern crate clap;
extern crate fst;
extern crate serde_json;
extern crate tantivy;

mod fields;
use fields::get_fields;
mod reconstruct;
use reconstruct::reconstruct;
mod top_terms;
use top_terms::top_terms;
mod term_docs;
use term_docs::term_docs;

use std::path::Path;
use tantivy::Index;
use tantivy::Result;
use clap::App;
use clap::Arg;
use clap::SubCommand;

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
        .subcommand(SubCommand::with_name("termdocs")
            .arg(Arg::with_name("field")
                .takes_value(true)
                .required(true)
                .index(1))
            .arg(Arg::with_name("term")
                .takes_value(true)
                .required(true)
                .index(2)))
        .subcommand(SubCommand::with_name("reconstruct")
            .arg(Arg::with_name("field")
                .takes_value(true)
                .required(true)
                .index(3))
            .arg(Arg::with_name("segment")
                .takes_value(true)
                .required(true)
                .index(1))
            .arg(Arg::with_name("doc")
                .takes_value(true)
                .required(true)
                .index(2)))
        .get_matches();
    let index_path = Path::new(matches.value_of("index").unwrap());
    let index = Index::open(index_path)?;
    match matches.subcommand() {
        ("fields", _) => {
            let fields = get_fields(&index)?;
            println!("{:#?}", fields);
        }
        ("reconstruct", Some(sub_args)) => {
            let field = sub_args.value_of("field").unwrap();
            let segment = sub_args.value_of("segment").unwrap();
            let doc = sub_args.value_of("doc").unwrap().parse::<u32>().expect("Invalid 'doc' value provided.");
            let reconstructed = reconstruct(&index, field, segment, doc)?;
            for value in reconstructed.into_iter().filter_map(|x| x) {
                print!("{:?} ", value);
            }
        }
        ("topterms", Some(sub_args)) => {
            let field = sub_args.value_of("field").unwrap();
            let k = sub_args.value_of("k").unwrap().parse::<usize>().expect("invalid 'k' value provided.");
            let top_terms = top_terms(&index, field.to_string(), k)?;
            for term in top_terms.terms.into_iter() {
                println!("{:?}", term);
            }
        }
        ("termdocs", Some(sub_args)) => {
            let field = sub_args.value_of("field").unwrap();
            let term_str = sub_args.value_of("term").unwrap();
            let docs = term_docs(&index, field, term_str)?;
            for (segment, doc) in docs.into_iter() {
                println!("({}, {})", segment.short_uuid_string(), doc);
            }
        }
        (ref command, _) => {
            println!("Unknown sub-command!: {}", command);
        }
    }
    Ok(())
}