use tantivy::Index;
use std::collections::HashMap;
use tantivy::space_usage::PerFieldSpaceUsage;
use tantivy::schema::Schema;
use tantivy::space_usage::ByteCount;

fn add_fields(schema: &Schema, accum: &mut HashMap<String, usize>, usage: &PerFieldSpaceUsage) {
    for (field, usage) in usage.fields() {
        let name = schema.get_field_name(*field).to_string();
        *accum.entry(name).or_insert(0) += usage.total().0;
    }
}

fn add_concept(accum: &mut HashMap<String, usize>, key: &str, space: ByteCount) {
    *accum.entry(key.to_string()).or_insert(0) += space.0;
}

#[derive(Serialize)]
pub struct SpaceUsage {
    fields: HashMap<String, usize>,
    concepts: HashMap<String, usize>,
    total: usize,
}

pub fn space_usage(index: &Index) -> SpaceUsage {
    let schema = index.schema();
    let searcher = index.searcher();
    let space_usage = searcher.space_usage();

    let total = space_usage.total().0;
    let mut fields = HashMap::new();
    let mut concepts = HashMap::new();

    for segment in space_usage.segments() {
        add_fields(&schema, &mut fields, segment.termdict());
        add_concept(&mut concepts, "termdict", segment.termdict().total());

        add_fields(&schema, &mut fields, segment.postings());
        add_concept(&mut concepts, "postings", segment.postings().total());

        add_fields(&schema, &mut fields, segment.positions());
        add_concept(&mut concepts, "positions", segment.positions().total());

        add_fields(&schema, &mut fields, segment.fast_fields());
        add_concept(&mut concepts, "fast_fields", segment.fast_fields().total());

        add_fields(&schema, &mut fields, segment.fieldnorms());
        add_concept(&mut concepts, "fieldnorms", segment.fieldnorms().total());

        add_concept(&mut concepts, "deletes", segment.deletes());
        add_concept(&mut concepts, "store", segment.store().total());
    }

    SpaceUsage {
        fields,
        concepts,
        total,
    }
}