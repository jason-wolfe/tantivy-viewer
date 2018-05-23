use std::collections::HashMap;
use tantivy::schema::Type;
use serde_json::Value;
use tantivy::schema::FieldType;
use tantivy::Index;
use tantivy::Result;
use serde_json;

#[derive(Debug)]
pub struct Fields {
    fields: HashMap<String, FieldDescriptor>,
}

#[derive(Debug)]
pub struct FieldDescriptor {
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

pub fn get_fields(index: &Index) -> Result<Fields> {
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
