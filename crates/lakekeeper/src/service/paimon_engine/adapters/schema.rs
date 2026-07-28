use crate::service::{
    LogicalField, LogicalPrimitiveType, LogicalSchema, LogicalType, PaimonEngineError,
    PaimonEngineField, PaimonEnginePrimitiveType, PaimonEngineSchema, PaimonEngineType,
};

pub fn logical_schema_from_engine(
    schema: &PaimonEngineSchema,
) -> Result<LogicalSchema, PaimonEngineError> {
    Ok(LogicalSchema {
        schema_id: schema.schema_id,
        root_fields: schema
            .root_fields
            .iter()
            .map(logical_field_from_engine)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

pub fn engine_schema_from_logical(
    schema: &LogicalSchema,
) -> Result<PaimonEngineSchema, PaimonEngineError> {
    Ok(PaimonEngineSchema {
        schema_id: schema.schema_id,
        root_fields: schema
            .root_fields
            .iter()
            .map(engine_field_from_logical)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn logical_field_from_engine(field: &PaimonEngineField) -> Result<LogicalField, PaimonEngineError> {
    Ok(LogicalField {
        field_id: field.field_id,
        name: field.name.clone(),
        required: field.required,
        doc: field.doc.clone(),
        field_type: logical_type_from_engine(&field.field_type)?,
        initial_default: field.initial_default.clone(),
        write_default: field.write_default.clone(),
        is_identity_hint: field.is_primary_key,
    })
}

fn logical_type_from_engine(
    field_type: &PaimonEngineType,
) -> Result<LogicalType, PaimonEngineError> {
    match field_type {
        PaimonEngineType::Primitive(primitive) => Ok(LogicalType::Primitive(
            logical_primitive_from_engine(primitive),
        )),
        PaimonEngineType::Struct { fields } => Ok(LogicalType::Struct {
            fields: fields
                .iter()
                .map(logical_field_from_engine)
                .collect::<Result<Vec<_>, _>>()?,
        }),
        PaimonEngineType::List { element_field } => Ok(LogicalType::List {
            element_field: Box::new(logical_field_from_engine(element_field)?),
        }),
        PaimonEngineType::Map {
            key_field,
            value_field,
        } => Ok(LogicalType::Map {
            key_field: Box::new(logical_field_from_engine(key_field)?),
            value_field: Box::new(logical_field_from_engine(value_field)?),
        }),
    }
}

fn logical_primitive_from_engine(primitive: &PaimonEnginePrimitiveType) -> LogicalPrimitiveType {
    match primitive {
        PaimonEnginePrimitiveType::Boolean => LogicalPrimitiveType::Boolean,
        PaimonEnginePrimitiveType::Int => LogicalPrimitiveType::Int,
        PaimonEnginePrimitiveType::Long => LogicalPrimitiveType::Long,
        PaimonEnginePrimitiveType::Float => LogicalPrimitiveType::Float,
        PaimonEnginePrimitiveType::Double => LogicalPrimitiveType::Double,
        PaimonEnginePrimitiveType::Decimal { precision, scale } => LogicalPrimitiveType::Decimal {
            precision: *precision,
            scale: *scale,
        },
        PaimonEnginePrimitiveType::Date => LogicalPrimitiveType::Date,
        PaimonEnginePrimitiveType::Time => LogicalPrimitiveType::Time,
        PaimonEnginePrimitiveType::Timestamp => LogicalPrimitiveType::Timestamp,
        PaimonEnginePrimitiveType::String => LogicalPrimitiveType::String,
        PaimonEnginePrimitiveType::Binary => LogicalPrimitiveType::Binary,
    }
}

fn engine_field_from_logical(field: &LogicalField) -> Result<PaimonEngineField, PaimonEngineError> {
    Ok(PaimonEngineField {
        field_id: field.field_id,
        name: field.name.clone(),
        required: field.required,
        doc: field.doc.clone(),
        field_type: engine_type_from_logical(&field.field_type)?,
        initial_default: field.initial_default.clone(),
        write_default: field.write_default.clone(),
        is_primary_key: field.is_identity_hint,
    })
}

fn engine_type_from_logical(
    field_type: &LogicalType,
) -> Result<PaimonEngineType, PaimonEngineError> {
    match field_type {
        LogicalType::Primitive(primitive) => Ok(PaimonEngineType::Primitive(
            engine_primitive_from_logical(primitive)?,
        )),
        LogicalType::Struct { fields } => Ok(PaimonEngineType::Struct {
            fields: fields
                .iter()
                .map(engine_field_from_logical)
                .collect::<Result<Vec<_>, _>>()?,
        }),
        LogicalType::List { element_field } => Ok(PaimonEngineType::List {
            element_field: Box::new(engine_field_from_logical(element_field)?),
        }),
        LogicalType::Map {
            key_field,
            value_field,
        } => Ok(PaimonEngineType::Map {
            key_field: Box::new(engine_field_from_logical(key_field)?),
            value_field: Box::new(engine_field_from_logical(value_field)?),
        }),
    }
}

fn engine_primitive_from_logical(
    primitive: &LogicalPrimitiveType,
) -> Result<PaimonEnginePrimitiveType, PaimonEngineError> {
    match primitive {
        LogicalPrimitiveType::Boolean => Ok(PaimonEnginePrimitiveType::Boolean),
        LogicalPrimitiveType::Int => Ok(PaimonEnginePrimitiveType::Int),
        LogicalPrimitiveType::Long => Ok(PaimonEnginePrimitiveType::Long),
        LogicalPrimitiveType::Float => Ok(PaimonEnginePrimitiveType::Float),
        LogicalPrimitiveType::Double => Ok(PaimonEnginePrimitiveType::Double),
        LogicalPrimitiveType::Decimal { precision, scale } => {
            Ok(PaimonEnginePrimitiveType::Decimal {
                precision: *precision,
                scale: *scale,
            })
        }
        LogicalPrimitiveType::Date => Ok(PaimonEnginePrimitiveType::Date),
        LogicalPrimitiveType::Time => Ok(PaimonEnginePrimitiveType::Time),
        LogicalPrimitiveType::Timestamp => Ok(PaimonEnginePrimitiveType::Timestamp),
        LogicalPrimitiveType::String => Ok(PaimonEnginePrimitiveType::String),
        LogicalPrimitiveType::Binary => Ok(PaimonEnginePrimitiveType::Binary),
        unsupported => Err(PaimonEngineError::unsupported_schema(format!(
            "logical type '{unsupported:?}' has no Paimon engine boundary mapping",
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::{engine_schema_from_logical, logical_schema_from_engine};
    use crate::service::{
        LogicalField, LogicalPrimitiveType, LogicalSchema, LogicalType, PaimonEngineField,
        PaimonEnginePrimitiveType, PaimonEngineSchema, PaimonEngineType,
    };

    fn nested_engine_schema() -> PaimonEngineSchema {
        PaimonEngineSchema {
            schema_id: 9,
            root_fields: vec![PaimonEngineField {
                field_id: 1,
                name: "payload".to_string(),
                required: true,
                doc: Some("root".to_string()),
                field_type: PaimonEngineType::Map {
                    key_field: Box::new(PaimonEngineField {
                        field_id: 2,
                        name: "key".to_string(),
                        required: true,
                        doc: None,
                        field_type: PaimonEngineType::Primitive(PaimonEnginePrimitiveType::String),
                        initial_default: None,
                        write_default: None,
                        is_primary_key: false,
                    }),
                    value_field: Box::new(PaimonEngineField {
                        field_id: 3,
                        name: "value".to_string(),
                        required: false,
                        doc: None,
                        field_type: PaimonEngineType::Struct {
                            fields: vec![PaimonEngineField {
                                field_id: 4,
                                name: "count".to_string(),
                                required: false,
                                doc: None,
                                field_type: PaimonEngineType::Primitive(
                                    PaimonEnginePrimitiveType::Int,
                                ),
                                initial_default: None,
                                write_default: None,
                                is_primary_key: false,
                            }],
                        },
                        initial_default: None,
                        write_default: None,
                        is_primary_key: false,
                    }),
                },
                initial_default: None,
                write_default: None,
                is_primary_key: true,
            }],
        }
    }

    #[test]
    fn converts_nested_engine_schema_into_logical_schema() {
        let logical = logical_schema_from_engine(&nested_engine_schema())
            .expect("nested engine schema must convert");
        assert_eq!(logical.schema_id, 9);
        assert_eq!(logical.root_fields[0].field_id, 1);
        assert!(logical.root_fields[0].is_identity_hint);
    }

    #[test]
    fn rejects_unsupported_logical_primitive_types() {
        let schema = LogicalSchema {
            schema_id: 1,
            root_fields: vec![LogicalField {
                field_id: 1,
                name: "id".to_string(),
                required: true,
                doc: None,
                field_type: LogicalType::Primitive(LogicalPrimitiveType::Uuid),
                initial_default: None,
                write_default: None,
                is_identity_hint: true,
            }],
        };

        let err = engine_schema_from_logical(&schema)
            .expect_err("unsupported logical primitives must fail");
        assert!(
            err.to_string()
                .contains("no Paimon engine boundary mapping")
        );
    }
}
