// Copyright 2026 PicoMQ contributors
// Copyright ⓒ 2024-2026 Peter Morgan
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{collections::HashMap, iter::zip};

use crate::record::Batch;
use apache_avro::{
    schema::{ArraySchema, MapSchema, RecordSchema, Schema as AvroSchema, UnionSchema},
    types::Value,
    Reader,
};
use arrow::{
    array::{
        ArrayBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Decimal256Builder,
        Float32Builder, Float64Builder, Int32Builder, Int64Builder, LargeBinaryBuilder,
        ListBuilder, MapBuilder, NullBuilder, StringBuilder, StructBuilder,
        Time32MillisecondBuilder, Time64MicrosecondBuilder, Time64NanosecondBuilder,
        TimestampMicrosecondBuilder, TimestampMillisecondBuilder, TimestampNanosecondBuilder,
        UInt32Builder,
    },
    datatypes::{
        DataType, Field, FieldRef, Fields, Schema as ArrowSchema, TimeUnit, UnionFields, UnionMode,
    },
    record_batch::RecordBatch,
};
use bytes::Bytes;
use chrono::{DateTime, Datelike};
use num_bigint::BigInt;
use tracing::{debug, error, instrument};

use crate::{
    avro::{r, schema_write, Schema},
    AsArrow, Error, Result, ARROW_LIST_FIELD_NAME,
};

const NULLABLE: bool = true;
const SORTED_MAP_KEYS: bool = false;

trait NullableVariant {
    fn nullable_variant(&self) -> Option<&AvroSchema>;
}

impl NullableVariant for UnionSchema {
    fn nullable_variant(&self) -> Option<&AvroSchema> {
        if self.variants().len() == 2
            && self
                .variants()
                .iter()
                .inspect(|variant| debug!(?variant))
                .any(|schema| matches!(schema, AvroSchema::Null))
        {
            self.variants()
                .iter()
                .find(|schema| !matches!(schema, AvroSchema::Null))
                .inspect(|schema| debug!(?schema))
        } else {
            None
        }
    }
}

fn append<'a>(path: &[&'a str], name: &'a str) -> Vec<&'a str> {
    let mut path = Vec::from(path);
    path.push(name);
    path
}

impl Schema {
    fn new_list_field(&self, path: &[&str], data_type: DataType) -> Field {
        self.new_field(path, ARROW_LIST_FIELD_NAME, data_type)
    }

    fn new_field(&self, path: &[&str], name: &str, data_type: DataType) -> Field {
        self.new_nullable_field(path, name, data_type, NULLABLE)
    }

    fn new_nullable_field(
        &self,
        path: &[&str],
        name: &str,
        data_type: DataType,
        nullable: bool,
    ) -> Field {
        debug!(?path, name, ?data_type, ?nullable);

        let path = append(path, name).join(".");

        Field::new(name.to_owned(), data_type, nullable).with_metadata(
            self.ids
                .get(path.as_str())
                .inspect(|field_id| debug!(?path, name, field_id))
                .map(|field_id| {
                    (
                        crate::PARQUET_FIELD_ID_META_KEY.to_string(),
                        field_id.to_string(),
                    )
                })
                .into_iter()
                .collect(),
        )
    }

    fn schema_data_type(&self, path: &[&str], schema: &AvroSchema) -> Result<DataType> {
        debug!(?path, ?schema);

        match schema {
            AvroSchema::Null => Ok(DataType::Null),
            AvroSchema::Boolean => Ok(DataType::Boolean),
            AvroSchema::Int => Ok(DataType::Int32),
            AvroSchema::Long => Ok(DataType::Int64),
            AvroSchema::Float => Ok(DataType::Float32),
            AvroSchema::Double => Ok(DataType::Float64),
            AvroSchema::Bytes => Ok(DataType::LargeBinary),
            AvroSchema::String | AvroSchema::Uuid | AvroSchema::Enum(_) => Ok(DataType::Utf8),

            AvroSchema::Array(schema) => self
                .schema_data_type(path, &schema.items)
                .inspect(|data_type| debug!(?schema, ?data_type))
                .map(|data_type| {
                    DataType::List(FieldRef::new(self.new_list_field(path, data_type)))
                }),

            AvroSchema::Map(schema) => self
                .schema_data_type(path, &schema.types)
                .inspect(|value| debug!(?schema, ?value))
                .map(|value| {
                    let inside = append(path, "entries");

                    DataType::Map(
                        FieldRef::new(self.new_nullable_field(
                            path,
                            "entries",
                            DataType::Struct(Fields::from_iter([
                                self.new_nullable_field(
                                    &inside[..],
                                    "keys",
                                    DataType::Utf8,
                                    !NULLABLE,
                                ),
                                self.new_field(&inside[..], "values", value),
                            ])),
                            !NULLABLE,
                        )),
                        SORTED_MAP_KEYS,
                    )
                }),

            AvroSchema::Union(schema) => {
                debug!(?schema);

                if let Some(schema) = schema.nullable_variant() {
                    self.schema_data_type(path, schema)
                } else {
                    schema
                        .variants()
                        .iter()
                        .enumerate()
                        .map(|(index, variant)| {
                            self.schema_data_type(path, variant)
                                .map(|data_type| {
                                    Field::new(format!("field{}", index + 1), data_type, NULLABLE)
                                })
                                .inspect(|field| debug!(?field))
                        })
                        .collect::<Result<Vec<_>>>()
                        .inspect(|fields| debug!(?fields))
                        .and_then(|fields| {
                            i8::try_from(schema.variants().len())
                                .map_err(Into::into)
                                .and_then(|type_ids| {
                                    UnionFields::try_new((1..=type_ids).collect::<Vec<_>>(), fields)
                                        .map_err(Into::into)
                                })
                        })
                        .inspect(|union_fields| debug!(?union_fields))
                        .map(|fields| DataType::Union(fields, UnionMode::Dense))
                }
            }

            AvroSchema::Record(schema) => {
                debug!(?schema);
                schema
                    .fields
                    .iter()
                    .map(|field| {
                        let inside = append(path, &field.name);

                        self.schema_data_type(&inside[..], &field.schema)
                            .map(|data_type| self.new_field(path, &field.name, data_type))
                    })
                    .collect::<Result<Vec<_>>>()
                    .map(Fields::from)
                    .map(DataType::Struct)
            }

            AvroSchema::Fixed(schema) => i32::try_from(schema.size)
                .map(DataType::FixedSizeBinary)
                .map_err(Into::into),

            AvroSchema::Decimal(schema) => u8::try_from(schema.precision)
                .and_then(|precision| {
                    i8::try_from(schema.scale).map(|scale| {
                        if precision <= 16 {
                            DataType::Decimal128(precision, scale)
                        } else {
                            DataType::Decimal256(precision, scale)
                        }
                    })
                })
                .map_err(Into::into),

            AvroSchema::BigDecimal => todo!(),

            AvroSchema::Date => Ok(DataType::Date32),

            AvroSchema::TimeMillis => Ok(DataType::Time32(TimeUnit::Millisecond)),

            AvroSchema::TimeMicros => Ok(DataType::Time64(TimeUnit::Microsecond)),

            AvroSchema::TimestampMillis => Ok(DataType::Timestamp(TimeUnit::Millisecond, None)),

            AvroSchema::TimestampMicros => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),

            AvroSchema::TimestampNanos => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),

            AvroSchema::LocalTimestampMillis => {
                Ok(DataType::Timestamp(TimeUnit::Millisecond, None))
            }

            AvroSchema::LocalTimestampMicros => {
                Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
            }

            AvroSchema::LocalTimestampNanos => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),

            AvroSchema::Duration => Ok(DataType::Struct(Fields::from_iter([
                Field::new("month", DataType::UInt32, NULLABLE),
                Field::new("days", DataType::UInt32, NULLABLE),
                Field::new("milliseconds", DataType::UInt32, NULLABLE),
            ]))),

            AvroSchema::Ref { name } => {
                let _ = name;
                todo!();
            }
        }
    }

    fn schema_array_builder(
        &self,
        path: &[&str],
        schema: &AvroSchema,
    ) -> Result<Box<dyn ArrayBuilder>> {
        debug!(?path, ?schema);
        match schema {
            AvroSchema::Null => Ok(Box::new(NullBuilder::new())),
            AvroSchema::Boolean => Ok(Box::new(BooleanBuilder::new())),
            AvroSchema::Int => Ok(Box::new(Int32Builder::new())),
            AvroSchema::Long => Ok(Box::new(Int64Builder::new())),
            AvroSchema::Float => Ok(Box::new(Float32Builder::new())),
            AvroSchema::Double => Ok(Box::new(Float64Builder::new())),
            AvroSchema::Bytes => Ok(Box::new(LargeBinaryBuilder::new())),
            AvroSchema::String | AvroSchema::Uuid | AvroSchema::Enum(_) => {
                Ok(Box::new(StringBuilder::new()))
            }

            AvroSchema::Array(schema) => self
                .schema_array_builder(path, &schema.items)
                .map(ListBuilder::new)
                .and_then(|list_builder| {
                    self.schema_data_type(path, &schema.items).map(|data_type| {
                        list_builder.with_field(self.new_list_field(path, data_type))
                    })
                })
                .map(|builder| Box::new(builder) as Box<dyn ArrayBuilder>),

            AvroSchema::Map(schema) => self
                .schema_array_builder(
                    {
                        let mut path = Vec::from(path);
                        path.push("entries");
                        path.push("values");
                        path
                    }
                    .as_slice(),
                    &schema.types,
                )
                .and_then(|builder| {
                    self.schema_data_type(path, &schema.types).map(|data_type| {
                        let path = {
                            let mut path = Vec::from(path);
                            path.push("entries");
                            path
                        };

                        MapBuilder::new(
                            None,
                            Box::new(StringBuilder::new()) as Box<dyn ArrayBuilder>,
                            builder,
                        )
                        .with_keys_field(self.new_nullable_field(
                            &path[..],
                            "keys",
                            DataType::Utf8,
                            !NULLABLE,
                        ))
                        .with_values_field(self.new_field(
                            &path[..],
                            "values",
                            data_type,
                        ))
                    })
                })
                .map(|builder| Box::new(builder) as Box<dyn ArrayBuilder>),

            AvroSchema::Union(schema) => {
                if let Some(schema) = schema.nullable_variant() {
                    self.schema_array_builder(path, schema)
                } else {
                    todo!()
                }
            }

            AvroSchema::Record(schema) => schema
                .fields
                .iter()
                .map(|record_field| {
                    let inside = &append(path, &record_field.name)[..];

                    self.schema_data_type(inside, &record_field.schema)
                        .map(|data_type| self.new_field(path, &record_field.name, data_type))
                        .and_then(|field| {
                            self.schema_array_builder(inside, &record_field.schema)
                                .map(|builder| (field, builder))
                        })
                })
                .collect::<Result<(Vec<_>, Vec<_>)>>()
                .map(|(fields, builders)| StructBuilder::new(fields, builders))
                .map(|builder| Box::new(builder) as Box<dyn ArrayBuilder>),

            AvroSchema::Fixed(_schema) => Ok(Box::new(LargeBinaryBuilder::new())),

            AvroSchema::Decimal(schema) => u8::try_from(schema.precision)
                .map(|precision| {
                    if precision <= 16 {
                        Box::new(Decimal128Builder::new()) as Box<dyn ArrayBuilder>
                    } else {
                        Box::new(Decimal256Builder::new()) as Box<dyn ArrayBuilder>
                    }
                })
                .map_err(Into::into),

            AvroSchema::BigDecimal => todo!(),
            AvroSchema::Date => Ok(Box::new(Date32Builder::new())),
            AvroSchema::TimeMillis => Ok(Box::new(Time32MillisecondBuilder::new())),
            AvroSchema::TimeMicros => Ok(Box::new(Time64MicrosecondBuilder::new())),
            AvroSchema::TimestampMillis => Ok(Box::new(TimestampMillisecondBuilder::new())),
            AvroSchema::TimestampMicros => Ok(Box::new(TimestampMicrosecondBuilder::new())),
            AvroSchema::TimestampNanos => Ok(Box::new(TimestampNanosecondBuilder::new())),
            AvroSchema::LocalTimestampMillis => Ok(Box::new(Time32MillisecondBuilder::new())),
            AvroSchema::LocalTimestampMicros => Ok(Box::new(Time64MicrosecondBuilder::new())),
            AvroSchema::LocalTimestampNanos => Ok(Box::new(Time64NanosecondBuilder::new())),

            AvroSchema::Duration => Ok(Box::new(StructBuilder::new(
                vec![
                    Field::new("month", DataType::UInt32, NULLABLE),
                    Field::new("days", DataType::UInt32, NULLABLE),
                    Field::new("milliseconds", DataType::UInt32, NULLABLE),
                ],
                vec![
                    Box::new(UInt32Builder::new()),
                    Box::new(UInt32Builder::new()),
                    Box::new(UInt32Builder::new()),
                ],
            ))),

            AvroSchema::Ref { name } => {
                let _ = name;
                todo!();
            }
        }
    }
}

#[derive(Default)]
struct RecordBuilder(Vec<Box<dyn ArrayBuilder>>);

impl TryFrom<&Schema> for RecordBuilder {
    type Error = Error;

    fn try_from(schema: &Schema) -> Result<Self, Self::Error> {
        debug!(?schema);

        schema
            .complete
            .as_ref()
            .map_or(Ok(vec![]), |complete| {
                complete
                    .fields
                    .iter()
                    .inspect(|field| debug!(?field))
                    .map(|field| schema.schema_array_builder(&[&field.name], &field.schema))
                    .collect::<Result<Vec<_>>>()
            })
            .map(Self)
    }
}

macro_rules! try_as {
    ($name:ident, $pattern:path, $type:ty) => {
        fn $name(value: Value) -> Result<$type> {
            if let $pattern(value) = value {
                Ok(value)
            } else {
                Err(Error::InvalidValue(value))
            }
        }
    };
}

try_as!(try_as_i32, Value::Int, i32);
try_as!(try_as_bool, Value::Boolean, bool);
try_as!(try_as_i64, Value::Long, i64);
try_as!(try_as_f32, Value::Float, f32);
try_as!(try_as_f64, Value::Double, f64);
try_as!(try_as_bytes, Value::Bytes, Vec<u8>);
try_as!(try_as_string, Value::String, String);
try_as!(try_as_record, Value::Record, Vec<(String, Value)>);

fn append_list_builder(
    schema: &ArraySchema,
    values: Vec<Value>,
    builder: &mut ListBuilder<Box<dyn ArrayBuilder>>,
) -> Result<()> {
    match schema.items.as_ref() {
        AvroSchema::Null => builder
            .values()
            .as_any_mut()
            .downcast_mut::<NullBuilder>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema, ?values))
            .and_then(|builder| {
                values
                    .into_iter()
                    .map(try_as_bool)
                    .collect::<Result<Vec<_>>>()
                    .map(|values| builder.append_nulls(values.len()))
            })?,

        AvroSchema::Boolean => builder
            .values()
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema, ?values))
            .and_then(|builder| {
                values
                    .into_iter()
                    .map(try_as_bool)
                    .collect::<Result<Vec<_>>>()
                    .map(|values| builder.append_slice(values.as_slice()))
            })?,

        AvroSchema::Int => builder
            .values()
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema, ?values))
            .and_then(|builder| {
                values
                    .into_iter()
                    .map(try_as_i32)
                    .collect::<Result<Vec<_>>>()
                    .map(|values| builder.append_slice(values.as_slice()))
            })?,

        AvroSchema::Long => builder
            .values()
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema, ?values))
            .and_then(|builder| {
                values
                    .into_iter()
                    .map(try_as_i64)
                    .collect::<Result<Vec<_>>>()
                    .map(|values| builder.append_slice(values.as_slice()))
            })?,

        AvroSchema::Float => builder
            .values()
            .as_any_mut()
            .downcast_mut::<Float32Builder>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema, ?values))
            .and_then(|builder| {
                values
                    .into_iter()
                    .map(try_as_f32)
                    .collect::<Result<Vec<_>>>()
                    .map(|values| builder.append_slice(values.as_slice()))
            })?,

        AvroSchema::Double => builder
            .values()
            .as_any_mut()
            .downcast_mut::<Float64Builder>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema, ?values))
            .and_then(|builder| {
                values
                    .into_iter()
                    .map(try_as_f64)
                    .collect::<Result<Vec<_>>>()
                    .map(|values| builder.append_slice(values.as_slice()))
            })?,

        AvroSchema::Bytes => builder
            .values()
            .as_any_mut()
            .downcast_mut::<LargeBinaryBuilder>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema, ?values))
            .and_then(|builder| {
                values
                    .into_iter()
                    .map(try_as_bytes)
                    .collect::<Result<Vec<_>>>()
                    .map(|values| {
                        for value in values {
                            builder.append_value(value);
                        }
                    })
            })?,

        AvroSchema::String | AvroSchema::Uuid => builder
            .values()
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema, ?values))
            .and_then(|builder| {
                values
                    .into_iter()
                    .map(try_as_string)
                    .collect::<Result<Vec<_>>>()
                    .map(|values| {
                        for value in values {
                            builder.append_value(value);
                        }
                    })
            })?,

        AvroSchema::Array(_schema) => todo!(),
        AvroSchema::Map(_schema) => todo!(),
        AvroSchema::Union(_schema) => todo!(),

        AvroSchema::Record(schema) => builder
            .values()
            .as_any_mut()
            .downcast_mut::<StructBuilder>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema, ?values))
            .and_then(|builder| {
                values
                    .into_iter()
                    .map(try_as_record)
                    .collect::<Result<Vec<_>>>()
                    .and_then(|values| {
                        values
                            .into_iter()
                            .map(|items| append_struct_builder(schema, items, builder))
                            .collect::<Result<Vec<_>>>()
                    })
            })
            .map(|_| ())?,

        AvroSchema::Enum(_schema) => todo!(),
        AvroSchema::Fixed(_schema) => todo!(),
        AvroSchema::Decimal(_schema) => todo!(),
        AvroSchema::BigDecimal => todo!(),

        AvroSchema::Date => builder
            .values()
            .as_any_mut()
            .downcast_mut::<Date32Builder>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema, ?values))
            .and_then(|builder| {
                values
                    .into_iter()
                    .map(try_as_i32)
                    .collect::<Result<Vec<_>>>()
                    .map(|values| {
                        for value in values {
                            builder.append_value(value);
                        }
                    })
            })?,

        AvroSchema::TimeMillis => builder
            .values()
            .as_any_mut()
            .downcast_mut::<Time32MillisecondBuilder>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema, ?values))
            .and_then(|builder| {
                values
                    .into_iter()
                    .map(try_as_i32)
                    .collect::<Result<Vec<_>>>()
                    .map(|values| {
                        for value in values {
                            builder.append_value(value);
                        }
                    })
            })?,

        AvroSchema::TimeMicros => builder
            .values()
            .as_any_mut()
            .downcast_mut::<Time64MicrosecondBuilder>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema, ?values))
            .and_then(|builder| {
                values
                    .into_iter()
                    .map(try_as_i64)
                    .collect::<Result<Vec<_>>>()
                    .map(|values| {
                        for value in values {
                            builder.append_value(value);
                        }
                    })
            })?,

        AvroSchema::TimestampMillis => todo!(),
        AvroSchema::TimestampMicros => todo!(),
        AvroSchema::TimestampNanos => todo!(),
        AvroSchema::LocalTimestampMillis => todo!(),
        AvroSchema::LocalTimestampMicros => todo!(),
        AvroSchema::LocalTimestampNanos => todo!(),
        AvroSchema::Duration => todo!(),
        AvroSchema::Ref { name } => {
            let _ = name;
            todo!()
        }
    }

    builder.append(true);

    Ok(())
}

fn append_map_builder(
    schema: &MapSchema,
    values: HashMap<String, Value>,
    builder: &mut MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>,
) -> Result<()> {
    debug!(?schema, ?values);

    for (key, value) in values {
        append_value(None, Value::String(key), builder.keys())?;
        append_value(None, value, builder.values())?;
    }

    builder.append(true).map_err(Into::into)
}

fn append_struct_builder(
    schema: &RecordSchema,
    items: Vec<(String, Value)>,
    builder: &mut StructBuilder,
) -> Result<()> {
    for (index, (field, (name, value))) in zip(schema.fields.as_slice(), items).enumerate() {
        debug!(?index, ?field, ?name, ?value);

        match (&field.schema, value) {
            (AvroSchema::Null, Value::Null) => builder
                .field_builder::<NullBuilder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_null())?,

            (AvroSchema::Boolean, Value::Boolean(value)) => builder
                .field_builder::<BooleanBuilder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (AvroSchema::Int, Value::Int(value)) => builder
                .field_builder::<Int32Builder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (AvroSchema::Long, Value::Long(value)) => builder
                .field_builder::<Int64Builder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (AvroSchema::Float, Value::Float(value)) => builder
                .field_builder::<Float32Builder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (AvroSchema::Double, Value::Double(value)) => builder
                .field_builder::<Float64Builder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (AvroSchema::Bytes, Value::Bytes(value)) => builder
                .field_builder::<LargeBinaryBuilder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (AvroSchema::String, Value::String(value))
            | (AvroSchema::Enum(_), Value::Enum(_, value)) => builder
                .field_builder::<StringBuilder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (AvroSchema::Array(schema), Value::Array(values)) => builder
                .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(index)
                .ok_or(Error::BadDowncast { field: name })
                .inspect_err(|err| error!(?err, ?schema, ?values))
                .and_then(|builder| append_list_builder(schema, values, builder))?,

            (AvroSchema::Map(schema), Value::Map(values)) => builder
                .field_builder::<MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>>(index)
                .ok_or(Error::BadDowncast { field: name })
                .inspect_err(|err| error!(?err, ?schema, ?values))
                .and_then(|builder| append_map_builder(schema, values, builder))?,

            (AvroSchema::Union(_schema), Value::Union(_, _value)) => {
                todo!()
            }

            (AvroSchema::Record(schema), Value::Record(items)) => builder
                .field_builder::<StructBuilder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .and_then(|builder| append_struct_builder(schema, items, builder))?,

            (AvroSchema::Fixed(_fixed_schema), _) => todo!(),
            (AvroSchema::Decimal(_decimal_schema), _) => todo!(),
            (AvroSchema::BigDecimal, _) => todo!(),

            (AvroSchema::Uuid, Value::Uuid(value)) => builder
                .field_builder::<StringBuilder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value.to_string()))?,

            (AvroSchema::Date, Value::Date(value)) => builder
                .field_builder::<Date32Builder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (AvroSchema::TimeMillis, Value::TimeMillis(value)) => builder
                .field_builder::<Time32MillisecondBuilder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (AvroSchema::TimeMicros, Value::TimeMicros(value)) => builder
                .field_builder::<Time64MicrosecondBuilder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (AvroSchema::TimestampMillis, Value::TimestampMillis(value)) => builder
                .field_builder::<TimestampMillisecondBuilder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (AvroSchema::TimestampMicros, Value::TimestampMicros(value)) => builder
                .field_builder::<TimestampMicrosecondBuilder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (AvroSchema::TimestampNanos, Value::TimestampNanos(value)) => builder
                .field_builder::<TimestampNanosecondBuilder>(index)
                .ok_or(Error::BadDowncast { field: name })
                .map(|values| values.append_value(value))?,

            (AvroSchema::LocalTimestampMillis, _) => todo!(),
            (AvroSchema::LocalTimestampMicros, _) => todo!(),
            (AvroSchema::LocalTimestampNanos, _) => todo!(),
            (AvroSchema::Duration, _) => todo!(),
            (AvroSchema::Ref { name }, _) => {
                let _ = name;
                todo!();
            }
            (schema, value) => unimplemented!("schema: {schema:?}, value: {value:?}"),
        }
    }

    builder.append(true);
    Ok(())
}

fn append_value(
    schema: Option<&AvroSchema>,
    value: Value,
    column: &mut Box<dyn ArrayBuilder>,
) -> Result<()> {
    debug!(?value);

    match (schema, value) {
        (Some(AvroSchema::Boolean), Value::Null) => column
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_null()),

        (Some(AvroSchema::Int), Value::Null) => column
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_null()),

        (Some(AvroSchema::Long), Value::Null) => column
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_null()),

        (Some(AvroSchema::Float), Value::Null) => column
            .as_any_mut()
            .downcast_mut::<Float32Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_null()),

        (Some(AvroSchema::Double), Value::Null) => column
            .as_any_mut()
            .downcast_mut::<Float64Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_null()),

        (Some(AvroSchema::Bytes), Value::Null) => column
            .as_any_mut()
            .downcast_mut::<LargeBinaryBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_null()),

        (Some(AvroSchema::String), Value::Null) | (Some(AvroSchema::Enum(_)), Value::Null) => {
            column
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .ok_or(Error::Downcast)
                .map(|builder| builder.append_null())
        }

        (Some(AvroSchema::Fixed(_)), Value::Null) => column
            .as_any_mut()
            .downcast_mut::<LargeBinaryBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_null()),

        (Some(AvroSchema::Array(schema)), Value::Null) => column
            .as_any_mut()
            .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema))
            .map(|builder| builder.append_null()),

        (Some(AvroSchema::Record(_)), Value::Null) => column
            .as_any_mut()
            .downcast_mut::<StructBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_null()),

        (Some(AvroSchema::Map(schema)), Value::Null) => column
            .as_any_mut()
            .downcast_mut::<MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema))
            .inspect(|_| debug!(?schema))
            .and_then(|builder| builder.append(true).map_err(Into::into)),

        (Some(AvroSchema::Date), Value::Null) => column
            .as_any_mut()
            .downcast_mut::<Date32Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_null()),

        (Some(AvroSchema::TimeMillis), Value::Null) => column
            .as_any_mut()
            .downcast_mut::<Time32MillisecondBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_null()),

        (Some(AvroSchema::TimeMicros), Value::Null) => column
            .as_any_mut()
            .downcast_mut::<Time64MicrosecondBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_null()),

        (Some(AvroSchema::TimestampMillis), Value::Null) => column
            .as_any_mut()
            .downcast_mut::<TimestampMillisecondBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_null()),

        (Some(AvroSchema::TimestampMicros), Value::Null) => column
            .as_any_mut()
            .downcast_mut::<TimestampMicrosecondBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_null()),

        (Some(AvroSchema::LocalTimestampNanos), Value::Null) => column
            .as_any_mut()
            .downcast_mut::<TimestampNanosecondBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_null()),

        (Some(AvroSchema::Uuid), Value::Null) => column
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_null()),

        (schema, Value::Null) => {
            debug!(?schema);
            todo!()
        }

        (_, Value::Boolean(value)) => column
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (_, Value::Int(value)) => column
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (_, Value::Long(value)) => column
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (_, Value::Float(value)) => column
            .as_any_mut()
            .downcast_mut::<Float32Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (_, Value::Double(value)) => column
            .as_any_mut()
            .downcast_mut::<Float64Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (_, Value::Bytes(value)) => column
            .as_any_mut()
            .downcast_mut::<LargeBinaryBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (_, Value::String(value)) | (Some(AvroSchema::Enum(_)), Value::Enum(_, value)) => column
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (_, Value::Fixed(_, value)) => column
            .as_any_mut()
            .downcast_mut::<LargeBinaryBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (Some(AvroSchema::Union(schema)), Value::Union(_, value)) => {
            debug!(?schema, ?value);

            if let Some(schema) = schema.nullable_variant() {
                append_value(Some(schema), *value, column)
            } else {
                todo!()
            }
        }

        (Some(AvroSchema::Array(schema)), Value::Array(values)) => column
            .as_any_mut()
            .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema, ?values))
            .and_then(|builder| append_list_builder(schema, values, builder)),

        (Some(AvroSchema::Record(schema)), Value::Record(items)) => column
            .as_any_mut()
            .downcast_mut::<StructBuilder>()
            .ok_or(Error::Downcast)
            .and_then(|builder| append_struct_builder(schema, items, builder)),

        (Some(AvroSchema::Map(schema)), Value::Map(values)) => column
            .as_any_mut()
            .downcast_mut::<MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?schema, ?values))
            .inspect(|_| debug!(?schema, ?values))
            .and_then(|builder| append_map_builder(schema, values, builder)),

        (Some(AvroSchema::Date), Value::Date(value)) => column
            .as_any_mut()
            .downcast_mut::<Date32Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (schema, Value::Decimal(value)) => {
            let big_int = BigInt::from(value);
            todo!("schema: {schema:?}, value: {big_int:?}")
        }

        (schema, Value::BigDecimal(value)) => todo!("schema: {schema:?}, value: {value:?}"),

        (_, Value::TimeMillis(value)) => column
            .as_any_mut()
            .downcast_mut::<Time32MillisecondBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (_, Value::TimeMicros(value)) => column
            .as_any_mut()
            .downcast_mut::<Time64MicrosecondBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (_, Value::TimestampMillis(value)) => column
            .as_any_mut()
            .downcast_mut::<TimestampMillisecondBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (_, Value::TimestampMicros(value)) => column
            .as_any_mut()
            .downcast_mut::<TimestampMicrosecondBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (_, Value::TimestampNanos(value)) => column
            .as_any_mut()
            .downcast_mut::<TimestampNanosecondBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (schema, Value::LocalTimestampMillis(value)) => {
            todo!("schema: {schema:?}, value: {value:?}")
        }
        (schema, Value::LocalTimestampMicros(value)) => {
            todo!("schema: {schema:?}, value: {value:?}")
        }
        (schema, Value::LocalTimestampNanos(value)) => {
            todo!("schema: {schema:?}, value: {value:?}")
        }

        (schema, Value::Duration(value)) => todo!("schema: {schema:?}, value: {value:?}"),

        (_, Value::Uuid(value)) => column
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value.to_string())),

        (schema, value) => unimplemented!("schema: {schema:?}, value: {value:?}"),
    }
}

fn process<'a, T>(
    schema: Option<&AvroSchema>,
    encoded: Option<Bytes>,
    builders: &mut T,
) -> Result<()>
where
    T: Iterator<Item = &'a mut Box<dyn ArrayBuilder>>,
{
    schema.map_or(Ok(()), |schema| {
        builders
            .next()
            .ok_or(Error::BuilderExhausted)
            .and_then(|builder| {
                encoded
                    .map_or(Err(crate::Error::InvalidRecord), |encoded| {
                        Reader::with_schema(schema, &encoded[..])?
                            .next()
                            .transpose()
                            .map_err(Into::into)
                    })
                    .inspect(|value| debug!(?value))
                    .and_then(|value| value.ok_or(crate::Error::InvalidRecord))
                    .and_then(|value| append_value(Some(schema), value, builder))
                    .inspect_err(|err| error!(?err, ?schema))
            })
    })
}

impl AsArrow for Schema {
    #[instrument(skip(self, batch), ret)]
    async fn as_arrow(&self, topic: &str, partition: i32, batch: &Batch) -> Result<RecordBatch> {
        debug!(ids = ?self.ids);

        let schema = ArrowSchema::try_from(self)?;
        debug!(?schema);

        let mut record_builder = RecordBuilder::try_from(self)?;

        for record in &batch.records {
            debug!(?record);

            let mut builders = record_builder.0.iter_mut();

            process(self.key.as_ref(), record.key.clone(), &mut builders)?;

            process(self.value.as_ref(), record.value.clone(), &mut builders)?;

            process(
                self.meta.as_ref(),
                self.meta
                    .as_ref()
                    .map(|schema| {
                        schema_write(
                            schema,
                            r(
                                schema,
                                DateTime::from_timestamp_millis(
                                    batch.base_timestamp + record.timestamp_delta,
                                )
                                .map_or(
                                    [
                                        ("partition", Value::Int(partition)),
                                        (
                                            "timestamp",
                                            Value::Long(
                                                (batch.base_timestamp + record.timestamp_delta)
                                                    * 1_000,
                                            ),
                                        ),
                                        ("year", Value::Int(0)),
                                        ("month", Value::Int(0)),
                                        ("day", Value::Int(0)),
                                    ],
                                    |date_time| {
                                        [
                                            ("partition", Value::Int(partition)),
                                            (
                                                "timestamp",
                                                Value::Long(
                                                    (batch.base_timestamp + record.timestamp_delta)
                                                        * 1_000,
                                                ),
                                            ),
                                            ("year", Value::Int(date_time.date_naive().year())),
                                            (
                                                "month",
                                                Value::Int(date_time.date_naive().month() as i32),
                                            ),
                                            (
                                                "day",
                                                Value::Int(date_time.date_naive().day() as i32),
                                            ),
                                        ]
                                    },
                                ),
                            )
                            .into(),
                        )
                    })
                    .transpose()?,
                &mut builders,
            )?;
        }

        debug!(
            rows = ?record_builder.0.iter().map(|rows| rows.len()).collect::<Vec<_>>(),
        );

        RecordBatch::try_new(
            schema.into(),
            record_builder
                .0
                .iter_mut()
                .map(|builder| builder.finish())
                .collect(),
        )
        .map_err(Into::into)
    }
}

impl TryFrom<&Schema> for Fields {
    type Error = Error;

    fn try_from(schema: &Schema) -> Result<Self, Self::Error> {
        schema
            .complete
            .as_ref()
            .map_or(Ok(vec![]), |complete| {
                complete
                    .fields
                    .iter()
                    .inspect(|field| debug!(?field))
                    .map(|field| {
                        schema
                            .schema_data_type(&[&field.name], &field.schema)
                            .map(|data_type| schema.new_field(&[], &field.name, data_type))
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .map(Into::into)
    }
}

impl TryFrom<&Schema> for ArrowSchema {
    type Error = Error;

    fn try_from(schema: &Schema) -> Result<Self, Self::Error> {
        Fields::try_from(schema)
            .inspect(|fields| debug!(?fields))
            .map(ArrowSchema::new)
    }
}
