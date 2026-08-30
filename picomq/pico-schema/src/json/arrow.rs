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

use std::{any::type_name_of_val, sync::Arc};

use crate::{
    json::{MessageKind, Schema},
    AsArrow, Error, Result, ARROW_LIST_FIELD_NAME,
};

use arrow::{
    array::{
        ArrayBuilder, BooleanBuilder, Float64Builder, Int64Builder, ListBuilder, NullBuilder,
        StringBuilder, StructBuilder,
    },
    datatypes::{DataType, Field, FieldRef, Fields, Schema as ArrowSchema},
    record_batch::RecordBatch,
};

use chrono::{DateTime, Datelike};

use serde_json::{json, Map, Value};

use crate::record::Batch;
use tracing::{debug, error, instrument};

const NULLABLE: bool = true;

struct Record {
    meta: Value,
    key: Option<Value>,
    value: Option<Value>,
}

fn sort_dedup(mut input: Vec<DataType>) -> Vec<DataType> {
    input.sort();
    input.dedup();
    input
}

impl Schema {
    fn new_list_field(&self, path: &[&str], data_type: DataType) -> Field {
        self.new_field(path, ARROW_LIST_FIELD_NAME, data_type)
    }

    #[cfg(feature = "arrow")]
    fn new_field(&self, path: &[&str], name: &str, data_type: DataType) -> Field {
        debug!(?path, name, ?data_type, ids = ?self.ids);

        let path = {
            let mut path = Vec::from(path);
            path.push(name);
            path.join(".")
        };

        Field::new(name.to_owned(), data_type, NULLABLE).with_metadata(
            self.ids
                .get(path.as_str())
                .inspect(|field_id| debug!(?path, field_id))
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

    #[cfg(not(feature = "arrow"))]
    fn new_field(&self, path: &[&str], name: &str, data_type: DataType) -> Field {
        debug!(?path, name, ?data_type, ids = ?self.ids);

        Field::new(name.to_owned(), data_type, NULLABLE)
    }

    fn data_type(&self, path: &[&str], value: &Value) -> Result<DataType> {
        match value {
            Value::Null => Ok(DataType::Null),

            Value::Bool(_) => Ok(DataType::Boolean),

            Value::Number(value) => {
                if value.is_i64() | value.is_u64() {
                    Ok(DataType::Int64)
                } else {
                    Ok(DataType::Float64)
                }
            }

            Value::String(_) => Ok(DataType::Utf8),

            Value::Array(values) => self.common_data_type(path, values).map(|data_type| {
                DataType::List(FieldRef::new(self.new_list_field(path, data_type)))
            }),

            Value::Object(object) => object
                .iter()
                .map(|(k, v)| {
                    let child_path = {
                        let mut path = Vec::from(path);
                        path.push(k.as_str());
                        path
                    };

                    self.data_type(&child_path[..], v)
                        .map(|data_type| self.new_field(path, k, data_type))
                })
                .collect::<Result<Vec<_>>>()
                .map(Fields::from)
                .map(DataType::Struct),
        }
        .inspect(|data_type| debug!(?path, ?value, ?data_type))
        .inspect_err(|err| error!(?err, ?value))
    }

    fn common_data_type(&self, path: &[&str], values: &[Value]) -> Result<DataType> {
        debug!(?path, ?values);

        values
            .iter()
            .map(|value| self.data_type(path, value))
            .inspect(|data_type| debug!(?data_type))
            .collect::<Result<Vec<_>>>()
            .map(sort_dedup)
            .inspect(|data_types| debug!(?data_types))
            .and_then(|mut data_types| {
                if data_types.len() > 1 {
                    Err(Error::NoCommonType(data_types))
                } else if let Some(data_type) = data_types.pop() {
                    Ok(data_type)
                } else {
                    Ok(DataType::Null)
                }
            })
            .inspect(|data_type| debug!(?path, ?values, ?data_type))
            .inspect_err(|err| error!(?err, ?values))
    }

    fn data_type_builder(&self, path: &[&str], data_type: &DataType) -> Box<dyn ArrayBuilder> {
        debug!(path = path.join("."), ?data_type);

        match data_type {
            DataType::Null => Box::new(NullBuilder::new()),
            DataType::Boolean => Box::new(BooleanBuilder::new()),
            DataType::UInt64 => Box::new(Int64Builder::new()),
            DataType::Int64 => Box::new(Int64Builder::new()),
            DataType::Float64 => Box::new(Float64Builder::new()),
            DataType::Utf8 => Box::new(StringBuilder::new()),

            DataType::List(element) => {
                debug!(?element);

                Box::new(
                    ListBuilder::new(self.data_type_builder(
                        &append_path(path, ARROW_LIST_FIELD_NAME)[..],
                        element.data_type(),
                    ))
                    .with_field(self.new_list_field(path, element.data_type().to_owned())),
                ) as Box<dyn ArrayBuilder>
            }

            DataType::Struct(fields) => {
                debug!(?fields);

                Box::new(StructBuilder::new(
                    fields.to_owned(),
                    fields
                        .iter()
                        .map(|field| {
                            self.data_type_builder(
                                &append_path(path, field.name())[..],
                                field.data_type(),
                            )
                        })
                        .collect::<Vec<_>>(),
                ))
            }

            _ => unimplemented!("unexpected: {}", type_name_of_val(data_type)),
        }
    }
}

fn append_path<'a>(path: &[&'a str], name: &'a str) -> Vec<&'a str> {
    let mut path = Vec::from(path);
    path.push(name);
    path
}

fn append_list_builder(
    element: Arc<Field>,
    items: Vec<Value>,
    builder: &mut ListBuilder<Box<dyn ArrayBuilder>>,
) -> Result<()> {
    let values = builder.values().as_any_mut();

    for item in items {
        match (element.data_type(), item) {
            (_, Value::Null) => values
                .downcast_mut::<NullBuilder>()
                .ok_or(Error::Downcast)
                .map(|builder| builder.append_null())?,

            (_, Value::Bool(value)) => values
                .downcast_mut::<BooleanBuilder>()
                .ok_or(Error::Downcast)
                .map(|builder| builder.append_value(value))?,

            (DataType::Int64, Value::Number(value)) if value.is_u64() => values
                .downcast_mut::<Int64Builder>()
                .ok_or(Error::Downcast)
                .map(|builder| {
                    if let Some(value) = value.as_u64() {
                        builder.append_value(value as i64)
                    } else {
                        builder.append_null()
                    }
                })
                .inspect_err(|err| error!(?value, ?err))?,

            (DataType::Int64, Value::Number(value)) if value.is_i64() => values
                .downcast_mut::<Int64Builder>()
                .ok_or(Error::Downcast)
                .map(|builder| {
                    if let Some(value) = value.as_i64() {
                        builder.append_value(value)
                    } else {
                        builder.append_null()
                    }
                })
                .inspect_err(|err| error!(?value, ?err))?,

            (DataType::Float64, Value::Number(value)) if value.is_f64() => values
                .downcast_mut::<Float64Builder>()
                .ok_or(Error::Downcast)
                .map(|builder| {
                    if let Some(value) = value.as_f64() {
                        builder.append_value(value)
                    } else {
                        builder.append_null()
                    }
                })
                .inspect_err(|err| error!(?value, ?err))?,

            (_, Value::String(value)) => values
                .downcast_mut::<StringBuilder>()
                .ok_or(Error::Downcast)
                .map(|builder| builder.append_value(value))?,

            (DataType::List(element), Value::Array(items)) => values
                .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
                .ok_or(Error::Downcast)
                .inspect_err(|err| error!(?err, ?element, ?items))
                .and_then(|builder| append_list_builder(element.to_owned(), items, builder))?,

            (DataType::Struct(fields), Value::Object(object)) => values
                .downcast_mut::<StructBuilder>()
                .ok_or(Error::Downcast)
                .inspect_err(|err| error!(?err, ?fields, ?object))
                .and_then(|builder| append_struct_builder(fields, object, builder))?,

            (data_type, value) => Err(Error::UnsupportedSchemaRuntimeValue(
                data_type.to_owned(),
                value,
            ))?,
        }
    }

    builder.append(true);

    Ok(())
}

fn append_struct_builder(
    fields: &Fields,
    mut object: Map<String, Value>,
    builder: &mut StructBuilder,
) -> Result<()> {
    debug!(?fields, ?object);

    for (index, field) in fields.iter().enumerate() {
        if let Some(value) = object.remove(field.name()) {
            match (field.data_type(), value) {
                (_, Value::Null) => builder
                    .field_builder::<NullBuilder>(index)
                    .ok_or(Error::Downcast)
                    .map(|builder| builder.append_null())
                    .inspect_err(|err| error!(?err))?,

                (_, Value::Bool(value)) => builder
                    .field_builder::<BooleanBuilder>(index)
                    .ok_or(Error::Downcast)
                    .map(|builder| builder.append_value(value))
                    .inspect_err(|err| error!(?err))?,

                (DataType::Int64, Value::Number(value)) if value.is_u64() => builder
                    .field_builder::<Int64Builder>(index)
                    .ok_or(Error::Downcast)
                    .map(|builder| {
                        if let Some(value) = value.as_u64() {
                            builder.append_value(value as i64)
                        } else {
                            builder.append_null()
                        }
                    })
                    .inspect_err(|err| error!(?field, ?value, ?err))?,

                (DataType::Int64, Value::Number(value)) if value.is_i64() => builder
                    .field_builder::<Int64Builder>(index)
                    .ok_or(Error::Downcast)
                    .map(|builder| {
                        if let Some(value) = value.as_i64() {
                            builder.append_value(value)
                        } else {
                            builder.append_null()
                        }
                    })?,

                (DataType::Float64, Value::Number(value)) if value.is_f64() => builder
                    .field_builder::<Float64Builder>(index)
                    .ok_or(Error::Downcast)
                    .map(|builder| {
                        if let Some(value) = value.as_f64() {
                            builder.append_value(value)
                        } else {
                            builder.append_null()
                        }
                    })?,

                (DataType::Utf8, Value::String(value)) => builder
                    .field_builder::<StringBuilder>(index)
                    .ok_or(Error::Downcast)
                    .map(|builder| builder.append_value(value))
                    .inspect_err(|err| error!(?err))?,

                (DataType::List(element), Value::Array(items)) => builder
                    .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(index)
                    .ok_or(Error::Downcast)
                    .and_then(|builder| append_list_builder(element.to_owned(), items, builder))
                    .inspect_err(|err| error!(?err))?,

                (DataType::Struct(fields), Value::Object(object)) => builder
                    .field_builder::<StructBuilder>(index)
                    .ok_or(Error::Downcast)
                    .and_then(|builder| append_struct_builder(fields, object, builder))
                    .inspect_err(|err| error!(?err))?,

                (data_type, value) => Err(Error::UnsupportedSchemaRuntimeValue(
                    data_type.to_owned(),
                    value,
                ))?,
            }
        }
    }

    builder.append(true);

    Ok(())
}

fn append(field: &Field, value: Value, builder: &mut dyn ArrayBuilder) -> Result<()> {
    debug!(?field, ?value, builder = type_name_of_val(builder));

    match (field.data_type(), value) {
        (DataType::Null, _) => builder
            .as_any_mut()
            .downcast_mut::<NullBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_null()),

        (DataType::Boolean, Value::Bool(value)) => builder
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (DataType::Int64, Value::Number(value)) if value.is_u64() => builder
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| {
                if let Some(value) = value.as_u64() {
                    builder.append_value(value as i64)
                } else {
                    builder.append_null()
                }
            })
            .inspect_err(|err| error!(?field, ?value, ?err)),

        (DataType::Int64, Value::Number(value)) if value.is_i64() => builder
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| {
                if let Some(value) = value.as_i64() {
                    builder.append_value(value)
                } else {
                    builder.append_null()
                }
            }),

        (DataType::Float64, Value::Number(value)) if value.is_f64() => builder
            .as_any_mut()
            .downcast_mut::<Float64Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| {
                if let Some(value) = value.as_f64() {
                    builder.append_value(value)
                } else {
                    builder.append_null()
                }
            }),

        (DataType::Utf8, Value::String(value)) => builder
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (DataType::List(element), Value::Array(items)) => builder
            .as_any_mut()
            .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?element, ?items))
            .and_then(|builder| append_list_builder(element.to_owned(), items, builder)),

        (DataType::Struct(fields), Value::Object(object)) => builder
            .as_any_mut()
            .downcast_mut::<StructBuilder>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?fields, ?object))
            .and_then(|builder| append_struct_builder(fields, object, builder)),

        (data_type, value) => Err(Error::UnsupportedSchemaRuntimeValue(
            data_type.to_owned(),
            value,
        ))?,
    }
}

impl AsArrow for Schema {
    #[instrument(skip(self, batch), ret)]
    async fn as_arrow(&self, topic: &str, partition: i32, batch: &Batch) -> Result<RecordBatch> {
        let mut builders = vec![];
        let mut fields = vec![];

        {
            let meta = DateTime::from_timestamp_millis(batch.base_timestamp)
                .as_ref()
                .map(|date_time| {
                    json!({
                    "partition": partition,
                    "timestamp": date_time.to_rfc3339(),
                    "year": date_time.date_naive().year(),
                    "month": date_time.date_naive().month(),
                    "day": date_time.date_naive().day()})
                })
                .unwrap_or(json!({"partition": partition}));

            let data_type = self.common_data_type(&[MessageKind::Meta.as_ref()], &[meta][..])?;

            debug!(?data_type);
            builders.push(self.data_type_builder(&[MessageKind::Meta.as_ref()], &data_type));
            fields.push(self.new_field(&[], MessageKind::Meta.as_ref(), data_type))
        }

        if let Some(data_type) = batch
            .records
            .iter()
            .map(|record| {
                record.key.clone().map_or(Ok(None), |encoded| {
                    serde_json::from_slice::<Value>(&encoded[..])
                        .map(Some)
                        .map_err(Into::into)
                })
            })
            .collect::<Result<Vec<_>>>()
            .map(|values| values.into_iter().flatten().collect::<Vec<_>>())
            .and_then(|values| {
                if values.is_empty() {
                    Ok(None)
                } else {
                    self.common_data_type(&[MessageKind::Key.as_ref()], values.as_slice())
                        .map(Some)
                }
            })
            .inspect(|data_type| debug!(?data_type))?
        {
            builders.push(self.data_type_builder(&[MessageKind::Key.as_ref()], &data_type));
            fields.push(self.new_field(&[], MessageKind::Key.as_ref(), data_type))
        };

        if let Some(data_type) = batch
            .records
            .iter()
            .map(|record| {
                record.value.clone().map_or(Ok(None), |encoded| {
                    serde_json::from_slice::<Value>(&encoded[..])
                        .map(Some)
                        .map_err(Into::into)
                })
            })
            .collect::<Result<Vec<_>>>()
            .map(|values| values.into_iter().flatten().collect::<Vec<_>>())
            .and_then(|values| {
                if values.is_empty() {
                    Ok(None)
                } else {
                    self.common_data_type(&[MessageKind::Value.as_ref()], values.as_slice())
                        .map(Some)
                }
            })
            .inspect(|data_type| debug!(?data_type))?
        {
            builders.push(self.data_type_builder(&[MessageKind::Value.as_ref()], &data_type));
            fields.push(self.new_field(&[], MessageKind::Value.as_ref(), data_type))
        };

        for kv in batch
            .records
            .iter()
            .map(|record| {
                record
                    .key
                    .as_ref()
                    .map(|encoded| serde_json::from_slice::<Value>(&encoded[..]))
                    .transpose()
                    .map_err(Into::into)
                    .and_then(|key| {
                        let meta = DateTime::from_timestamp_millis(
                            batch.base_timestamp + record.timestamp_delta,
                        )
                        .as_ref()
                        .map(|date_time| {
                            json!({
                            "partition": partition,
                            "timestamp": date_time.to_rfc3339(),
                            "year": date_time.date_naive().year(),
                            "month": date_time.date_naive().month(),
                            "day": date_time.date_naive().day()})
                        })
                        .unwrap_or(json!({"partition": partition}));

                        record
                            .value
                            .as_ref()
                            .map(|encoded| serde_json::from_slice::<Value>(&encoded[..]))
                            .transpose()
                            .map_err(Into::into)
                            .map(|value| Record { meta, key, value })
                    })
            })
            .collect::<Result<Vec<_>>>()?
        {
            let mut i = fields.iter().zip(builders.iter_mut());

            let (field, builder) = i.next().unwrap();
            debug!(meta = %kv.meta, ?field);
            append(field, kv.meta, builder)?;

            if let Some(key) = kv.key {
                let (field, builder) = i.next().unwrap();
                debug!(%key, ?field);
                append(field, key, builder)?;
            }

            if let Some(value) = kv.value {
                let (field, builder) = i.next().unwrap();
                debug!(%value, ?field);
                append(field, value, builder)?;
            }
        }

        debug!(len = ?builders.iter().map(|builder|builder.len()).collect::<Vec<_>>());

        RecordBatch::try_new(
            Arc::new(ArrowSchema::new(Fields::from(fields))),
            builders
                .iter_mut()
                .map(|builder| builder.finish())
                .collect(),
        )
        .map_err(Into::into)
    }
}
