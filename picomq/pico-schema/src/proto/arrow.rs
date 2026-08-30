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

use std::{collections::BTreeMap, ops::Deref};

use crate::{
    proto::{MessageKind, Schema},
    AsArrow, Error, Result, ARROW_LIST_FIELD_NAME,
};

use arrow::{
    array::{
        ArrayBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder,
        LargeBinaryBuilder, ListBuilder, MapBuilder, StringBuilder, StructBuilder,
        TimestampMicrosecondBuilder,
    },
    datatypes::{DataType, Field, FieldRef, Fields, Schema as ArrowSchema, TimeUnit},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use chrono::{DateTime, Datelike};

use crate::record::Batch;
use protobuf::{
    reflect::{
        FieldDescriptor, FileDescriptor, MessageDescriptor, ReflectValueRef, RuntimeFieldType,
        RuntimeType,
    },
    CodedInputStream, MessageDyn,
};
use protobuf_json_mapping::print_to_string;
use serde_json::json;
use tracing::{debug, error, instrument};

const GOOGLE_PROTOBUF_TIMESTAMP: &str = "google.protobuf.Timestamp";

const KEY: &str = "Key";
const META: &str = "Meta";
const VALUE: &str = "Value";

const NULLABLE: bool = true;
const SORTED_MAP_KEYS: bool = false;

fn append<'a>(path: &[&'a str], name: &'a str) -> Vec<&'a str> {
    let mut path = Vec::from(path);
    path.push(name);
    path
}

#[derive(Default)]
struct RecordBuilder {
    meta: Option<Box<dyn ArrayBuilder>>,
    key: Option<Box<dyn ArrayBuilder>>,
    value: Option<Box<dyn ArrayBuilder>>,
}

impl RecordBuilder {
    fn new(ids: &BTreeMap<String, i32>, schema: &Schema) -> RecordBuilder {
        Self {
            meta: schema.message_by_package_relative_name_array_builder(ids, MessageKind::Meta),
            key: schema.message_by_package_relative_name_array_builder(ids, MessageKind::Key),
            value: schema.message_by_package_relative_name_array_builder(ids, MessageKind::Value),
        }
    }
}

impl Schema {
    fn new_list_field(
        &self,
        ids: &BTreeMap<String, i32>,
        path: &[&str],
        data_type: DataType,
    ) -> Field {
        self.new_field(ids, path, ARROW_LIST_FIELD_NAME, data_type)
    }

    fn new_field(
        &self,
        ids: &BTreeMap<String, i32>,
        path: &[&str],
        name: &str,
        data_type: DataType,
    ) -> Field {
        self.new_nullable_field(ids, path, name, data_type, NULLABLE)
    }

    fn new_nullable_field(
        &self,
        ids: &BTreeMap<String, i32>,
        path: &[&str],
        name: &str,
        data_type: DataType,
        nullable: bool,
    ) -> Field {
        debug!(?path, name, ?data_type, ?nullable, ?ids);

        let path = append(path, name).join(".");

        Field::new(name.to_owned(), data_type, nullable).with_metadata(
            ids.get(path.as_str())
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
    fn field(&self, ids: &BTreeMap<String, i32>, message_kind: MessageKind) -> Option<Field> {
        debug!(?message_kind);

        self.message_by_package_relative_name(message_kind)
            .inspect(|descriptor| debug!(?descriptor))
            .map(|descriptor| {
                let name = message_kind.as_ref().to_lowercase();

                self.new_nullable_field(
                    ids,
                    &[],
                    &name,
                    DataType::Struct(Fields::from(self.message_descriptor_to_fields(
                        ids,
                        &[&name],
                        &descriptor,
                    ))),
                    NULLABLE,
                )
            })
    }

    fn runtime_type_to_data_type(
        &self,
        ids: &BTreeMap<String, i32>,
        path: &[&str],
        runtime_type: &RuntimeType,
    ) -> DataType {
        debug!(?path, ?runtime_type);

        match runtime_type {
            RuntimeType::U32 | RuntimeType::I32 | RuntimeType::Enum(_) => DataType::Int32,
            RuntimeType::U64 | RuntimeType::I64 => DataType::Int64,
            RuntimeType::F32 => DataType::Float32,
            RuntimeType::F64 => DataType::Float64,
            RuntimeType::Bool => DataType::Boolean,
            RuntimeType::String => DataType::Utf8,
            RuntimeType::VecU8 => DataType::LargeBinary,
            RuntimeType::Message(descriptor) => {
                if descriptor.full_name() == GOOGLE_PROTOBUF_TIMESTAMP {
                    DataType::Timestamp(TimeUnit::Microsecond, None)
                } else {
                    DataType::Struct(Fields::from(
                        self.message_descriptor_to_fields(ids, path, descriptor),
                    ))
                }
            }
        }
    }

    fn message_descriptor_to_fields(
        &self,
        ids: &BTreeMap<String, i32>,
        path: &[&str],
        descriptor: &MessageDescriptor,
    ) -> Vec<Field> {
        debug!(?path, ?ids, descriptor_full_name = ?descriptor.full_name());

        descriptor
            .fields()
            .inspect(|field| {
                debug!(
                    name = field.name(),
                    full_name = field.full_name(),
                    type_name = field.proto().type_name()
                )
            })
            .map(|field| match field.runtime_field_type() {
                RuntimeFieldType::Singular(ref singular) => {
                    debug!(
                        descriptor = descriptor.name(),
                        field_name = field.name(),
                        ?singular
                    );

                    self.new_nullable_field(
                        ids,
                        path,
                        field.name(),
                        self.runtime_type_to_data_type(
                            ids,
                            &append(path, field.name())[..],
                            singular,
                        ),
                        !field.is_required(),
                    )
                }

                RuntimeFieldType::Repeated(ref repeated) => {
                    debug!(
                        descriptor = descriptor.name(),
                        field_name = field.name(),
                        ?repeated
                    );

                    self.new_nullable_field(
                        ids,
                        path,
                        field.name(),
                        {
                            let path = &append(path, field.name())[..];

                            DataType::List(FieldRef::new(self.new_list_field(
                                ids,
                                path,
                                self.runtime_type_to_data_type(
                                    ids,
                                    &append(path, ARROW_LIST_FIELD_NAME)[..],
                                    repeated,
                                ),
                            )))
                        },
                        !field.is_required(),
                    )
                }

                RuntimeFieldType::Map(ref key, ref value) => {
                    debug!(
                        descriptor = descriptor.name(),
                        field_name = field.name(),
                        ?key,
                        ?value
                    );

                    self.new_nullable_field(
                        ids,
                        path,
                        field.name(),
                        {
                            let path = &append(path, field.name())[..];

                            DataType::Map(
                                FieldRef::new(self.new_nullable_field(
                                    ids,
                                    path,
                                    "entries",
                                    DataType::Struct({
                                        let path = &append(path, "entries")[..];

                                        Fields::from_iter([
                                            self.new_nullable_field(
                                                ids,
                                                path,
                                                "keys",
                                                self.runtime_type_to_data_type(
                                                    ids,
                                                    append(path, "keys").as_slice(),
                                                    key,
                                                ),
                                                !NULLABLE,
                                            ),
                                            self.new_field(
                                                ids,
                                                path,
                                                "values",
                                                self.runtime_type_to_data_type(
                                                    ids,
                                                    append(path, "values").as_slice(),
                                                    value,
                                                ),
                                            ),
                                        ])
                                    }),
                                    !NULLABLE,
                                )),
                                SORTED_MAP_KEYS,
                            )
                        },
                        !field.is_required(),
                    )
                }
            })
            .collect::<Vec<_>>()
    }

    fn message_by_package_relative_name_array_builder(
        &self,
        ids: &BTreeMap<String, i32>,
        message_kind: MessageKind,
    ) -> Option<Box<dyn ArrayBuilder>> {
        debug!(?message_kind);
        self.message_by_package_relative_name(message_kind)
            .map(|descriptor| {
                self.message_descriptor_to_array_builder(
                    ids,
                    &[&message_kind.as_ref().to_lowercase()],
                    &descriptor,
                )
            })
    }

    fn message_descriptor_to_array_builder(
        &self,
        ids: &BTreeMap<String, i32>,
        path: &[&str],
        descriptor: &MessageDescriptor,
    ) -> Box<dyn ArrayBuilder> {
        debug!(?path, descriptor = descriptor.name());
        let fields = self.message_descriptor_to_fields(ids, path, descriptor);
        let builders = self.message_descriptor_array_builders(ids, path, descriptor);

        Box::new(StructBuilder::new(fields, builders)) as Box<dyn ArrayBuilder>
    }

    fn runtime_type_to_array_builder(
        &self,
        ids: &BTreeMap<String, i32>,
        path: &[&str],
        runtime_type: &RuntimeType,
    ) -> Box<dyn ArrayBuilder> {
        debug!(?path, ?runtime_type, ?ids);

        match runtime_type {
            RuntimeType::U32 | RuntimeType::I32 | RuntimeType::Enum(_) => {
                Box::new(Int32Builder::new())
            }
            RuntimeType::U64 | RuntimeType::I64 => Box::new(Int64Builder::new()),
            RuntimeType::F32 => Box::new(Float32Builder::new()),
            RuntimeType::F64 => Box::new(Float64Builder::new()),
            RuntimeType::Bool => Box::new(BooleanBuilder::new()),
            RuntimeType::String => Box::new(StringBuilder::new()),
            RuntimeType::VecU8 => Box::new(LargeBinaryBuilder::new()),

            RuntimeType::Message(descriptor) => {
                if descriptor.full_name() == GOOGLE_PROTOBUF_TIMESTAMP {
                    Box::new(TimestampMicrosecondBuilder::new())
                } else {
                    let (fields, builders) = descriptor
                        .fields()
                        .map(|field| match field.runtime_field_type() {
                            RuntimeFieldType::Singular(ref singular) => {
                                debug!(
                                    descriptor = descriptor.name(),
                                    field_name = field.name(),
                                    ?singular
                                );

                                (
                                    self.new_nullable_field(
                                        ids,
                                        path,
                                        field.name(),
                                        self.runtime_type_to_data_type(
                                            ids,
                                            &append(path, field.name())[..],
                                            singular,
                                        ),
                                        !field.is_required(),
                                    ),
                                    self.runtime_type_to_array_builder(ids, path, singular),
                                )
                            }

                            RuntimeFieldType::Repeated(ref repeated) => {
                                debug!(
                                    descriptor = descriptor.name(),
                                    field_name = field.name(),
                                    ?repeated
                                );

                                (
                                    self.new_nullable_field(
                                        ids,
                                        path,
                                        field.name(),
                                        {
                                            let path = &append(path, field.name())[..];

                                            DataType::List(FieldRef::new(self.new_list_field(
                                                ids,
                                                path,
                                                self.runtime_type_to_data_type(
                                                    ids,
                                                    &append(path, ARROW_LIST_FIELD_NAME)[..],
                                                    repeated,
                                                ),
                                            )))
                                        },
                                        !field.is_required(),
                                    ),
                                    {
                                        let path = &append(path, field.name())[..];

                                        Box::new(
                                            ListBuilder::new(self.runtime_type_to_array_builder(
                                                ids,
                                                &append(path, ARROW_LIST_FIELD_NAME)[..],
                                                repeated,
                                            ))
                                            .with_field(self.new_list_field(
                                                ids,
                                                path,
                                                self.runtime_type_to_data_type(
                                                    ids,
                                                    &append(path, ARROW_LIST_FIELD_NAME)[..],
                                                    repeated,
                                                ),
                                            )),
                                        )
                                            as Box<dyn ArrayBuilder>
                                    },
                                )
                            }

                            RuntimeFieldType::Map(ref key, ref value) => {
                                debug!(
                                    descriptor = descriptor.name(),
                                    field_name = field.name(),
                                    ?key,
                                    ?value
                                );

                                (
                                    self.new_nullable_field(
                                        ids,
                                        path,
                                        field.name(),
                                        {
                                            let path = &append(path, field.name())[..];

                                            DataType::Map(
                                                FieldRef::new(self.new_nullable_field(
                                                    ids,
                                                    path,
                                                    "entries",
                                                    DataType::Struct({
                                                        let path = &append(path, "entries")[..];

                                                        Fields::from_iter([
                                                            self.new_nullable_field(
                                                                ids,
                                                                path,
                                                                "keys",
                                                                self.runtime_type_to_data_type(
                                                                    ids,
                                                                    &append(path, "keys")[..],
                                                                    key,
                                                                ),
                                                                !NULLABLE,
                                                            ),
                                                            self.new_field(
                                                                ids,
                                                                path,
                                                                "values",
                                                                self.runtime_type_to_data_type(
                                                                    ids,
                                                                    &append(path, "values")[..],
                                                                    value,
                                                                ),
                                                            ),
                                                        ])
                                                    }),
                                                    !NULLABLE,
                                                )),
                                                SORTED_MAP_KEYS,
                                            )
                                        },
                                        !field.is_required(),
                                    ),
                                    Box::new(MapBuilder::new(
                                        None,
                                        self.runtime_type_to_array_builder(ids, path, key),
                                        self.runtime_type_to_array_builder(ids, path, value),
                                    )) as Box<dyn ArrayBuilder>,
                                )
                            }
                        })
                        .collect::<(Vec<_>, Vec<_>)>();

                    Box::new(StructBuilder::new(fields, builders))
                }
            }
        }
    }

    fn message_descriptor_array_builders(
        &self,
        ids: &BTreeMap<String, i32>,
        path: &[&str],
        descriptor: &MessageDescriptor,
    ) -> Vec<Box<dyn ArrayBuilder>> {
        debug!(?path, descriptor = descriptor.full_name(), ?ids);

        descriptor
            .fields()
            .map(|field| {
                let inside = &append(path, field.name())[..];

                match field.runtime_field_type() {
                    RuntimeFieldType::Singular(ref singular) => {
                        debug!(
                            descriptor = descriptor.name(),
                            field_name = field.name(),
                            ?singular,
                            ?inside
                        );
                        self.runtime_type_to_array_builder(ids, inside, singular)
                    }

                    RuntimeFieldType::Repeated(ref repeated) => {
                        debug!(
                            descriptor = descriptor.name(),
                            field_name = field.name(),
                            ?repeated,
                            ?inside
                        );
                        Box::new(
                            ListBuilder::new(self.runtime_type_to_array_builder(
                                ids,
                                &append(inside, ARROW_LIST_FIELD_NAME)[..],
                                repeated,
                            ))
                            .with_field(self.new_list_field(
                                ids,
                                inside,
                                self.runtime_type_to_data_type(
                                    ids,
                                    &append(inside, ARROW_LIST_FIELD_NAME)[..],
                                    repeated,
                                ),
                            )),
                        )
                    }

                    RuntimeFieldType::Map(ref key, ref value) => {
                        debug!(
                            descriptor = descriptor.name(),
                            field_name = field.name(),
                            ?key,
                            ?value,
                            ?inside
                        );

                        let path = &append(inside, "entries");

                        Box::new(
                            MapBuilder::new(
                                None,
                                self.runtime_type_to_array_builder(ids, path, key),
                                self.runtime_type_to_array_builder(ids, path, value),
                            )
                            .with_keys_field(self.new_nullable_field(
                                ids,
                                &path[..],
                                "keys",
                                self.runtime_type_to_data_type(ids, &append(path, "keys")[..], key),
                                !NULLABLE,
                            ))
                            .with_values_field(self.new_field(
                                ids,
                                &path[..],
                                "values",
                                self.runtime_type_to_data_type(
                                    ids,
                                    &append(path, "values")[..],
                                    value,
                                ),
                            )),
                        )
                    }
                }
            })
            .collect::<Vec<_>>()
    }
}

fn fields(ids: &BTreeMap<String, i32>, schema: &Schema) -> Fields {
    let mut fields = vec![];

    if let Some(field) = schema.field(ids, MessageKind::Meta) {
        fields.push(field);
    }

    if let Some(field) = schema.field(ids, MessageKind::Key) {
        fields.push(field);
    }

    if let Some(field) = schema.field(ids, MessageKind::Value) {
        fields.push(field);
    }

    fields.into()
}

fn arrow_schema(ids: &BTreeMap<String, i32>, schema: &Schema) -> ArrowSchema {
    ArrowSchema::new(fields(ids, schema))
}

fn append_struct_builder(message: &dyn MessageDyn, builder: &mut StructBuilder) -> Result<()> {
    debug!(%message, ?builder);
    for (index, ref field) in message.descriptor_dyn().fields().enumerate() {
        debug!(field_name = field.name());

        match field.runtime_field_type() {
            RuntimeFieldType::Singular(singular) => {
                debug!(?singular);

                match field.get_singular_field_or_default(message) {
                    ReflectValueRef::U32(value) => builder
                        .field_builder::<Int32Builder>(index)
                        .ok_or(Error::Downcast)
                        .and_then(|values| {
                            i32::try_from(value)
                                .map_err(Into::into)
                                .map(|value| values.append_value(value))
                        })?,

                    ReflectValueRef::U64(value) => builder
                        .field_builder::<Int64Builder>(index)
                        .ok_or(Error::BadDowncast {
                            field: field.name().to_owned(),
                        })
                        .and_then(|values| {
                            i64::try_from(value)
                                .map_err(Into::into)
                                .map(|value| values.append_value(value))
                        })?,

                    ReflectValueRef::I32(value) | ReflectValueRef::Enum(_, value) => builder
                        .field_builder::<Int32Builder>(index)
                        .ok_or(Error::BadDowncast {
                            field: field.name().to_owned(),
                        })
                        .map(|values| values.append_value(value))?,

                    ReflectValueRef::I64(value) => builder
                        .field_builder::<Int64Builder>(index)
                        .ok_or(Error::BadDowncast {
                            field: field.name().to_owned(),
                        })
                        .map(|values| values.append_value(value))?,

                    ReflectValueRef::F32(value) => builder
                        .field_builder::<Float32Builder>(index)
                        .ok_or(Error::BadDowncast {
                            field: field.name().to_owned(),
                        })
                        .map(|values| values.append_value(value))?,

                    ReflectValueRef::F64(value) => builder
                        .field_builder::<Float64Builder>(index)
                        .ok_or(Error::BadDowncast {
                            field: field.name().to_owned(),
                        })
                        .map(|values| values.append_value(value))?,

                    ReflectValueRef::Bool(value) => builder
                        .field_builder::<BooleanBuilder>(index)
                        .ok_or(Error::BadDowncast {
                            field: field.name().to_owned(),
                        })
                        .map(|values| values.append_value(value))?,

                    ReflectValueRef::String(value) => builder
                        .field_builder::<StringBuilder>(index)
                        .ok_or(Error::BadDowncast {
                            field: field.name().to_owned(),
                        })
                        .map(|values| values.append_value(value))?,

                    ReflectValueRef::Bytes(value) => builder
                        .field_builder::<LargeBinaryBuilder>(index)
                        .ok_or(Error::BadDowncast {
                            field: field.name().to_owned(),
                        })
                        .map(|values| values.append_value(value))?,

                    ReflectValueRef::Message(message_ref) => {
                        if message_ref.deref().descriptor_dyn().full_name()
                            == GOOGLE_PROTOBUF_TIMESTAMP
                        {
                            let message = print_to_string(message_ref.deref())?;
                            debug!(message = message.trim_matches('"'));

                            let value = DateTime::parse_from_rfc3339(message.trim_matches('"'))
                                .inspect(|dt| debug!(?dt))
                                .map(|dt| dt.timestamp_micros())?;
                            debug!(?value);

                            builder
                                .field_builder::<TimestampMicrosecondBuilder>(index)
                                .ok_or(Error::BadDowncast {
                                    field: field.name().to_owned(),
                                })
                                .map(|builder| builder.append_value(value))
                                .inspect_err(|err| debug!(?err, ?message_ref, ?builder))?
                        } else {
                            builder
                                .field_builder::<StructBuilder>(index)
                                .ok_or(Error::BadDowncast {
                                    field: field.name().to_owned(),
                                })
                                .and_then(|builder| {
                                    append_struct_builder(message_ref.deref(), builder)
                                })
                                .inspect_err(|err| debug!(?err, ?message_ref, ?builder))?
                        }
                    }
                }
            }

            RuntimeFieldType::Repeated(repeated) => {
                debug!(?repeated);

                let builder = builder
                    .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(index)
                    .ok_or(Error::Downcast)
                    .inspect_err(|err| error!(?err, ?repeated))?;

                let values = builder.values().as_any_mut();

                for value in field.get_repeated(message) {
                    match value {
                        ReflectValueRef::U32(value) => values
                            .downcast_mut::<Int32Builder>()
                            .ok_or(Error::BadDowncast {
                                field: field.name().to_owned(),
                            })
                            .inspect_err(|err| error!(?err, ?value, ?repeated))
                            .and_then(|builder| {
                                i32::try_from(value)
                                    .map_err(Into::into)
                                    .map(|value| builder.append_value(value))
                            })?,

                        ReflectValueRef::U64(value) => values
                            .downcast_mut::<Int64Builder>()
                            .ok_or(Error::BadDowncast {
                                field: field.name().to_owned(),
                            })
                            .inspect_err(|err| error!(?err, ?value, ?repeated))
                            .and_then(|builder| {
                                i64::try_from(value)
                                    .map_err(Into::into)
                                    .map(|value| builder.append_value(value))
                            })?,

                        ReflectValueRef::I32(value) | ReflectValueRef::Enum(_, value) => values
                            .downcast_mut::<Int32Builder>()
                            .ok_or(Error::BadDowncast {
                                field: field.name().to_owned(),
                            })
                            .inspect_err(|err| error!(?err, ?value, ?repeated))
                            .map(|builder| builder.append_value(value))?,

                        ReflectValueRef::I64(value) => values
                            .downcast_mut::<Int64Builder>()
                            .ok_or(Error::BadDowncast {
                                field: field.name().to_owned(),
                            })
                            .inspect_err(|err| error!(?err, ?value, ?repeated))
                            .map(|builder| builder.append_value(value))?,

                        ReflectValueRef::F32(value) => values
                            .downcast_mut::<Float32Builder>()
                            .ok_or(Error::BadDowncast {
                                field: field.name().to_owned(),
                            })
                            .inspect_err(|err| error!(?err, ?value, ?repeated))
                            .map(|builder| builder.append_value(value))?,

                        ReflectValueRef::F64(value) => values
                            .downcast_mut::<Float64Builder>()
                            .ok_or(Error::BadDowncast {
                                field: field.name().to_owned(),
                            })
                            .inspect_err(|err| error!(?err, ?value, ?repeated))
                            .map(|builder| builder.append_value(value))?,

                        ReflectValueRef::Bool(value) => values
                            .downcast_mut::<BooleanBuilder>()
                            .ok_or(Error::BadDowncast {
                                field: field.name().to_owned(),
                            })
                            .inspect_err(|err| error!(?err, ?value, ?repeated))
                            .map(|builder| builder.append_value(value))?,

                        ReflectValueRef::String(value) => values
                            .downcast_mut::<StringBuilder>()
                            .ok_or(Error::BadDowncast {
                                field: field.name().to_owned(),
                            })
                            .inspect_err(|err| error!(?err, ?value, ?repeated))
                            .map(|builder| builder.append_value(value))?,

                        ReflectValueRef::Bytes(value) => values
                            .downcast_mut::<LargeBinaryBuilder>()
                            .ok_or(Error::BadDowncast {
                                field: field.name().to_owned(),
                            })
                            .inspect_err(|err| error!(?err, ?value, ?repeated))
                            .map(|builder| builder.append_value(value))?,

                        ReflectValueRef::Message(message_ref) => {
                            if message_ref.deref().descriptor_dyn().full_name()
                                == GOOGLE_PROTOBUF_TIMESTAMP
                            {
                                let message = print_to_string(message_ref.deref())?;
                                debug!(message = message.trim_matches('"'));

                                let value = DateTime::parse_from_rfc3339(message.trim_matches('"'))
                                    .inspect(|dt| debug!(?dt))
                                    .map(|dt| dt.timestamp_micros())?;
                                debug!(?value);

                                values
                                    .downcast_mut::<TimestampMicrosecondBuilder>()
                                    .ok_or(Error::BadDowncast {
                                        field: field.name().to_owned(),
                                    })
                                    .map(|builder| builder.append_value(value))?
                            } else {
                                values
                                    .downcast_mut::<StructBuilder>()
                                    .ok_or(Error::BadDowncast {
                                        field: field.name().to_owned(),
                                    })
                                    .inspect_err(|err| error!(?err, ?message_ref))
                                    .and_then(|builder| {
                                        append_struct_builder(message_ref.deref(), builder)
                                    })?
                            }
                        }
                    }
                }

                builder.append(true);
            }

            RuntimeFieldType::Map(key, value) => {
                debug!(?key, ?value);

                builder
                    .as_any_mut()
                    .downcast_mut::<MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>>()
                    .ok_or(Error::BadDowncast {
                        field: field.name().to_owned(),
                    })
                    .and_then(|builder| append_map_builder(message, field, builder))?
            }
        }
    }

    builder.append(true);

    Ok(())
}

fn append_map_builder(
    message: &dyn MessageDyn,
    field: &FieldDescriptor,
    builder: &mut MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>,
) -> Result<()> {
    for (key, value) in &field.get_map(message) {
        decode_value(key, builder.keys())?;
        decode_value(value, builder.values())?;
    }

    builder.append(true).map_err(Into::into)
}

fn decode_value(value: ReflectValueRef<'_>, builder: &mut dyn ArrayBuilder) -> Result<()> {
    debug!(?value);

    match value {
        ReflectValueRef::U32(value) => builder
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .ok_or(Error::Downcast)
            .and_then(|builder| {
                i32::try_from(value)
                    .map_err(Into::into)
                    .map(|value| builder.append_value(value))
            }),

        ReflectValueRef::U64(value) => builder
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .ok_or(Error::Downcast)
            .and_then(|builder| {
                i64::try_from(value)
                    .map_err(Into::into)
                    .map(|value| builder.append_value(value))
            }),

        ReflectValueRef::I32(value) | ReflectValueRef::Enum(_, value) => builder
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        ReflectValueRef::I64(value) => builder
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        ReflectValueRef::F32(value) => builder
            .as_any_mut()
            .downcast_mut::<Float32Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        ReflectValueRef::F64(value) => builder
            .as_any_mut()
            .downcast_mut::<Float64Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        ReflectValueRef::Bool(value) => builder
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        ReflectValueRef::String(value) => builder
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        ReflectValueRef::Bytes(value) => builder
            .as_any_mut()
            .downcast_mut::<LargeBinaryBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        ReflectValueRef::Message(message_ref) => {
            if message_ref.descriptor_dyn().full_name() == GOOGLE_PROTOBUF_TIMESTAMP {
                let message = print_to_string(message_ref.deref())?;
                debug!(message = message.trim_matches('"'));

                let value = DateTime::parse_from_rfc3339(message.trim_matches('"'))
                    .inspect(|dt| debug!(?dt))
                    .map(|dt| dt.timestamp_micros())?;
                debug!(?value);

                builder
                    .as_any_mut()
                    .downcast_mut::<TimestampMicrosecondBuilder>()
                    .ok_or(Error::Downcast)
                    .map(|builder| builder.append_value(value))
            } else {
                builder
                    .as_any_mut()
                    .downcast_mut::<StructBuilder>()
                    .ok_or(Error::Downcast)
                    .inspect_err(|err| error!(?err, ?message_ref))
                    .and_then(|builder| append_struct_builder(message_ref.deref(), builder))
            }
        }
    }
}

fn process_message_descriptor<'a, T>(
    descriptor: Option<MessageDescriptor>,
    encoded: Option<Bytes>,
    builders: &mut T,
) -> Result<()>
where
    T: Iterator<Item = &'a mut Box<dyn ArrayBuilder>>,
{
    let Some(descriptor) = descriptor else {
        return Ok(());
    };

    debug!(descriptor = descriptor.name(), ?encoded,);

    let message = {
        let mut message = descriptor.new_instance();
        encoded.map_or(Err(crate::Error::InvalidRecord), |encoded| {
            message
                .merge_from_dyn(&mut CodedInputStream::from_tokio_bytes(&encoded))
                .inspect_err(|err| error!(?err))
                .map_err(|_err| crate::Error::InvalidRecord)
        })?;

        message
    };

    builders
        .next()
        .ok_or(Error::BuilderExhausted)
        .map(|column| column.as_any_mut())
        .inspect(|column| debug!(?column))
        .and_then(|column| {
            column
                .downcast_mut::<StructBuilder>()
                .ok_or(Error::Downcast)
                .inspect_err(|err| debug!(?err))
        })
        .and_then(|column| append_struct_builder(message.as_ref(), column))
        .inspect_err(|err| debug!(?err))
}

impl AsArrow for Schema {
    #[instrument(skip(self, batch), ret)]
    async fn as_arrow(&self, topic: &str, partition: i32, batch: &Batch) -> Result<RecordBatch> {
        let ids = if true {
            field_ids(&self.file_descriptors)
        } else {
            BTreeMap::new()
        };

        let schema = arrow_schema(&ids, self);
        let mut record_builder = RecordBuilder::new(&ids, self);

        for record in batch.records.iter() {
            debug!(?record);

            process_message_descriptor(
                self.message_by_package_relative_name(MessageKind::Meta),
                self.encode_from_value(
                    MessageKind::Meta,
                    &DateTime::from_timestamp_millis(batch.base_timestamp + record.timestamp_delta)
                        .map_or(json!({"partition": partition}), |date_time| {
                            json!({
                            "partition": partition,
                            "timestamp": date_time.to_rfc3339(),
                            "year": date_time.date_naive().year(),
                            "month": date_time.date_naive().month(),
                            "day": date_time.date_naive().day()})
                        }),
                )
                .map(Some)?,
                &mut record_builder.meta.iter_mut(),
            )
            .inspect_err(|err| debug!(?err))?;

            process_message_descriptor(
                self.message_by_package_relative_name(MessageKind::Key),
                record.key.clone(),
                &mut record_builder.key.iter_mut(),
            )
            .inspect_err(|err| debug!(?err))?;

            process_message_descriptor(
                self.message_by_package_relative_name(MessageKind::Value),
                record.value.clone(),
                &mut record_builder.value.iter_mut(),
            )
            .inspect_err(|err| debug!(?err))?;
        }

        debug!(
            meta_rows = ?record_builder.meta.iter().map(|rows| rows.len()).collect::<Vec<_>>(),
            key_rows = ?record_builder.key.iter().map(|rows| rows.len()).collect::<Vec<_>>(),
            value_rows = ?record_builder.value.iter().map(|rows| rows.len()).collect::<Vec<_>>()
        );

        let mut columns = vec![];

        if let Some(meta) = record_builder.meta {
            columns.push(meta);
        }

        if let Some(key) = record_builder.key {
            columns.push(key);
        }

        if let Some(value) = record_builder.value {
            columns.push(value);
        }

        debug!(columns = columns.len(), ?schema);

        RecordBatch::try_new(
            schema.into(),
            columns.iter_mut().map(|builder| builder.finish()).collect(),
        )
        .inspect_err(|err| debug!(?err))
        .inspect(|record_batch| debug!(?record_batch))
        .map_err(Into::into)
    }
}

fn field_ids(schemas: &[FileDescriptor]) -> BTreeMap<String, i32> {
    fn field_ids_with_path(
        path: &[&str],
        schemas: &[MessageDescriptor],
        id: &mut i32,
    ) -> BTreeMap<String, i32> {
        debug!(?path, ?schemas, ?id);

        let mut ids = BTreeMap::new();

        if path.is_empty() {
            for schema in schemas {
                _ = ids.insert(schema.name().to_lowercase(), *id);
                *id += 1;
            }
        }

        debug!(?ids);

        for schema in schemas {
            let name = schema.name().to_lowercase();

            let path = if path.is_empty() {
                Vec::from([&name[..]])
            } else {
                Vec::from(path)
            };

            for field in schema.fields() {
                debug!(path = ?path.join("."), field_name = ?field.name());
                let name = field.name().to_string();

                let path = {
                    let mut path = path.clone();
                    path.push(&name[..]);
                    path
                };

                _ = ids.insert(path.join("."), *id);
                *id += 1;
            }

            for field in schema.fields() {
                debug!(path = ?path.join("."), field_name = ?field.name());
                let name = field.name().to_string();

                let path = {
                    let mut path = path.clone();
                    path.push(&name[..]);
                    path
                };

                match field.runtime_field_type() {
                    RuntimeFieldType::Singular(singular) => {
                        debug!(?path, ?singular);

                        if let RuntimeType::Message(message_descriptor) = singular {
                            debug!(?path, ?message_descriptor);

                            if message_descriptor.full_name() != GOOGLE_PROTOBUF_TIMESTAMP {
                                ids.extend(field_ids_with_path(
                                    &path[..],
                                    &[message_descriptor],
                                    id,
                                ))
                            }
                        }
                    }

                    RuntimeFieldType::Repeated(repeated) => {
                        debug!(?path, ?repeated);

                        let path = {
                            let mut path = path.clone();
                            path.push(ARROW_LIST_FIELD_NAME);
                            path
                        };

                        _ = ids.insert(path.join("."), *id);
                        *id += 1;

                        if let RuntimeType::Message(message_descriptor) = repeated {
                            debug!(?path, ?message_descriptor);

                            ids.extend(field_ids_with_path(&path[..], &[message_descriptor], id))
                        }
                    }

                    RuntimeFieldType::Map(keys, values) => {
                        debug!(?path, ?keys, ?values);

                        let path = {
                            let mut path = path.clone();
                            path.push("entries");
                            path
                        };

                        _ = ids.insert(path.join("."), *id);
                        *id += 1;

                        {
                            let path = {
                                let mut path = path.clone();
                                path.push("keys");
                                path
                            };

                            _ = ids.insert(path.join("."), *id);
                            *id += 1;

                            if let RuntimeType::Message(message_descriptor) = keys {
                                debug!(?path, ?message_descriptor);

                                ids.extend(field_ids_with_path(
                                    &path[..],
                                    &[message_descriptor],
                                    id,
                                ))
                            }
                        }

                        {
                            let path = {
                                let mut path = path.clone();
                                path.push("values");
                                path
                            };

                            _ = ids.insert(path.join("."), *id);
                            *id += 1;

                            if let RuntimeType::Message(message_descriptor) = values {
                                debug!(?path, ?message_descriptor);

                                ids.extend(field_ids_with_path(
                                    &path[..],
                                    &[message_descriptor],
                                    id,
                                ))
                            }
                        }
                    }
                }
            }
        }

        debug!(?ids);
        ids
    }

    let descriptors = schemas
        .iter()
        .find_map(|fd| fd.message_by_package_relative_name(META))
        .into_iter()
        .chain(
            schemas
                .iter()
                .find_map(|fd| fd.message_by_package_relative_name(KEY))
                .into_iter()
                .chain(
                    schemas
                        .iter()
                        .find_map(|fd| fd.message_by_package_relative_name(VALUE)),
                ),
        )
        .collect::<Vec<_>>();

    field_ids_with_path(&[], &descriptors[..], &mut 1)
}
