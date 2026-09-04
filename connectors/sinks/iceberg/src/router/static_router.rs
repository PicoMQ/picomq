// Modified from Apache Iggy for PicoMQ.
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use async_trait::async_trait;
use dashmap::DashMap;
use iceberg::Catalog;
use iceberg::table::Table;
use picomq_connector_sdk::destination::DestinationTemplate;
use picomq_connector_sdk::{ConsumedMessage, Error, MessagesMetadata, Payload, TopicMetadata};
use tracing::{error, info, warn};

use crate::router::{Router, is_valid_namespaced_table, table_exists, write_data};

#[derive(Debug)]
pub(crate) struct StaticRouter {
    templates: Vec<DestinationTemplate>,
    tables: DashMap<String, Table>,
    catalog: Box<dyn Catalog>,
}

impl StaticRouter {
    pub async fn new(
        catalog: Box<dyn Catalog>,
        declared_tables: &[DestinationTemplate],
    ) -> Result<Self, Error> {
        let tables = DashMap::new();
        let mut templates = Vec::with_capacity(declared_tables.len());
        for declared_table in declared_tables {
            if !declared_table.is_static() {
                templates.push(declared_table.clone());
                continue;
            }
            let name = declared_table.to_string();
            if !is_valid_namespaced_table(&name) {
                error!(
                    "Declared table {} is not valid. It has to include at least one namespace before the table name separated by '.' character",
                    name
                );
                continue;
            }
            match table_exists(&name, catalog.as_ref()).await {
                Some(table) => {
                    tables.insert(name, table);
                    templates.push(declared_table.clone());
                }
                None => warn!(
                    "Declared table {} doesn't exist in the configured catalog. Skipping...",
                    name
                ),
            }
        }
        info!(
            "Static router resolved {} tables ({} templated) from {} declared",
            tables.len(),
            templates
                .iter()
                .filter(|template| !template.is_static())
                .count(),
            declared_tables.len()
        );
        if templates.is_empty() {
            error!("No valid tables found. Can't initiate Iceberg connector");
            return Err(Error::InvalidConfig);
        }
        Ok(StaticRouter {
            templates,
            tables,
            catalog,
        })
    }

    async fn resolve_tables(&self, topic: &str) -> Result<Vec<(String, Table)>, Error> {
        let mut resolved = Vec::with_capacity(self.templates.len());
        for template in &self.templates {
            let name = template.resolve(topic)?;
            if let Some(table) = self.tables.get(&name) {
                resolved.push((name, table.clone()));
                continue;
            }
            if !is_valid_namespaced_table(&name) {
                return Err(Error::InvalidConfigValue(format!(
                    "Resolved table '{name}' from template '{template}' has no namespace"
                )));
            }
            let Some(table) = table_exists(&name, self.catalog.as_ref()).await else {
                return Err(Error::CannotStoreData(format!(
                    "Table '{name}' resolved from template '{template}' for topic '{topic}' does not exist in the catalog"
                )));
            };
            self.tables.insert(name.clone(), table.clone());
            resolved.push((name, table));
        }
        Ok(resolved)
    }
}

#[async_trait]
impl Router for StaticRouter {
    async fn route_data(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        let data: Vec<Payload> = messages
            .into_iter()
            .map(|message| message.payload)
            .collect();

        for (name, table) in self.resolve_tables(&topic_metadata.topic).await? {
            if let Err(error) = write_data(
                &data,
                &table,
                self.catalog.as_ref(),
                messages_metadata.schema,
            )
            .await
            {
                self.tables.remove(&name);
                return Err(error);
            }
            info!(
                "Routed {} messages from topic {} to iceberg table {} successfully",
                data.len(),
                topic_metadata.topic,
                name
            );
        }

        Ok(())
    }
}
