use std::collections::HashMap;
use std::sync::Arc;

use iceberg::{Catalog, CatalogBuilder};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
};
use iceberg_storage_opendal::OpenDalStorageFactory;

use super::{Error, IcebergSinkConfig, IcebergSinkStoreClass, IcebergSinkTypes};
use crate::props::init_props;

pub async fn init_catalog(config: &IcebergSinkConfig) -> Result<Box<dyn Catalog>, Error> {
    let props = init_props(config)?;
    match config.catalog_type {
        IcebergSinkTypes::REST => get_rest_catalog(config, props).await,
    }
}

#[inline(always)]
async fn get_rest_catalog(
    config: &IcebergSinkConfig,
    props: HashMap<String, String>,
) -> Result<Box<dyn Catalog>, Error> {
    let mut new_props = HashMap::from([
        (REST_CATALOG_PROP_URI.to_string(), config.uri.clone()),
        (
            REST_CATALOG_PROP_WAREHOUSE.to_string(),
            config.warehouse.clone(),
        ),
    ]);
    new_props.extend(props);

    let storage_factory: Arc<dyn iceberg::io::StorageFactory> = match &config.store_class {
        IcebergSinkStoreClass::S3 => Arc::new(OpenDalStorageFactory::S3 {
            customized_credential_load: None,
        }),
        other => {
            return Err(Error::InitError(format!(
                "Unsupported store class: {other}"
            )));
        }
    };

    let catalog = RestCatalogBuilder::default()
        .with_storage_factory(storage_factory)
        .load("rest", new_props)
        .await
        .map_err(|err| {
            let error = format!("Failed to initialize REST catalog: {}", err);
            Error::InitError(error)
        })?;

    Ok(Box::new(catalog))
}
