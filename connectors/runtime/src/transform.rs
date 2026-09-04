use crate::RuntimeError;
use crate::configs::connectors::{SharedTransformConfig, TransformsConfig};
use picomq_connector_sdk::transforms::Transform;
use serde::Deserialize;
use std::sync::Arc;

pub fn load(config: &TransformsConfig) -> Result<Vec<Arc<dyn Transform>>, RuntimeError> {
    let mut transforms: Vec<Arc<dyn Transform>> = vec![];
    for (r#type, transform_config) in config.transforms.iter() {
        let shared_config = if transform_config.is_null() {
            SharedTransformConfig::default()
        } else {
            SharedTransformConfig::deserialize(transform_config).map_err(|error| {
                RuntimeError::InvalidConfiguration(format!(
                    "Failed to parse transform config. {error}",
                ))
            })?
        };

        if !shared_config.enabled {
            continue;
        }

        let transform = picomq_connector_sdk::transforms::from_config(*r#type, transform_config)?;
        transforms.push(transform);
    }

    Ok(transforms)
}
