//! Schema commands.

use std::io::{Read, Write};
use std::path::PathBuf;

use bytes::Bytes;
use clap::Subcommand;
use picomq_client::{ClientError, StreamApi};

use crate::io::note;

#[derive(Debug, Subcommand)]
pub enum SchemaCommand {
    /// Register a schema, read from a file or stdin.
    Put {
        name: String,
        /// Read the schema from this file instead of stdin.
        #[arg(long)]
        file: Option<PathBuf>,
        /// The schema format. Defaults to the one the name's extension implies.
        #[arg(long)]
        content_type: Option<String>,
    },
    /// Print a schema.
    Get { name: String },
    /// Remove a schema.
    Rm { name: String },
}

pub async fn run(client: &dyn StreamApi, command: SchemaCommand) -> Result<i32, ClientError> {
    match command {
        SchemaCommand::Put {
            name,
            file,
            content_type,
        } => {
            let body = match file {
                Some(path) => std::fs::read(path),
                None => {
                    let mut body = Vec::new();
                    std::io::stdin().read_to_end(&mut body).map(|_| body)
                }
            }
            .map_err(|e| ClientError::transport(e.to_string()))?;
            client
                .put_schema(&name, content_type.as_deref(), Bytes::from(body))
                .await?;
            note(format!("registered {name}"));
        }
        SchemaCommand::Get { name } => {
            let Some(schema) = client.get_schema(&name).await? else {
                note("not found");
                return Ok(1);
            };
            std::io::stdout()
                .write_all(&schema.body)
                .map_err(|e| ClientError::transport(e.to_string()))?;
        }
        SchemaCommand::Rm { name } => {
            note(format!("deleted={}", client.delete_schema(&name).await?));
        }
    }

    Ok(0)
}
