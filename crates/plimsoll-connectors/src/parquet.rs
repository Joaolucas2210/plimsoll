//! Read-only Parquet connector on top of DataFusion's `ListingTable`.

use std::sync::Arc;

use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use plimsoll_core::federation::{Pushdown, PushdownPolicy};

use crate::{Connector, ConnectorError};

#[derive(Debug)]
pub struct ParquetConnector {
    name: String,
    table: Arc<ListingTable>,
    policy: PushdownPolicy,
}

impl ParquetConnector {
    /// `path` is a file or a directory (trailing `/`) of `.parquet` files, local
    /// or any URL whose object store is registered in the session.
    pub async fn try_new(
        name: impl Into<String>,
        path: &str,
        state: &dyn Session,
    ) -> Result<Self, ConnectorError> {
        let url = ListingTableUrl::parse(path)?;
        let options =
            ListingOptions::new(Arc::new(ParquetFormat::default())).with_file_extension(".parquet");
        let config = ListingTableConfig::new(url)
            .with_listing_options(options)
            .infer_schema(state)
            .await?;
        Ok(Self {
            name: name.into(),
            table: Arc::new(ListingTable::try_new(config)?),
            // DataFusion reads the files itself, but row-group/page pruning is a
            // hint: every filter may be pushed and every filter is re-checked.
            policy: PushdownPolicy::uniform(Pushdown::Inexact),
        })
    }
}

impl Connector for ParquetConnector {
    fn name(&self) -> &str {
        &self.name
    }
    fn policy(&self) -> &PushdownPolicy {
        &self.policy
    }
    fn source(&self) -> Arc<dyn TableProvider> {
        self.table.clone()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::prelude::{SessionContext, col, lit};

    fn write_parquet() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "plimsoll-parquet-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("city", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec![
                    "Lisboa", "Porto", "Recife", "Natal",
                ])),
            ],
        )
        .unwrap();
        let file = std::fs::File::create(dir.join("part-0.parquet")).unwrap();
        let mut w = ArrowWriter::try_new(file, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        dir
    }

    #[tokio::test]
    async fn parquet_connector_scans_and_filters() {
        let dir = write_parquet();
        let ctx = SessionContext::new();
        let path = format!("{}/", dir.display());
        let conn = ParquetConnector::try_new("cities", &path, &ctx.state())
            .await
            .unwrap();
        conn.register(&ctx).unwrap();

        let batches = ctx
            .table("cities")
            .await
            .unwrap()
            .filter(col("id").gt_eq(lit(3i64)))
            .unwrap()
            .select_columns(&["city"])
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut got: Vec<String> = batches
            .iter()
            .flat_map(|b| {
                // Parquet strings come back as Utf8View; normalise for the assert.
                let a = cast(b.column(0), &DataType::Utf8).unwrap();
                let a = a.as_any().downcast_ref::<StringArray>().unwrap();
                a.iter().map(|v| v.unwrap().to_string()).collect::<Vec<_>>()
            })
            .collect();
        got.sort();
        assert_eq!(got, ["Natal", "Recife"]);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
