// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::{Arc, Weak};

use arrow_schema::SchemaRef as ArrowSchemaRef;
use common_catalog::consts::PG_CATALOG_PG_DATABASE_TABLE_ID;
use common_error::ext::BoxedError;
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use common_recordbatch::{DfSendableRecordBatchStream, RecordBatch};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter as DfRecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream as DfPartitionStream;
use datatypes::scalars::{Scalar, ScalarVectorBuilder};
use datatypes::schema::{Schema, SchemaRef};
use datatypes::vectors::{ListVectorBuilder, StringVectorBuilder, UInt32VectorBuilder, VectorRef};
use snafu::{OptionExt, ResultExt};
use store_api::storage::{ConcreteDataType, ScanRequest};

use super::PG_DATABASE;
use crate::error::{
    CreateRecordBatchSnafu, InternalSnafu, Result, UpgradeWeakCatalogManagerRefSnafu,
};
use crate::system_schema::utils::tables::{list_column, string_column, u32_column};
use crate::system_schema::SystemTable;
use crate::CatalogManager;

// === column name ===
pub const DATNAME: &str = "datname";
pub const DATCOLLATE: &str = "relnamespace";
pub const DATCTYPE: &str = "datctype"; // LC_CTYPE of database, we don't actually support this
pub const DATACL: &str = "datacl";
pub const DATDBA: &str = "datdba";
pub const ENCODING: &str = "encoding";

/// The initial capacity of the vector builders.
const INIT_CAPACITY: usize = 42;
/// The dummy dba id for the database.
const DUMMY_DBA_ID: u32 = 0;
const DUMMY_CTYPE: &str = "";
const UTF8_COLLATE_NAME: &str = "utf8_bin";

mod encoding {
    // pg_enc value
    // reference: https://github.com/postgres/postgres/blob/364de74cff281e7363c7ca8de4fbf04c6e16f8ed/src/include/mb/pg_wchar.h#L248
    pub const PG_UTF8: u32 = 6; // TODO: map this to "UTF8"
                                // pg_catalog.pg_encoding_to_char refers to: https://github.com/postgres/postgres/blob/364de74cff281e7363c7ca8de4fbf04c6e16f8ed/src/common/encnames.c#L588
}

mod datacl {
    use std::sync::LazyLock;

    use datatypes::value::{ListValue, Value};
    use store_api::storage::ConcreteDataType;

    type ACLItem = &'static str;
    // refer to: https://www.postgresql.org/docs/current/ddl-priv.html
    // also: system_schema/information_schema/columns.rs:87 to align the default value
    pub const ACL_SELECT: ACLItem = "r";
    pub const ACL_INSERT: ACLItem = "a";
    pub static DEFAULT_PRIVILEGES: LazyLock<ListValue> = LazyLock::new(|| {
        ListValue::new(
            vec![Value::from(ACL_SELECT), Value::from(ACL_INSERT)],
            ConcreteDataType::string_datatype(),
        )
    });
}

/// The `pg_catalog.pg_database` table implementation.
pub(super) struct PGDatabase {
    schema: SchemaRef,
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,
}

impl PGDatabase {
    pub(super) fn new(catalog_name: String, catalog_manager: Weak<dyn CatalogManager>) -> Self {
        Self {
            schema: Self::schema(),
            catalog_name,
            catalog_manager,
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            string_column(DATNAME),
            u32_column(DATDBA),
            u32_column(ENCODING),
            string_column(DATCTYPE),
            string_column(DATCOLLATE),
            list_column(DATACL, ConcreteDataType::string_datatype()),
        ]))
    }

    fn builder(&self) -> PGDatabaseBuilder {
        PGDatabaseBuilder::new(
            self.schema.clone(),
            self.catalog_name.clone(),
            self.catalog_manager.clone(),
        )
    }
}

impl SystemTable for PGDatabase {
    fn table_id(&self) -> table::metadata::TableId {
        PG_CATALOG_PG_DATABASE_TABLE_ID
    }

    fn table_name(&self) -> &'static str {
        PG_DATABASE
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn to_stream(
        &self,
        request: ScanRequest,
    ) -> Result<common_recordbatch::SendableRecordBatchStream> {
        let schema = self.schema.arrow_schema().clone();
        let mut builder = self.builder();
        let stream = Box::pin(DfRecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move {
                builder
                    .make_database(Some(request))
                    .await
                    .map(|x| x.into_df_record_batch())
                    .map_err(Into::into)
            }),
        ));
        Ok(Box::pin(
            RecordBatchStreamAdapter::try_new(stream)
                .map_err(BoxedError::new)
                .context(InternalSnafu)?,
        ))
    }
}

impl DfPartitionStream for PGDatabase {
    fn schema(&self) -> &ArrowSchemaRef {
        self.schema.arrow_schema()
    }

    fn execute(&self, _: Arc<TaskContext>) -> DfSendableRecordBatchStream {
        let schema = self.schema.arrow_schema().clone();
        let mut builder = self.builder();
        Box::pin(DfRecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move {
                builder
                    .make_database(None)
                    .await
                    .map(|x| x.into_df_record_batch())
                    .map_err(Into::into)
            }),
        ))
    }
}

/// Builds the `pg_catalog.pg_database` table row by row
struct PGDatabaseBuilder {
    schema: SchemaRef,
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,

    datname: StringVectorBuilder,
    datdba: UInt32VectorBuilder,
    encoding: UInt32VectorBuilder,
    datctype: StringVectorBuilder,
    datcollate: StringVectorBuilder,
    datacl: ListVectorBuilder,
}

impl PGDatabaseBuilder {
    fn new(
        schema: SchemaRef,
        catalog_name: String,
        catalog_manager: Weak<dyn CatalogManager>,
    ) -> Self {
        Self {
            schema,
            catalog_name,
            catalog_manager,

            datname: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            datdba: UInt32VectorBuilder::with_capacity(INIT_CAPACITY),
            encoding: UInt32VectorBuilder::with_capacity(INIT_CAPACITY),
            datctype: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            datcollate: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            datacl: ListVectorBuilder::with_type_capacity(
                ConcreteDataType::string_datatype(),
                INIT_CAPACITY,
            ),
        }
    }

    /// Construct the `pg_catalog.pg_database` virtual table
    async fn make_database(&mut self, _request: Option<ScanRequest>) -> Result<RecordBatch> {
        let catalog_manager = self
            .catalog_manager
            .upgrade()
            .context(UpgradeWeakCatalogManagerRefSnafu)?;
        for schema_name in &catalog_manager.schema_names(&self.catalog_name).await? {
            self.add_database(schema_name);
        }
        self.finish()
    }

    fn add_database(&mut self, database_name: &str) {
        self.datname.push(Some(database_name));
        self.datdba.push(Some(DUMMY_DBA_ID));
        self.encoding.push(Some(encoding::PG_UTF8));
        self.datctype.push(Some(DUMMY_CTYPE));
        self.datcollate.push(Some(UTF8_COLLATE_NAME));
        self.datacl
            .push(Some(datacl::DEFAULT_PRIVILEGES.as_scalar_ref()));
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        let columns: Vec<VectorRef> = vec![
            Arc::new(self.datname.finish()),
            Arc::new(self.datdba.finish()),
            Arc::new(self.encoding.finish()),
            Arc::new(self.datctype.finish()),
            Arc::new(self.datcollate.finish()),
            Arc::new(self.datacl.finish()),
        ];
        RecordBatch::new(self.schema.clone(), columns).context(CreateRecordBatchSnafu)
    }
}
