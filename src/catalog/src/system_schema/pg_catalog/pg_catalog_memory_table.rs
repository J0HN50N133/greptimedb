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

use std::sync::Arc;

use datatypes::scalars::ScalarVector;
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use datatypes::type_id::LogicalTypeId;
use datatypes::vectors::{Int16Vector, StringVector, UInt32Vector, VectorRef};

use super::oid_column;
use super::table_names::PG_TYPE;
use crate::memory_table_cols;
use crate::system_schema::utils::tables::{i16_column, string_column};

mod pg_type {
    use datatypes::type_id::LogicalTypeId;

    pub type PGTypeRow = (
        u32,          /* oid */
        &'static str, /*typname*/
        i16,          /*typlen*/
        &'static str, /*typcategory*/
    );

    pub const fn oid_of_type(id: LogicalTypeId) -> u32 {
        match id {
            LogicalTypeId::String => 1,
            LogicalTypeId::Binary => 2,
            LogicalTypeId::Int8 => 3,
            LogicalTypeId::Int16 => 4,
            LogicalTypeId::Int32 => 5,
            LogicalTypeId::Int64 => 6,
            LogicalTypeId::UInt8 => 7,
            LogicalTypeId::UInt16 => 8,
            LogicalTypeId::UInt32 => 9,
            LogicalTypeId::UInt64 => 10,
            LogicalTypeId::Float32 => 11,
            LogicalTypeId::Float64 => 12,
            LogicalTypeId::Decimal128 => 13,
            LogicalTypeId::Date => 14,
            LogicalTypeId::DateTime => 15,
            LogicalTypeId::TimestampMillisecond => 16,
            LogicalTypeId::TimestampMicrosecond => 17,
            LogicalTypeId::TimestampNanosecond => 18,
            LogicalTypeId::TimestampSecond => 19,
            LogicalTypeId::TimeSecond => 20,
            LogicalTypeId::TimeMillisecond => 21,
            LogicalTypeId::TimeMicrosecond => 22,
            LogicalTypeId::TimeNanosecond => 17,
            LogicalTypeId::DurationSecond => 18,
            LogicalTypeId::DurationMillisecond => 19,
            LogicalTypeId::DurationMicrosecond => 20,
            LogicalTypeId::DurationNanosecond => 21,
            LogicalTypeId::IntervalYearMonth => 22,
            LogicalTypeId::IntervalDayTime => 23,
            LogicalTypeId::IntervalMonthDayNano => 24,
            LogicalTypeId::List => 25,
            LogicalTypeId::Boolean => 26,
            LogicalTypeId::Null => 27,
            LogicalTypeId::Dictionary => 28,
        }
    }

    pub const fn row(id: LogicalTypeId) -> PGTypeRow {
        const LEN_UNBOUND: i16 = -1;

        // refer to https://www.postgresql.org/docs/current/catalog-pg-type.html
        const _A: &'static str = "A"; // Array types
        const B: &'static str = "B"; // Boolean types
        const _C: &'static str = "C"; // Composite types
        const D: &'static str = "D"; // Date/time types
        const _E: &'static str = "E"; // Enum types
        const _G: &'static str = "G"; // Geometric types
        const _I: &'static str = "I"; // Network address types
        const N: &'static str = "N"; // Numeric types
        const P: &'static str = "P"; // Pseudo-types
        const _R: &'static str = "R"; // Range types
        const S: &'static str = "S"; // String types
        const T: &'static str = "T"; // Timespan types
        const U: &'static str = "U"; // User-defined types
        const _V: &'static str = "V"; // Bit-string types
        const _X: &'static str = "X"; // Unknown type(no category)
        const _Z: &'static str = "Z"; // Internal-use types

        let oid: u32 = oid_of_type(id);
        match id {
            LogicalTypeId::String => (oid, "String", LEN_UNBOUND, S),
            LogicalTypeId::Binary => (oid, "Binary", LEN_UNBOUND, U), // align to bytea
            LogicalTypeId::Int8 => (oid, "Int8", 1, N),
            LogicalTypeId::Int16 => (oid, "Int16", 2, N),
            LogicalTypeId::Int32 => (oid, "Int32", 4, N),
            LogicalTypeId::Int64 => (oid, "Int64", 8, N),
            LogicalTypeId::UInt8 => (oid, "UInt8", 1, N),
            LogicalTypeId::UInt16 => (oid, "UInt16", 2, N),
            LogicalTypeId::UInt32 => (oid, "UInt32", 4, N),
            LogicalTypeId::UInt64 => (oid, "UInt64", 8, N),
            LogicalTypeId::Float32 => (oid, "Float32", 4, N),
            LogicalTypeId::Float64 => (oid, "Float64", 8, N),
            LogicalTypeId::Decimal128 => (oid, "Decimal", 16, N),
            LogicalTypeId::Date => (oid, "Date", 4, D),
            LogicalTypeId::DateTime => (oid, "DateTime", 8, D),
            LogicalTypeId::TimestampMillisecond => (oid, "TimestampMillisecond", 8, D),
            LogicalTypeId::TimestampMicrosecond => (oid, "TimestampMicrosecond", 8, D),
            LogicalTypeId::TimestampNanosecond => (oid, "TimestampNanosecond", 8, D),
            LogicalTypeId::TimestampSecond => (oid, "TimestampSecond", 8, D),
            LogicalTypeId::TimeSecond => (oid, "TimeSecond", 8, T),
            LogicalTypeId::TimeMillisecond => (oid, "TimeMillisecond", 8, T),
            LogicalTypeId::TimeMicrosecond => (oid, "TimeMicrosecond", 8, T),
            LogicalTypeId::TimeNanosecond => (oid, "TimeNanosecond", 8, T),
            LogicalTypeId::DurationSecond => (oid, "DurationSecond", 8, T),
            LogicalTypeId::DurationMillisecond => (oid, "DurationMillisecond", 8, T),
            LogicalTypeId::DurationMicrosecond => (oid, "DurationMicrosecond", 8, T),
            LogicalTypeId::DurationNanosecond => (oid, "DurationNanosecond", 8, T),
            LogicalTypeId::IntervalYearMonth => (oid, "IntervalYearMonth", 16, T),
            LogicalTypeId::IntervalDayTime => (oid, "IntervalDayTime", 16, T),
            LogicalTypeId::IntervalMonthDayNano => (oid, "IntervalMonthDayNano", 16, T),
            LogicalTypeId::Boolean => (oid, "Boolean", LEN_UNBOUND, B),
            LogicalTypeId::Null => (oid, "Null", 0, P),
            LogicalTypeId::List => (oid, "List", LEN_UNBOUND, U),
            /* Specially Dictionary is a generic type */
            LogicalTypeId::Dictionary => (oid, "Dictionary", LEN_UNBOUND, U),
        }
    }
}

fn pg_type_schema_columns() -> (Vec<ColumnSchema>, Vec<VectorRef>) {
    memory_table_cols!(
        [oid, typname, typlen, typcategory],
        [
            pg_type::row(LogicalTypeId::String),
            pg_type::row(LogicalTypeId::Binary),
            pg_type::row(LogicalTypeId::Int8),
            pg_type::row(LogicalTypeId::Int16),
            pg_type::row(LogicalTypeId::Int32),
            pg_type::row(LogicalTypeId::Int64),
            pg_type::row(LogicalTypeId::UInt8),
            pg_type::row(LogicalTypeId::UInt16),
            pg_type::row(LogicalTypeId::UInt32),
            pg_type::row(LogicalTypeId::UInt64),
            pg_type::row(LogicalTypeId::Float32),
            pg_type::row(LogicalTypeId::Float64),
            pg_type::row(LogicalTypeId::Decimal128),
            pg_type::row(LogicalTypeId::Date),
            pg_type::row(LogicalTypeId::DateTime),
            pg_type::row(LogicalTypeId::TimestampMillisecond),
            pg_type::row(LogicalTypeId::TimestampMicrosecond),
            pg_type::row(LogicalTypeId::TimestampNanosecond),
            pg_type::row(LogicalTypeId::TimestampSecond),
            pg_type::row(LogicalTypeId::TimeSecond),
            pg_type::row(LogicalTypeId::TimeMillisecond),
            pg_type::row(LogicalTypeId::TimeMicrosecond),
            pg_type::row(LogicalTypeId::TimeNanosecond),
            pg_type::row(LogicalTypeId::DurationSecond),
            pg_type::row(LogicalTypeId::DurationMillisecond),
            pg_type::row(LogicalTypeId::DurationMicrosecond),
            pg_type::row(LogicalTypeId::DurationNanosecond),
            pg_type::row(LogicalTypeId::IntervalYearMonth),
            pg_type::row(LogicalTypeId::IntervalDayTime),
            pg_type::row(LogicalTypeId::IntervalMonthDayNano),
            pg_type::row(LogicalTypeId::Boolean),
            pg_type::row(LogicalTypeId::Null),
            pg_type::row(LogicalTypeId::List),
            pg_type::row(LogicalTypeId::Dictionary),
        ]
    );
    (
        // not quiet identical with pg, we only follow the definition in pg
        vec![
            oid_column(),
            string_column("typname"),
            i16_column("typlen"),
            string_column("typcategory"),
        ],
        vec![
            Arc::new(UInt32Vector::from_vec(oid)), // oid
            Arc::new(StringVector::from(typname)),
            Arc::new(Int16Vector::from_vec(typlen)), // typlen in bytes
            Arc::new(StringVector::from_vec(typcategory)),
        ],
    )
}

pub(super) fn get_schema_columns(table_name: &str) -> (SchemaRef, Vec<VectorRef>) {
    let (column_schemas, columns): (_, Vec<VectorRef>) = match table_name {
        PG_TYPE => pg_type_schema_columns(),
        _ => unreachable!("Unknown table in pg_catalog: {}", table_name),
    };
    (Arc::new(Schema::new(column_schemas)), columns)
}
