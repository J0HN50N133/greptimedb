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

use std::fmt::{self};
use std::sync::Arc;

use common_query::error::Result;
use common_query::prelude::{Signature, Volatility};
use datatypes::data_type::DataType;
use datatypes::prelude::{ConcreteDataType, VectorRef};
use datatypes::types::LogicalPrimitiveType;
use datatypes::with_match_primitive_type_id;
use num_traits::AsPrimitive;

use crate::function::{Function, FunctionContext};
use crate::scalars::expression::{scalar_unary_op, EvalContext};

// pg_catalog.pg_encoding_to_char refers to: https://github.com/postgres/postgres/blob/364de74cff281e7363c7ca8de4fbf04c6e16f8ed/src/common/encnames.c#L588
#[derive(Clone, Debug, Default)]
pub struct PGEncodingToCharFunction;

const NAME: &str = crate::pg_catalog_func_fullname!("pg_encoding_to_char");

impl fmt::Display for PGEncodingToCharFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, crate::pg_catalog_func_fullname!("PG_ENCODING_TO_CHAR"))
    }
}

impl Function for PGEncodingToCharFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::string_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::uniform(
            1,
            vec![ConcreteDataType::uint32_datatype()],
            Volatility::Immutable,
        )
    }

    fn eval(&self, _func_ctx: FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        with_match_primitive_type_id!(columns[0].data_type().logical_type_id(), |$T| {
            let col = scalar_unary_op::<<$T as LogicalPrimitiveType>::Native, String, _>(&columns[0], pg_encoding_to_char, &mut EvalContext::default())?;
            Ok(Arc::new(col))
        }, {
            unreachable!()
        })
    }
}

fn pg_encoding_to_char<I>(encoding: Option<I>, _ctx: &mut EvalContext) -> Option<String>
where
    I: AsPrimitive<u32>,
{
    // TODO(J0HN50N133): We lack way to get the user_info by a numeric value. Once we have it, we can implement this function.
    const PG_UTF8: u32 = 6;
    encoding.map(|encoding| match encoding.as_() {
        PG_UTF8 => "UTF8".to_string(),
        _ => "Unknown".to_string(),
    })
}
