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
use datatypes::prelude::{ConcreteDataType, DataType, VectorRef};
use datatypes::scalars::ScalarRef;
use datatypes::type_id::LogicalTypeId;
use datatypes::value::{ListValue, ListValueRef};

use crate::function::{Function, FunctionContext};
use crate::scalars::expression::{scalar_binary_op, EvalContext};
#[derive(Clone, Debug, Default)]
pub struct PGArrayToStringFunction;

const NAME: &str = crate::pg_catalog_func_fullname!("pg_array_to_string");

impl fmt::Display for PGArrayToStringFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, crate::pg_catalog_func_fullname!("PG_ARRAY_TO_STRING"))
    }
}

impl Function for PGArrayToStringFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::string_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::exact(
            vec![
                ConcreteDataType::list_datatype(ConcreteDataType::string_datatype()),
                ConcreteDataType::string_datatype(),
            ],
            Volatility::Immutable,
        )
    }

    fn eval(&self, _func_ctx: FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        match columns[0].data_type().logical_type_id() {
            LogicalTypeId::List => {
                let col = scalar_binary_op::<ListValue, String, String, _>(
                    &columns[0],
                    &columns[1],
                    array_to_string,
                    &mut EvalContext::default(),
                )?;
                Ok(Arc::new(col))
            }
            _ => unreachable!(),
        }
    }
}

fn array_to_string(
    acl_items: Option<ListValueRef>,
    seperator: Option<&str>,
    _ctx: &mut EvalContext,
) -> Option<String> {
    let seperator = seperator.unwrap_or("\n");
    acl_items.map(|l| {
        // TODO: avoid the value clone if possible
        let l = l.to_owned_scalar();
        l.items()
            .iter()
            .enumerate()
            .fold(String::new(), |mut s, (i, v)| {
                if i != 0 {
                    s.push_str(seperator)
                }
                if let Some(v) = v.as_string() {
                    s.push_str(&v)
                }
                s
            })
    })
}

#[cfg(test)]
mod tests {
    use common_query::prelude::TypeSignature;
    use datatypes::scalars::{Scalar, ScalarVector, ScalarVectorBuilder};
    use datatypes::value::Value;
    use datatypes::vectors::{ConstantVector, ListVectorBuilder, StringVector, Vector};
    use session::context::QueryContextBuilder;

    use super::*;

    #[test]
    fn test_array_to_string_function() {
        let pg_array_to_string = PGArrayToStringFunction;
        let string_list_type = ConcreteDataType::list_datatype(ConcreteDataType::string_datatype());

        assert_eq!("pg_catalog.pg_array_to_string", pg_array_to_string.name());
        assert_eq!(
            ConcreteDataType::string_datatype(),
            pg_array_to_string.return_type(&[]).unwrap()
        );
        assert!(matches!(
            pg_array_to_string.signature(),
            Signature {
                type_signature: TypeSignature::Exact(v),
                volatility: Volatility::Immutable
            } if v == [
                    string_list_type.clone(),
                    ConcreteDataType::string_datatype(),
            ]
        ));

        let query_ctx = QueryContextBuilder::default()
            .current_schema("whatever".to_string())
            .build()
            .into();

        let func_ctx = FunctionContext {
            query_ctx,
            ..Default::default()
        };

        macro_rules! str_list {
            ($($t:tt),*) => {
                {
                    let v:Vec<&str> = vec![$($t),*];
                    ListValue::new(
                        v.iter().map(|&s| Value::from(s)).collect(),
                        ConcreteDataType::string_datatype(),
                    )
                }
            };
        }

        let v = vec![
            str_list!["a", "b", "c"],
            str_list![],
            str_list![""],
            str_list!["", ""],
        ];
        let mut list_vector_builder =
            ListVectorBuilder::with_type_capacity(ConcreteDataType::string_datatype(), v.len());
        for i in &v {
            list_vector_builder.push(Some(i.as_scalar_ref()));
        }
        let list_vector = list_vector_builder.finish();
        let seperator = ConstantVector::new(
            Arc::new(StringVector::from_vec(vec![", "])),
            list_vector.len(),
        );
        let vector = pg_array_to_string
            .eval(func_ctx, &[Arc::new(list_vector), Arc::new(seperator)])
            .unwrap();
        let expect: VectorRef = Arc::new(StringVector::from(vec!["a, b, c", "", "", ", "]));
        assert_eq!(expect, vector);
    }
}
