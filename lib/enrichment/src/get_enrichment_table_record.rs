use std::collections::BTreeMap;

use vrl::prelude::*;

use crate::{
    vrl_util::{self, add_index, evaluate_condition},
    Case, Condition, IndexHandle, TableRegistry, TableSearch,
};

#[derive(Clone, Copy, Debug)]
pub struct GetEnrichmentTableRecord;
impl Function for GetEnrichmentTableRecord {
    fn identifier(&self) -> &'static str {
        "get_enrichment_table_record"
    }

    fn parameters(&self) -> &'static [Parameter] {
        &[
            Parameter {
                keyword: "table",
                kind: kind::BYTES,
                required: true,
            },
            Parameter {
                keyword: "condition",
                kind: kind::OBJECT,
                required: true,
            },
            Parameter {
                keyword: "select",
                kind: kind::ARRAY,
                required: false,
            },
            Parameter {
                keyword: "case_sensitive",
                kind: kind::BOOLEAN,
                required: false,
            },
        ]
    }

    fn examples(&self) -> &'static [Example] {
        &[Example {
            title: "find records",
            source: r#"get_enrichment_table_record!("test", {"id": 1})"#,
            result: Ok(r#"{"id": 1, "firstname": "Bob", "surname": "Smith"}"#),
        }]
    }

    fn compile(
        &self,
        _state: &state::Compiler,
        ctx: &mut FunctionCompileContext,
        mut arguments: ArgumentList,
    ) -> Compiled {
        let registry = ctx
            .get_external_context_mut::<TableRegistry>()
            .ok_or(Box::new(vrl_util::Error::TablesNotLoaded) as Box<dyn DiagnosticError>)?;

        let tables = registry
            .table_ids()
            .into_iter()
            .map(Value::from)
            .collect::<Vec<_>>();

        let table = arguments
            .required_enum("table", &tables)?
            .try_bytes_utf8_lossy()
            .expect("table is not valid utf8")
            .into_owned();
        let condition = arguments.required_object("condition")?;

        let select = arguments.optional("select");

        let case_sensitive = arguments
            .optional_literal("case_sensitive")?
            .map(|literal| literal.to_value().try_boolean())
            .transpose()
            .expect("case_sensitive should be boolean") // This will have been caught by the type checker.
            .map(|case_sensitive| {
                if case_sensitive {
                    Case::Sensitive
                } else {
                    Case::Insensitive
                }
            })
            .unwrap_or(Case::Sensitive);

        let index = Some(
            add_index(registry, &table, case_sensitive, &condition)
                .map_err(|err| Box::new(err) as Box<_>)?,
        );

        Ok(Box::new(GetEnrichmentTableRecordFn {
            table,
            condition,
            index,
            select,
            case_sensitive,
            enrichment_tables: registry.as_readonly(),
        }))
    }
}

#[derive(Debug, Clone)]
pub struct GetEnrichmentTableRecordFn {
    table: String,
    condition: BTreeMap<String, expression::Expr>,
    index: Option<IndexHandle>,
    select: Option<Box<dyn Expression>>,
    case_sensitive: Case,
    enrichment_tables: TableSearch,
}

impl Expression for GetEnrichmentTableRecordFn {
    fn resolve(&self, ctx: &mut Context) -> Resolved {
        let condition = self
            .condition
            .iter()
            .map(|(key, value)| evaluate_condition(ctx, key, value))
            .collect::<Result<Vec<Condition>>>()?;

        let select = self
            .select
            .as_ref()
            .map(|array| match array.resolve(ctx)? {
                Value::Array(arr) => arr
                    .iter()
                    .map(|value| Ok(value.try_bytes_utf8_lossy()?.to_string()))
                    .collect::<std::result::Result<Vec<_>, _>>(),
                value => Err(value::Error::Expected {
                    got: value.kind(),
                    expected: Kind::array(Collection::any()),
                }),
            })
            .transpose()?;

        let data = self.enrichment_tables.find_table_row(
            &self.table,
            self.case_sensitive,
            &condition,
            select.as_ref().map(|select| select.as_ref()),
            self.index,
        )?;

        Ok(Value::Object(data))
    }

    fn type_def(&self, _: &state::Compiler) -> TypeDef {
        TypeDef::object(Collection::any()).fallible()
    }
}

#[cfg(test)]
mod tests {
    use vector_common::{btreemap, TimeZone};

    use super::*;
    use crate::test_util::get_table_registry;

    #[test]
    fn find_table_row() {
        let registry = get_table_registry();
        let func = GetEnrichmentTableRecordFn {
            table: "dummy1".to_string(),
            condition: btreemap! {
                "field" =>  expression::Literal::from("value"),
            },
            index: Some(IndexHandle(999)),
            select: None,
            case_sensitive: Case::Sensitive,
            enrichment_tables: registry.as_readonly(),
        };

        let tz = TimeZone::default();
        let mut object: Value = BTreeMap::new().into();
        let mut runtime_state = vrl::state::Runtime::default();
        let mut ctx = Context::new(&mut object, &mut runtime_state, &tz);

        registry.finish_load();

        let got = func.resolve(&mut ctx);

        assert_eq!(Ok(value! ({ "field": "result" })), got);
    }
}
