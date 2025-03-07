use std::str::FromStr;

use chrono::{TimeZone as _, Utc};
use vector_common::{conversion::Conversion, TimeZone};
use vrl::{function::Error, prelude::*};

fn to_timestamp(value: Value, unit: Unit) -> Resolved {
    use Value::*;

    let value = match value {
        v @ Timestamp(_) => v,
        Integer(v) => match unit {
            Unit::Seconds => {
                let t = Utc.timestamp_opt(v, 0).single();
                match t {
                    Some(time) => time.into(),
                    None => return Err(format!("unable to coerce {} into timestamp", v).into()),
                }
            }
            Unit::Milliseconds => {
                let t = Utc.timestamp_millis_opt(v).single();
                match t {
                    Some(time) => time.into(),
                    None => return Err(format!("unable to coerce {} into timestamp", v).into()),
                }
            }
            Unit::Nanoseconds => Utc.timestamp_nanos(v).into(),
        },
        Float(v) => match unit {
            Unit::Seconds => {
                let t = Utc
                    .timestamp_opt(
                        v.trunc() as i64,
                        (v.fract() * 1_000_000_000.0).round() as u32,
                    )
                    .single();
                match t {
                    Some(time) => time.into(),
                    None => return Err(format!("unable to coerce {} into timestamp", v).into()),
                }
            }
            Unit::Milliseconds => {
                let t = Utc
                    .timestamp_opt(
                        (v.trunc() / 1_000.0) as i64,
                        (v.fract() * 1_000_000.0).round() as u32,
                    )
                    .single();
                match t {
                    Some(time) => time.into(),
                    None => return Err(format!("unable to coerce {} into timestamp", v).into()),
                }
            }
            Unit::Nanoseconds => {
                let t = Utc
                    .timestamp_opt(
                        (v.trunc() / 1_000_000_000.0) as i64,
                        v.fract().round() as u32,
                    )
                    .single();
                match t {
                    Some(time) => time.into(),
                    None => return Err(format!("unable to coerce {} into timestamp", v).into()),
                }
            }
        },
        Bytes(v) => Conversion::Timestamp(TimeZone::Local)
            .convert::<Value>(v)
            .map_err(|err| err.to_string())?,
        v => return Err(format!("unable to coerce {} into timestamp", v.kind()).into()),
    };
    Ok(value)
}

#[derive(Clone, Copy, Debug)]
pub struct ToTimestamp;

impl Function for ToTimestamp {
    fn identifier(&self) -> &'static str {
        "to_timestamp"
    }

    fn parameters(&self) -> &'static [Parameter] {
        &[
            Parameter {
                keyword: "value",
                kind: kind::ANY,
                required: true,
            },
            Parameter {
                keyword: "unit",
                kind: kind::BYTES,
                required: false,
            },
        ]
    }

    fn examples(&self) -> &'static [Example] {
        &[
            Example {
                title: "timestamp",
                source: "to_timestamp(t'2020-01-01T00:00:00Z')",
                result: Ok("t'2020-01-01T00:00:00Z'"),
            },
            Example {
                title: "integer as seconds",
                source: "to_timestamp!(5)",
                result: Ok("t'1970-01-01T00:00:05Z'"),
            },
            Example {
                title: "float as seconds",
                source: "to_timestamp!(5.6)",
                result: Ok("t'1970-01-01T00:00:05.600Z'"),
            },
            Example {
                title: "integer as milliseconds",
                source: r#"to_timestamp!(5000, unit: "milliseconds")"#,
                result: Ok("t'1970-01-01T00:00:05Z'"),
            },
            Example {
                title: "float as nanoseconds",
                source: r#"to_timestamp!(56000000000.7, unit: "nanoseconds")"#,
                result: Ok("t'1970-01-01T00:00:56.000000001Z'"),
            },
            Example {
                title: "string valid",
                source: "to_timestamp!(s'2020-01-01T00:00:00Z')",
                result: Ok("t'2020-01-01T00:00:00Z'"),
            },
            Example {
                title: "string invalid",
                source: "to_timestamp!(s'foo')",
                result: Err(
                    r#"function call error for "to_timestamp" at (0:21): No matching timestamp format found for "foo""#,
                ),
            },
            Example {
                title: "true",
                source: "to_timestamp!(true)",
                result: Err(
                    r#"function call error for "to_timestamp" at (0:19): unable to coerce boolean into timestamp"#,
                ),
            },
            Example {
                title: "false",
                source: "to_timestamp!(false)",
                result: Err(
                    r#"function call error for "to_timestamp" at (0:20): unable to coerce boolean into timestamp"#,
                ),
            },
            Example {
                title: "null",
                source: "to_timestamp!(null)",
                result: Err(
                    r#"function call error for "to_timestamp" at (0:19): unable to coerce null into timestamp"#,
                ),
            },
            Example {
                title: "array",
                source: "to_timestamp!([])",
                result: Err(
                    r#"function call error for "to_timestamp" at (0:17): unable to coerce array into timestamp"#,
                ),
            },
            Example {
                title: "object",
                source: "to_timestamp!({})",
                result: Err(
                    r#"function call error for "to_timestamp" at (0:17): unable to coerce object into timestamp"#,
                ),
            },
            Example {
                title: "regex",
                source: "to_timestamp!(r'foo')",
                result: Err(
                    r#"function call error for "to_timestamp" at (0:21): unable to coerce regex into timestamp"#,
                ),
            },
        ]
    }

    fn compile(
        &self,
        _state: &state::Compiler,
        _ctx: &mut FunctionCompileContext,
        mut arguments: ArgumentList,
    ) -> Compiled {
        let value = arguments.required("value");

        let unit = arguments
            .optional_enum("unit", Unit::all_value().as_slice())?
            .map(|s| {
                Unit::from_str(&s.try_bytes_utf8_lossy().expect("unit not bytes"))
                    .expect("validated enum")
            })
            .unwrap_or_default();

        Ok(Box::new(ToTimestampFn { value, unit }))
    }

    fn compile_argument(
        &self,
        _args: &[(&'static str, Option<FunctionArgument>)],
        _ctx: &FunctionCompileContext,
        name: &str,
        expr: Option<&expression::Expr>,
    ) -> CompiledArgument {
        match (name, expr) {
            ("unit", Some(expr)) => match expr.as_value() {
                None => Ok(None),
                Some(value) => {
                    let s = value.try_bytes_utf8_lossy().expect("unit not bytes");
                    Ok(Some(
                        Unit::from_str(&s)
                            .map(|unit| Box::new(unit) as Box<dyn std::any::Any + Send + Sync>)
                            .map_err(|_| Error::InvalidEnumVariant {
                                keyword: "unit",
                                value,
                                variants: Unit::all_value(),
                            })?,
                    ))
                }
            },
            _ => Ok(None),
        }
    }

    fn call_by_vm(&self, _ctx: &mut Context, args: &mut VmArgumentList) -> Resolved {
        let value = args.required("value");
        let unit = args
            .optional_any("unit")
            .map(|unit| *unit.downcast_ref::<Unit>().unwrap())
            .unwrap_or_default();

        to_timestamp(value, unit)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Unit {
    Seconds,
    Milliseconds,
    Nanoseconds,
}

impl Unit {
    fn all_value() -> Vec<Value> {
        use Unit::*;

        vec![Seconds, Milliseconds, Nanoseconds]
            .into_iter()
            .map(|u| u.as_str().into())
            .collect::<Vec<_>>()
    }

    const fn as_str(self) -> &'static str {
        use Unit::*;

        match self {
            Seconds => "seconds",
            Milliseconds => "milliseconds",
            Nanoseconds => "nanoseconds",
        }
    }
}

impl Default for Unit {
    fn default() -> Self {
        Unit::Seconds
    }
}

impl FromStr for Unit {
    type Err = &'static str;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        use Unit::*;

        match s {
            "seconds" => Ok(Seconds),
            "milliseconds" => Ok(Milliseconds),
            "nanoseconds" => Ok(Nanoseconds),
            _ => Err("unit not recognized"),
        }
    }
}

#[derive(Debug, Clone)]
struct ToTimestampFn {
    value: Box<dyn Expression>,
    unit: Unit,
}

impl Expression for ToTimestampFn {
    fn resolve(&self, ctx: &mut Context) -> Resolved {
        let value = self.value.resolve(ctx)?;
        let unit = self.unit;

        to_timestamp(value, unit)
    }

    fn type_def(&self, state: &state::Compiler) -> TypeDef {
        self.value
            .type_def(state)
            .fallible_unless(Kind::timestamp())
            .with_kind(Kind::timestamp())
    }
}

#[cfg(test)]
#[allow(overflowing_literals)]
mod tests {
    use std::collections::BTreeMap;

    use vector_common::TimeZone;
    use vrl::prelude::expression::Literal;

    use super::*;

    #[test]
    fn out_of_range_integer() {
        let mut object: Value = BTreeMap::new().into();
        let mut runtime_state = vrl::state::Runtime::default();
        let tz = TimeZone::default();
        let mut ctx = Context::new(&mut object, &mut runtime_state, &tz);
        let f = ToTimestampFn {
            value: Box::new(Literal::Integer(9999999999999)),
            unit: Unit::default(),
        };
        let string = f.resolve(&mut ctx).err().unwrap().message();
        assert_eq!(string, r#"unable to coerce 9999999999999 into timestamp"#)
    }

    #[test]
    fn out_of_range_float() {
        let mut object: Value = BTreeMap::new().into();
        let mut runtime_state = vrl::state::Runtime::default();
        let tz = TimeZone::default();
        let mut ctx = Context::new(&mut object, &mut runtime_state, &tz);
        let f = ToTimestampFn {
            value: Box::new(Literal::Float(NotNan::new(9999999999999.9).unwrap())),
            unit: Unit::default(),
        };
        let string = f.resolve(&mut ctx).err().unwrap().message();
        assert_eq!(string, r#"unable to coerce 9999999999999.9 into timestamp"#)
    }

    test_function![
        to_timestamp => ToTimestamp;

        integer {
             args: func_args![value: 1431648000],
             want: Ok(chrono::Utc.ymd(2015, 5, 15).and_hms(0, 0, 0)),
             tdef: TypeDef::timestamp().fallible(),
        }

        integer_seconds {
            args: func_args![value: 1609459200i64, unit: "seconds"],
            want: Ok(chrono::Utc.ymd(2021, 1, 1).and_hms_milli(0,0,0,0)),
            tdef: TypeDef::timestamp().fallible(),
        }

        integer_milliseconds {
            args: func_args![value: 1609459200000i64, unit: "milliseconds"],
            want: Ok(chrono::Utc.ymd(2021, 1, 1).and_hms_milli(0,0,0,0)),
            tdef: TypeDef::timestamp().fallible(),
        }

        integer_nanoseconds {
            args: func_args![value: 1609459200000000000i64, unit: "nanoseconds"],
            want: Ok(chrono::Utc.ymd(2021, 1, 1).and_hms_milli(0,0,0,0)),
            tdef: TypeDef::timestamp().fallible(),
        }

        float {
            args: func_args![value: 1431648000.5],
            want: Ok(chrono::Utc.ymd(2015, 5, 15).and_hms_milli(0, 0, 0, 500)),
            tdef: TypeDef::timestamp().fallible(),
       }

        float_seconds {
            args: func_args![value: 1609459200.0f64, unit: "seconds"],
            want: Ok(chrono::Utc.ymd(2021, 1, 1).and_hms_milli(0,0,0,0)),
            tdef: TypeDef::timestamp().fallible(),
        }

        float_milliseconds {
            args: func_args![value: 1609459200000.0f64, unit: "milliseconds"],
            want: Ok(chrono::Utc.ymd(2021, 1, 1).and_hms_milli(0,0,0,0)),
            tdef: TypeDef::timestamp().fallible(),
        }

        float_nanoseconds {
            args: func_args![value: 1609459200000000000.0f64, unit: "nanoseconds"],
            want: Ok(chrono::Utc.ymd(2021, 1, 1).and_hms_milli(0,0,0,0)),
            tdef: TypeDef::timestamp().fallible(),
        }
    ];
}
