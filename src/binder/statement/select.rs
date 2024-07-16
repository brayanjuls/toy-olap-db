

use sqlparser::ast::{Expr, Query, SelectItem, SetExpr, Value};

use super::BindError;

#[derive(Debug,PartialEq,Clone)]
pub struct BoundSelect{
    pub values:Vec<Value>
}

impl BoundSelect {
    pub fn select(query:&Query) -> Result<BoundSelect,BindError>{
        match &query.body.as_ref() {
            SetExpr::Select(select) => {
                let mut values = vec![];
                for item in &select.projection{
                    match item {
                        SelectItem::UnnamedExpr(Expr::Value(v)) => values.push(v.to_owned()),
                        _ => todo!("")
                    }
                }
                Ok(BoundSelect { values: values })
            },
            _ => Err(BindError::NotSupportedStatement)
        }
    }
}