use sqlparser::ast::{Expr, SelectItem, SetExpr, Statement};
use sqlparser::ast::Value::{SingleQuotedString,Number};
use std::fmt::Write;

pub fn execute(stmt:&Statement) -> Result<String,ExecuteError>{
        match stmt {
            Statement::Query(query) => match &query.body.as_ref() {
                SetExpr::Select(select) => {
                    let mut output = String::new();
                    for item in &select.projection{
                        match item {
                            SelectItem::UnnamedExpr(Expr::Value(value)) => match value {
                                SingleQuotedString(content) => write!(output,"{}",content).unwrap(),
                                Number(content,_) => {write!(output,"{}",content).unwrap()},
                                _ => todo!("not supprted statement")
                            },
                            _ =>  todo!("not supported statement")
                        }
                    }
                    Ok(output.to_owned())
                },
                _ =>  todo!("not supported statement")
            }
            _ => todo!("not supported statement")
        }
}

#[derive(thiserror::Error,Debug)]
pub enum ExecuteError{
    #[error("execution error {0}")]
    UnsupportedStatmentError(#[from] anyhow::Error)
}