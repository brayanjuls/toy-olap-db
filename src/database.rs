use crate::{executor::{self, ExecuteError}, parser};
use sqlparser::parser::ParserError;


pub struct Database{}

impl Database {
    pub fn new() -> Self {
        Database{}
    }
    pub fn run(&self,sql:&str) -> Result<Vec<String>,Error>{
        let stmts = parser::parse(sql)?;
        let mut outputs = Vec::<String>::new();
        for stmt in stmts{
            let result = executor::execute(&stmt)?;
            outputs.push(result);
        }
        Ok(outputs)
    }
}



#[derive(thiserror::Error, Debug)]
pub enum Error{
    #[error("parser error: {0}")]
    Parse(#[from] ParserError),

    #[error("execute error: {0}")]
    Execute(#[from] ExecuteError)
}
