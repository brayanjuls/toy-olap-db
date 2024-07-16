use std::sync::Arc;

use crate::{binder::statement::{BindError, Binder}, catalog::database::DatabaseCatalog, executor::{self, ExecuteError, Executor}, parser};
use sqlparser::parser::ParserError;


pub struct Database{
    database_catalog:Arc<DatabaseCatalog>
}

impl Database {
    pub fn new() -> Self {
        Database{
            database_catalog: Arc::new(DatabaseCatalog::new())
        }
    }
    pub fn run_d(&self,sql:&str) -> Result<Vec<String>,Error>{
        let stmts = parser::parse(sql)?;
        let mut outputs = Vec::<String>::new();
        let mut binder = Binder::new(Arc::clone(&self.database_catalog));
        let executor = Executor::new(Arc::clone(&self.database_catalog));
        for stmt in stmts{
            let bind_stmt = binder.bind(&stmt)?;
            let result = executor.execute(&bind_stmt)?;
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
    Execute(#[from] ExecuteError),

    #[error("bind error: {0}")]
    Bind(#[from] BindError)
}
