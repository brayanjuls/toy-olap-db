use std::fmt::Write;
use std::sync::Arc;

use crate::binder::statement::BoundStatement;
use crate::catalog::database::DatabaseCatalog;
pub struct Executor{
    catalog: Arc<DatabaseCatalog>
}


impl Executor {
    pub fn new(catalog: Arc<DatabaseCatalog>) -> Executor{
        Executor { catalog: catalog }
    }
    pub fn execute(&self,stmt:&BoundStatement) -> Result<String,ExecuteError>{
        let mut output = String::new();
        match stmt {
            BoundStatement::Select(select) => {
                for value in &select.values{
                    write!(output,"{}",value).unwrap()
                }
            },
            BoundStatement::CreateTable(table) => {
                let current_schema = self.catalog.get_current_schema();
                let _ = current_schema.unwrap().add_table(table.table_name.as_str(), &table.columns);
                write!(output,"table {} created.",table.table_name.as_str()).unwrap()
            },
            _ => todo!("Unsupported statement")
        } 
        Ok(output.to_owned())               
    }
}

#[derive(thiserror::Error,Debug)]
pub enum ExecuteError{
    #[error("execution error {0}")]
    UnsupportedStatmentError(#[from] anyhow::Error)
}