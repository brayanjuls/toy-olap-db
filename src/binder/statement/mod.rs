use std::sync::Arc;


use create::BoundCreateTable;
use select::BoundSelect;
use sqlparser::ast::{Query, Statement};

use crate::catalog::database::DatabaseCatalog;

mod create;
mod select;

#[derive(Debug,Clone,PartialEq)]
pub enum BoundStatement{
    CreateTable(BoundCreateTable),
    Select(BoundSelect)
}

pub struct Binder{
    catalog:Arc<DatabaseCatalog>
}

impl Binder {
    pub fn new(catalog:Arc<DatabaseCatalog>) -> Binder {
        Binder { catalog: catalog }
    }

    pub fn bind(&mut self, stmt:&Statement) -> Result<BoundStatement,BindError>{
        use Statement::*;
        match &stmt {
            CreateTable {..} => Ok(BoundStatement::CreateTable(self.bind_create_table(stmt)?)),
            Query(query) => Ok(BoundStatement::Select(self.bind_select(query)?)),
            _ => Err(BindError::NotSupportedStatement)
        }
    }

    pub fn bind_create_table(&mut self, stmt:&Statement) -> Result<BoundCreateTable,BindError>{
        BoundCreateTable::create_table(Arc::clone(&self.catalog), &stmt)
    }

    pub fn bind_select(&mut self, query:&Query) -> Result<BoundSelect,BindError>{
        BoundSelect::select(query)
    }
}

#[derive(thiserror::Error,Debug)]
pub enum BindError{
    #[error("Statement not supported")]
    NotSupportedStatement,
    #[error("")]
    Default,
    #[error("Unable to get current schema")]
    UnableToGetCurrentSchemaCatalog,
    #[error("Schema not found in the database {0}")]
    SchemaNotFound(String),

    #[error("Table Already Exists")]
    TableAlreadyExist

}