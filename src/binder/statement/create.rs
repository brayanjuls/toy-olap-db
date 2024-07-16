use std::sync::Arc;
use sqlparser::ast::{DataType, Statement,Statement::CreateTable};

use crate::catalog::{column::{ColumnDesc, ColumnDescBuilder}, database::DatabaseCatalog, schema::{SchemaCatalog, SchemaId}};

use super::BindError;


#[derive(Debug,PartialEq,Clone)]
pub struct BoundCreateTable{
    pub schema_id: SchemaId,
    pub table_name: String,
    pub columns: Vec<(String, ColumnDesc)>
}

impl BoundCreateTable {
    pub fn create_table(db_catalog:Arc<DatabaseCatalog>, statement:&Statement) -> Result<BoundCreateTable,BindError>{
     
        match statement {
            CreateTable { name, columns, .. } => {
                let columns_desc = &columns
                .into_iter()
                .map(|c|(c.name.to_string(),ColumnDescBuilder::new().datatype(c.data_type.clone()).build()))
                .collect::<Vec<(String,ColumnDesc)>>();
                if let Some(schema_catalog) = db_catalog.get_current_schema(){
                    let table_name = &name.to_string();
                    let table_name = BoundCreateTable::lower_case_object_name(table_name);
                    let table_name = BoundCreateTable::complete_schema_if_required(&table_name, schema_catalog.name());
                    let schema_id = BoundCreateTable::get_schema_from_table(&table_name, Arc::clone(&db_catalog))?;
                    BoundCreateTable::table_name_exist(&table_name, schema_catalog)?;
                    Ok(BoundCreateTable { schema_id:schema_id, table_name:table_name, columns:columns_desc.to_owned()})
                }else{
                    Err(BindError::UnableToGetCurrentSchemaCatalog)
                }
                
            },
            _ => Err(BindError::NotSupportedStatement)
        }
    }

    fn lower_case_object_name(name:&str) -> String {
        let name = name.to_lowercase();
        name
    }

    fn get_schema_from_table(name:&String,db_catalog:Arc<DatabaseCatalog>) -> Result<SchemaId,BindError> {
        let result = 
        if let Some((schema_name,table_name)) = name.split_once('.'){
            if let Some(schema) = db_catalog.get_schema_by_name(schema_name){
                Ok(schema.id())
            }else{
                Err(BindError::SchemaNotFound(schema_name.to_string()))
            }
        }else{
            Err(BindError::SchemaNotFound(name.to_string()))
        };
        result
        
    }

    fn table_name_exist(name:&String,schema_catalog:Arc<SchemaCatalog>) -> Result<(),BindError>{
        if let Some(table) = schema_catalog.get_table_by_name(name){
            Err(BindError::TableAlreadyExist)
        }else{
            Ok(())
        }
    }

    fn complete_schema_if_required(name:&str,current_schema_name:String) -> String {
        if !name.contains("."){
            let name = format!("{}.{}",&current_schema_name,&name);
            return name
        }
        name.to_string()
    }

}