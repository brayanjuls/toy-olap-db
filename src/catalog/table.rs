use std::{collections::HashMap, sync::{Arc, Mutex}};


use sqlparser::ast::DataType;
use uuid::Uuid;

use super::{column::{ColumnCatalog, ColumnDesc, ColumnId}, CatalogeError};

pub struct TableCatalog{
    id: TableId,
    inner:Mutex<Inner>
}

struct Inner{
    name: String,
    columns_catalog:HashMap<ColumnId,Arc<ColumnCatalog>>,
    columns_idxs:HashMap<String,ColumnId>,
    next_column_id:u32
}

impl TableCatalog {
    pub fn id(&self) -> TableId { self.id.clone()}
    pub fn name(&self) -> String {
        let inner = self.inner.lock().unwrap();
        inner.name.clone()
    }
    pub fn get_column(&self, id: ColumnId) -> Option<Arc<ColumnCatalog>> {
        let inner = self.inner.lock().unwrap();
        if inner.columns_catalog.contains_key(&id) {   
            return inner.columns_catalog.get(&id).cloned()
        }
        None
    }
    pub fn all_columns(&self) -> HashMap<ColumnId,Arc<ColumnCatalog>> {
        //todo: think about the most efficient way to return this va;ue
        let inner = self.inner.lock().unwrap();
        inner.columns_catalog.clone()
    }

    pub fn add_column(&self, name:String,desc:ColumnDesc) -> Result<ColumnId, CatalogeError> {
        let mut inner = self.inner.lock().unwrap();
        if !inner.columns_idxs.contains_key(&name){
            let new_column = Arc::new(ColumnCatalog::new(inner.next_column_id, name.clone(), desc));
            let new_column_id = inner.next_column_id;
            inner.columns_catalog.insert(new_column_id, new_column);
            inner.next_column_id+=1;
           
            return Ok(new_column_id)
        }
       
        Err(CatalogeError::DuplicatedObject(format!("duplicated column {}",name)))
    }

    
    pub fn new(name:String, columns: &Vec<(String,ColumnDesc)>) -> Self {
        let mut col_id = 1;
        let mut new_columns = HashMap::<ColumnId,Arc<ColumnCatalog>>::new();
        let mut column_idxs = HashMap::<String,ColumnId>::new();
        for (column_name,column_desc) in columns{
            let current_column = Arc::new(ColumnCatalog::new(col_id,column_name.to_owned(), column_desc.to_owned()));
            new_columns.insert(col_id,current_column);
            column_idxs.insert(column_name.to_owned(),col_id);
            col_id+=1;
        }
        Self { 
            id: TableId { uuid: Uuid::new_v4() },
            inner: Mutex::new(Inner { 
                name: name, 
                columns_catalog: new_columns,
                columns_idxs: column_idxs,
                next_column_id: if col_id == 1 {col_id} else {col_id+1}
            }) 
        }
    }
}

#[derive(Debug,PartialEq,Eq,Hash,Clone)]
pub struct TableId{
    uuid:Uuid
}

#[cfg(test)]
mod tests{

    use sqlparser::ast::DataType as DataTypeKind;

    use crate::catalog::{column::ColumnDescBuilder, CatalogeError};

    use super::TableCatalog;

    
    #[test]
    fn  test_table_catalog_creation(){

        let cols = &vec![
            ("name".to_string(), ColumnDescBuilder::new().datatype(DataTypeKind::Text).build()),
            ("value".to_string(), ColumnDescBuilder::new().datatype(DataTypeKind::Float64).build()),
        ];
        let table: TableCatalog = TableCatalog::new("example".to_string(), cols);

        let expected = [String::from("name"),String::from("value")];    
        table.all_columns().iter().for_each(|(_,v)| assert!(expected.contains(&v.name())));
        table.all_columns().iter().for_each(|(_,v)| assert!(expected.contains(&table.get_column(v.id()).unwrap().name())));
        assert!(table.name() == "example") 
    }

    #[test]
    fn test_empty_table_creation(){
        let table = TableCatalog::new("products".to_string(), &Vec::new());
        let _ = table.add_column(String::from("id"), ColumnDescBuilder::new().datatype(DataTypeKind::Int64).build());
        let _ = table.add_column(String::from("cost"), ColumnDescBuilder::new().datatype(DataTypeKind::Double).build());

        assert_eq!(table.get_column(2).unwrap().name(), String::from("cost"))
    }

    #[test]
    fn test_duplicate_column_names(){
        let table = TableCatalog::new(String::from("products"), &vec![(String::from("id"),ColumnDescBuilder::new().datatype(DataTypeKind::Int64).build())]);
        let operation_result =  table.add_column("id".to_string(), ColumnDescBuilder::new().datatype(DataTypeKind::Int64).build());
        assert!(operation_result.is_err());
        assert_eq!(operation_result.unwrap_err(),CatalogeError::DuplicatedObject("duplicated column id".to_string()));
    }
}
