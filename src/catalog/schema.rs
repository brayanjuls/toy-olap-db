use std::{borrow::Borrow, collections::HashMap, sync::{Arc,Mutex}};

use uuid::Uuid;

use super::{column::ColumnDesc, table::{TableCatalog, TableId}};

pub struct SchemaCatalog{
    inner:Mutex<InnerSchema>
}

pub struct InnerSchema{
    id:SchemaId,
    name: String,
    table_catalog: HashMap::<TableId,Arc<TableCatalog>>
}



impl SchemaCatalog {
    pub fn id(&self) -> SchemaId { self.inner.lock().unwrap().id.clone()}
    pub fn name(&self) -> String { self.inner.lock().unwrap().name.clone()}
    pub fn add_table(&self, name: &str, columns: &[(String, ColumnDesc)]) -> TableId { 
        let table = Arc::new(TableCatalog::new(name.to_string(),columns));
        self.inner.lock().unwrap().table_catalog.insert(table.id(), Arc::clone(&table));
        table.id()
    }
        
    pub fn get_table(&self, id: TableId) -> Option<Arc<TableCatalog>> { self.inner.lock().unwrap().table_catalog.get(&id).cloned() }
    pub fn del_table(&self, id: TableId) { self.inner.lock().unwrap().table_catalog.remove(&id);}
    pub fn new(name:String) -> Self{
        Self{
            inner: Mutex::new(InnerSchema { 
                id: SchemaId{uuid:Uuid::new_v4().to_string()}, 
                name: name, 
                table_catalog: HashMap::<TableId,Arc<TableCatalog>>::new() 
            })
        }
    }
}

#[derive(Debug,PartialEq,Eq,Hash,Clone)]
pub struct SchemaId{
    uuid:String
}