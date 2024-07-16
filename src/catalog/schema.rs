use std::{collections::HashMap, sync::{Arc,Mutex}};


use super::{column::ColumnDesc, table::{TableCatalog, TableId}};

pub struct SchemaCatalog{
    inner:Mutex<InnerSchema>
}

pub struct InnerSchema{
    id:SchemaId,
    name: String,
    table_catalog: HashMap::<TableId,Arc<TableCatalog>>,
    table_idxs: HashMap<String,TableId>
}



impl SchemaCatalog {
    pub fn id(&self) -> SchemaId { self.inner.lock().unwrap().id}
    pub fn name(&self) -> String { self.inner.lock().unwrap().name.clone()}
    pub fn add_table(&self, name: &str, columns: &Vec<(String, ColumnDesc)>) -> TableId { 
        let table = Arc::new(TableCatalog::new(name.to_string(),columns));
        let mut inner  = self.inner.lock().unwrap();
        inner.table_catalog.insert(table.id(), Arc::clone(&table));
        inner.table_idxs.insert(table.name(),table.id());
        table.id()
    }
        
    pub fn get_table(&self, id: TableId) -> Option<Arc<TableCatalog>> { self.inner.lock().unwrap().table_catalog.get(&id).cloned() }
    pub fn del_table(&self, id: TableId) { self.inner.lock().unwrap().table_catalog.remove(&id);}
    pub fn new(name:String, id:SchemaId) -> Self{
        Self{
            inner: Mutex::new(InnerSchema { 
                id: id, 
                name: name, 
                table_catalog: HashMap::<TableId,Arc<TableCatalog>>::new(),
                table_idxs: HashMap::<String,TableId>::new(),
            })
        }
    }
    pub fn get_table_by_name(&self, name:&str) -> Option<Arc<TableCatalog>>{
        let inner = self.inner.lock().unwrap();
        inner.table_idxs.get(name)
        .and_then(|id|inner.table_catalog.get(id))
        .cloned()
    }
}


pub type SchemaId = u32;