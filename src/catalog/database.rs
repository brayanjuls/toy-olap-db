use std::{collections::HashMap, sync::{Arc, Mutex}};

use super::schema::{SchemaCatalog, SchemaId};


pub struct DatabaseCatalog{
    inner:Mutex<Inner>
}

pub struct Inner{
    schema_catalog:HashMap<SchemaId,Arc<SchemaCatalog>>
}


impl DatabaseCatalog {
    pub fn add_schema(&self, name: &str) -> SchemaId { 
        let schema = Arc::new(SchemaCatalog::new( name.to_string()));
        self.inner.lock().unwrap().schema_catalog.insert(schema.id(),Arc::clone(&schema));
        schema.id()
    }
    pub fn get_schema(&self, id: SchemaId) -> Option<Arc<SchemaCatalog>> { 
       self.inner.lock().unwrap().schema_catalog.get(&id).cloned()
    }
    pub fn del_schema(&self, id: SchemaId) { self.inner.lock().unwrap().schema_catalog.remove(&id);}
    pub fn new(&self) -> Self{
        Self{inner: Mutex::new(Inner { schema_catalog: HashMap::new() })}
    }
}