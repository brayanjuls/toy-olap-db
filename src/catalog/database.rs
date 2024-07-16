use std::{collections::HashMap, sync::{Arc, Mutex}};

use super::schema::{SchemaCatalog, SchemaId};


pub struct DatabaseCatalog{
    inner:Mutex<Inner>
}

pub struct Inner{
    schema_catalog:HashMap<SchemaId,Arc<SchemaCatalog>>,
    schema_idxs: HashMap<String,SchemaId>,
    default_schema: SchemaId,
    next_schema_id:SchemaId
}


impl DatabaseCatalog {
    pub fn add_schema(&self, name: &str) -> SchemaId { 
        let mut inner = self.inner.lock().unwrap();
        let schema = Arc::new(SchemaCatalog::new( name.to_string(),inner.next_schema_id));
        inner.schema_catalog.insert(schema.id(),Arc::clone(&schema));
        inner.schema_idxs.insert(name.to_string(), schema.id());
        inner.next_schema_id +=1;
        schema.id()
    }
    pub fn get_schema(&self, id: SchemaId) -> Option<Arc<SchemaCatalog>> { 
       let inner = self.inner.lock().unwrap();
       inner.schema_catalog.get(&id).cloned()
    }
    pub fn del_schema(&self, id: SchemaId) { self.inner.lock().unwrap().schema_catalog.remove(&id);}
    pub fn new() -> Self{
        let id: u32 = 1;
        let schema_name = String::from("public");
        let default_schema = Arc::new(SchemaCatalog::new(schema_name.clone(),id));
        let mut schema_catalog = HashMap::new();
        schema_catalog.insert( id,Arc::clone(&default_schema));
        let mut schema_idx = HashMap::new();
        schema_idx.insert(schema_name.clone(), id);
        Self{inner: Mutex::new(Inner { schema_catalog:schema_catalog ,default_schema:id, next_schema_id: id + 1,schema_idxs:schema_idx})}
    }

    pub fn get_current_schema(&self) -> Option<Arc<SchemaCatalog>> {
        let inner = self.inner.lock().unwrap();
        let schema_id = inner.default_schema;
        inner.schema_catalog.get(&schema_id).cloned()
    }

    pub fn set_current_schema(&self, id:SchemaId){
        let mut inner = self.inner.lock().unwrap();
        inner.default_schema = id;
        
    }
    
    pub fn get_all_schemas(&self) -> Vec<Arc<SchemaCatalog>>{
        let inner = self.inner.lock().unwrap();
        inner.schema_catalog.values().cloned().collect()
    }

    pub fn get_schema_by_name(&self, name:&str) -> Option<Arc<SchemaCatalog>>{
        let inner = self.inner.lock().unwrap();
        inner.schema_idxs.get(name)
        .and_then(|id|inner.schema_catalog.get(id))
        .cloned()
    }
    
}


#[cfg(test)]
mod test{
    use super::DatabaseCatalog;

    #[test]
    fn test_create_db(){
        let db_catalog = DatabaseCatalog::new();
        if let Some(cur_schema) = db_catalog.get_current_schema() {
            let schema_catalog = cur_schema.as_ref();
            assert_eq!(schema_catalog.name(),String::from("public"));
        }        
        assert_eq!(db_catalog.get_all_schemas().len(),1);
    }

    #[test]
    fn test_add_new_schema(){
        let db_catalog = DatabaseCatalog::new();
        let new_schema_id = db_catalog.add_schema("product");
        db_catalog.set_current_schema(new_schema_id);

        if let Some(cur_schema) = db_catalog.get_current_schema(){
            let schema_catalog = cur_schema.as_ref();
            assert_eq!(schema_catalog.name(),String::from("product"));
        }
        assert_eq!(db_catalog.get_all_schemas().len(),2);
    }
}