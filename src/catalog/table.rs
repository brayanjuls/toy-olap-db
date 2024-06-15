use std::sync::Arc;

use uuid::Uuid;

use super::column::{ColumnCatalog, ColumnDesc, ColumnId};

pub struct TableCatalog{
    id: TableId,
    name: String,
    columns_catalog:Vec<Arc<ColumnCatalog>>
}

impl TableCatalog {
    pub fn id(&self) -> TableId { self.id.clone()}
    pub fn name(&self) -> String {self.name.clone()}
    pub fn get_column(&self, id: ColumnId) -> Option<Arc<ColumnCatalog>> {
        for column in self.all_columns(){
            if column.id() == id{
                return Some(Arc::clone(&column));
            }
        }
        None
    }
    pub fn all_columns(&self) -> Vec<Arc<ColumnCatalog>> {
        //todo: think about the most efficient way to return this va;ue
        self.columns_catalog.clone()
    }
    
    pub fn new(name:String, columns: &[(String, ColumnDesc)]) -> Self {
        let new_columns = columns.iter().map(|(n,d)| Arc::new(ColumnCatalog::new(n.clone(), d.clone()))).collect::<Vec<Arc<ColumnCatalog>>>();
        Self { id: TableId { uuid: Uuid::new_v4() }, name: name, columns_catalog: new_columns }
    }
}

#[derive(Debug,PartialEq,Eq,Hash,Clone)]
pub struct TableId{
    uuid:Uuid
}
