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

#[cfg(test)]
mod tests{
    use crate::{catalog::column::ColumnDesc, types::DataType};
    use sqlparser::ast::DataType as DataTypeKind;

    use super::TableCatalog;

    
    #[test]
    pub fn  test_table_catalog(){

        let cols = &[
            ("name".to_string(), ColumnDesc::new("name".to_string(),DataType::new(DataTypeKind::Text) )),
            ("value".to_string(), ColumnDesc::new("value".to_string(),DataType::new(DataTypeKind::Float64) )),
        ];
        let table = TableCatalog::new("example".to_string(), cols);

        let expected = [String::from("name"),String::from("value")];    
        table.all_columns().iter().for_each(|x| assert!(expected.contains(&x.name())));
        table.all_columns().iter().for_each(|c| assert!(expected.contains(&table.get_column(c.id()).unwrap().name())));
    }
}
