use sqlparser::ast::DataType as DataTypeKind;

use crate::types::DataType;

#[derive(Debug)]
pub struct ColumnCatalog {
    id: ColumnId,
    name: String,
    description: ColumnDesc
}

impl ColumnCatalog {
    pub fn id(&self) -> ColumnId { self.id.clone()}
    pub fn name(&self) -> String { self.name.clone()}
    pub fn desc(&self) -> ColumnDesc { self.description.clone()}
    pub fn new(id:ColumnId, name:String, datatype_kind:DataTypeKind) -> Self {
        Self { 
            id,
            description:ColumnDesc::new(name.clone(), false, false, datatype_kind),
            name
        }
    }
}

pub type ColumnId = u32;


#[derive(Debug,Clone)]
pub struct ColumnDesc{
    text:String,
    is_nullable:bool,
    is_primary:bool,
    datatype: DataType
}

impl ColumnDesc {
    pub fn new(text:String,is_nullable:bool,is_primary:bool,datatype_kind:DataTypeKind) -> Self{
        Self {
            text,
            datatype: DataType::new(datatype_kind),
            is_nullable:false,
            is_primary:false
        }
    }
}

impl ColumnDesc {
    pub fn is_nullable(&self) -> bool {self.is_nullable.clone()}
    pub fn is_primary(&self) -> bool { self.is_primary.clone()}
    pub fn datatype(&self) -> DataType { self.datatype.clone()}
    pub fn description(&self) -> String { self.text.clone()}
}
