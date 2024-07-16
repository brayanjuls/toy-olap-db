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
    pub fn new(id:ColumnId, name:String, description:ColumnDesc) -> Self {
        Self { 
            id,
            description,
            name
        }
    }
}

pub type ColumnId = u32;


#[derive(Debug,Clone,PartialEq)]
pub struct ColumnDesc{
    text:String,
    is_nullable:bool,
    is_primary:bool,
    datatype: DataType
}

pub struct ColumnDescBuilder{
    text:String,
    is_nullable:bool,
    is_primary:bool,
    datatype: DataType
}

impl ColumnDescBuilder {

    pub fn new() -> Self{
        Self { 
            text: Default::default(), 
            is_nullable: Default::default(), 
            is_primary: Default::default(), 
            datatype:DataType::new(DataTypeKind::Int64) 
        }
    }
    pub fn text(mut self, text:String) -> Self{
        self.text = text;
        self
    }

    pub fn is_nullable(mut self, is_nullable:bool) -> Self{
        self.is_nullable = is_nullable;
        self
    }

    pub fn is_primary(mut self, is_primary:bool) -> Self{
        self.is_primary = is_primary;
        self
    }

    pub fn datatype(mut self, datatype:DataTypeKind) -> Self{
        self.datatype = DataType::new(datatype);
        self
    }

    pub fn build(self) -> ColumnDesc {
        ColumnDesc{
            text:self.text,
            datatype:self.datatype,
            is_nullable:self.is_nullable,
            is_primary:self.is_primary
        }
    }
}

impl ColumnDesc {
    pub fn is_nullable(&self) -> bool {self.is_nullable.clone()}
    pub fn is_primary(&self) -> bool { self.is_primary.clone()}
    pub fn datatype(&self) -> DataType { self.datatype.clone()}
    pub fn description(&self) -> String { self.text.clone()}
}
