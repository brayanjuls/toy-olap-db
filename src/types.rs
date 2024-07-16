pub use sqlparser::ast::DataType as DataTypeKind;

#[derive(Debug,Clone,PartialEq)]
pub struct DataType {
    is_nullable:bool,
    kind: DataTypeKind
}

impl DataType {
    pub fn is_nullable(&self) -> bool { self.is_nullable}
    pub fn kind(&self) -> DataTypeKind { self.kind.clone()}
    pub fn new(kind:DataTypeKind) -> Self{
        Self { is_nullable: false, kind }
    }
}