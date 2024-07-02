pub mod database;
pub mod schema;
pub mod table;
pub mod column;

#[derive(thiserror::Error,Debug,PartialEq)]
pub enum CatalogeError{
    #[error("catalog error {0}")]
    NonExistingObjectError(&'static str),

    #[error("catalog error {0}")]
    DuplicatedObject(String)
}