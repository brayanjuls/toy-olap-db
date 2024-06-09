use std::path::Path;

use sqllogictest::{DefaultColumnType,DBOutput};
use crate::database::{Database,Error};

impl sqllogictest::DB for Database {

    fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        let result = self.run(sql)?;
        Ok(result)
    }
    
    type Error = Error;
    
    type ColumnType = DefaultColumnType;
}

#[test]
fn test() {
    let script = std::fs::read_to_string(Path::new("./../sql/01-01.slt")).unwrap();
    let mut tester = sqllogictest::Runner::new(|| async {Ok(Database::new())});
    tester.run_script(&script).unwrap();
}