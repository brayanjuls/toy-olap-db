use sqlparser::{ast::Statement, dialect::PostgreSqlDialect, parser::Parser};

pub fn parse(sql:&str) -> Result<Vec<Statement>, sqlparser::parser::ParserError>{
    let dialect = PostgreSqlDialect {};
    let ast = Parser::parse_sql(&dialect, sql);
    ast
}