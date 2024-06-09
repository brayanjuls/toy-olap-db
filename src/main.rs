
use rustyline;
use rustyline::error::ReadlineError;
use toy_olap_db::database::Database;

fn main() {
    let mut rl = rustyline::DefaultEditor::new().unwrap();
    let db = Database::new();
    loop{
        match rl.readline("> ") {
            Ok(line) => {
                rl.add_history_entry(&line).unwrap();
                let result = db.run(&line);
                match result {
                    Ok(ret) => print!("{:#?}", ret),
                    Err(err) => println!("error: {:#?}", err),
                }
            },
            Err(ReadlineError::Interrupted) => {},
            Err(ReadlineError::Eof) => break,
            Err(err) => println!("Error {:?}",err)
         }
    }

}

