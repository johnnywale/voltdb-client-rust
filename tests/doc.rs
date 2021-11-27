use std::{io, thread};
use std::cell::RefCell;
use std::fs::File;
use std::io::BufRead;
use std::path::Path;
use std::rc::Rc;

#[test]
fn doc() {
    let rs = read_lines("src/main.rs").unwrap();
    rs.for_each(|f| {
        println!("//!{}", f.unwrap());
    });
}

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
    where P: AsRef<Path>, {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

#[test]
fn rc_test() {

    let data = Rc::new(RefCell::new(Vec::new()));
    let mut vec = vec![];
    for _ in 0..10 {
        let x = data.clone();
        let handle = thread::spawn(move || unsafe {
            x.borrow();
        }
        );
        vec.push(handle);
    }

    for handle in vec {
        handle.join().unwrap();
    }
}


