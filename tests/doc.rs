use std::fs::File;
use std::io;
use std::io::BufRead;
use std::path::Path;

#[test]
fn doc() {
    let rs = read_lines("src/main.rs").unwrap();
    rs.for_each(|f| {
        println!("//!{}", f.unwrap());
    });
}

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}
