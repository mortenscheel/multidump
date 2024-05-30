use clap::{Parser, Subcommand};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::Command;
use indicatif::{ProgressBar, ProgressStyle};

#[derive(Parser)]
#[command(name = "multidump")]
#[command(about = "Tool to split and import MySQL dump files", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Split {
        #[arg(long)]
        input: String,
        #[arg(long)]
        output: String,
    },
    Import {
        #[arg(long)]
        input: String,
        #[arg(long)]
        database: String,
        #[arg(long)]
        host: Option<String>,
        #[arg(long)]
        port: Option<u16>,
        #[arg(long)]
        user: Option<String>,
        #[arg(long)]
        password: Option<String>,
        #[arg(long)]
        parallel: usize,
        #[arg(long)]
        delete: bool,
        #[arg(long)]
        debug: bool,
    },
    SplitImport {
        #[arg(long)]
        input: String,
        #[arg(long)]
        output: String,
        #[arg(long)]
        database: String,
        #[arg(long)]
        host: Option<String>,
        #[arg(long)]
        port: Option<u16>,
        #[arg(long)]
        user: Option<String>,
        #[arg(long)]
        password: Option<String>,
        #[arg(long)]
        parallel: usize,
        #[arg(long)]
        delete: bool,
        #[arg(long)]
        debug: bool,
    },
}

fn scan_sql_dump(file_path: &str) -> std::io::Result<(String, String)> {
    println!("Scanning SQL dump to determine preamble and postamble...");
    let infile = File::open(file_path)?;
    let reader = BufReader::new(infile);
    let mut preamble = Vec::new();
    let mut postamble = Vec::new();
    let mut in_preamble = true;
    let mut last_unlock_line = None;

    for (index, line) in reader.lines().enumerate() {
        let line = line?;
        if in_preamble {
            if line.starts_with("DROP TABLE") {
                in_preamble = false;
            } else {
                preamble.push(line.clone());
            }
        }

        if !in_preamble {
            if line.starts_with("UNLOCK TABLES;") {
                last_unlock_line = Some(index);
            }
        }

        postamble.push(line);
    }

    if let Some(last_unlock_index) = last_unlock_line {
        postamble.drain(0..=last_unlock_index);
    } else {
        postamble.clear();
    }

    println!("Preamble and postamble determined.");
    Ok((preamble.join("\n"), postamble.join("\n")))
}

fn split_sql_dump(file_path: &str, output_dir: &str, preamble: &str, postamble: &str) -> std::io::Result<()> {
    if !Path::new(output_dir).exists() {
        fs::create_dir(output_dir)?;
    }

    println!("Splitting SQL dump file...");
    let infile = File::open(file_path)?;
    let reader = BufReader::new(infile);
    let mut table_file: Option<File> = None;
    let mut table_lines: Vec<String> = Vec::new();

    for line in reader.lines() {
        let line = line?;
        if line.starts_with("DROP TABLE") {
            if let Some(mut file) = table_file.take() {
                file.write_all(table_lines.concat().as_bytes())?;
                file.write_all(postamble.as_bytes())?;
                file.flush()?;
            }

            table_lines.clear();
            table_lines.push(line.clone() + "\n");

            let table_name = line.split('`').nth(1).unwrap_or("").to_string();
            println!("Creating file for table: {}", table_name);
            let file_path = format!("{}/{}.sql", output_dir, table_name);
            table_file = Some(File::create(file_path)?);

            if let Some(ref mut file) = table_file {
                file.write_all(preamble.as_bytes())?;
                file.write_all(b"\n")?;
                file.write_all(table_lines.concat().as_bytes())?;
                table_lines.clear();
            }
        } else {
            table_lines.push(line.clone() + "\n");
        }

        if line.starts_with("UNLOCK TABLES;") {
            if let Some(mut file) = table_file.take() {
                file.write_all(table_lines.concat().as_bytes())?;
                file.write_all(postamble.as_bytes())?;
                file.flush()?;
            }
            table_lines.clear();
        }
    }

    if let Some(mut file) = table_file.take() {
        file.write_all(table_lines.concat().as_bytes())?;
        file.write_all(postamble.as_bytes())?;
        file.flush()?;
    }

    println!("Splitting completed.");
    Ok(())
}

fn import_sql_files(
    input: &str,
    database: &str,
    host: Option<&str>,
    port: Option<u16>,
    user: Option<&str>,
    password: Option<&str>,
    parallel: usize,
    delete: bool,
    debug: bool,
) -> std::io::Result<()> {
    let paths = fs::read_dir(input)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.is_file())
        .collect::<Vec<_>>();

    let total_files = paths.len() as u64;
    let pb = ProgressBar::new(total_files);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} {msg} (ETA: {eta})")
            .progress_chars("#>-"),
    );

    println!("Importing SQL files...");
    let mut handles = vec![];

    for chunk in paths.chunks(parallel) {
        for path in chunk {
            let path = path.clone();
            let db = database.to_string();
            let mut args = Vec::new();

            if let Some(host) = host {
                args.push(format!("--host={}", host));
            }

            if let Some(port) = port {
                args.push(format!("--port={}", port));
            }

            if let Some(user) = user {
                args.push(format!("--user={}", user));
            }

            if let Some(password) = password {
                args.push(format!("--password={}", password));
            }

            args.push(db.clone());
            args.push("<".to_string());
            args.push(path.to_str().unwrap().to_string());

            if debug {
                println!("Running command: mysql {}", args.join(" "));
            }

            let pb_clone = pb.clone();
            let handle = std::thread::spawn(move || {
                let output = Command::new("sh")
                    .arg("-c")
                    .arg(format!("mysql {}", args.join(" ")))
                    .output();

                if let Err(e) = output {
                    eprintln!("Failed to execute mysql import command: {}", e);
                }

                pb_clone.inc(1);
                pb_clone.set_message(format!("Importing file: {}", path.display()));
            });
            handles.push(handle);
        }

        for handle in handles.drain(..) {
            handle.join().expect("Thread panicked");
        }
    }

    pb.finish_with_message("Import completed.");

    if delete {
        println!("Deleting directory: {}", input);
        fs::remove_dir_all(input)?;
    }

    println!("Importing completed.");
    Ok(())
}

fn main() -> std::io::Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Split { input, output } => {
            let (preamble, postamble) = scan_sql_dump(input)?;
            split_sql_dump(input, output, &preamble, &postamble)?;
        }
        Commands::Import {
            input,
            database,
            host,
            port,
            user,
            password,
            parallel,
            delete,
            debug,
        } => {
            import_sql_files(input, database, host.as_deref(), *port, user.as_deref(), password.as_deref(), *parallel, *delete, *debug)?;
        }
        Commands::SplitImport {
            input,
            output,
            database,
            host,
            port,
            user,
            password,
            parallel,
            delete,
            debug,
        } => {
            let (preamble, postamble) = scan_sql_dump(input)?;
            split_sql_dump(input, output, &preamble, &postamble)?;
            import_sql_files(output, database, host.as_deref(), *port, user.as_deref(), password.as_deref(), *parallel, *delete, *debug)?;
        }
    }

    Ok(())
}
