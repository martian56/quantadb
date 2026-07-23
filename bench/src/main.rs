//! Drives a running QuantaDB server with a mixed point workload and reports
//! throughput and latency percentiles. Reads and writes are reported
//! separately because durable writes pay for storage syncs and reads do not.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::process::ExitCode;
use std::thread;
use std::time::{Duration, Instant};

const USAGE: &str = "loadgen: mixed point workload for a running QuantaDB server

Options:
  --address HOST:PORT   server address (default 127.0.0.1:54321)
  --connections N       concurrent client connections (default 8)
  --seconds N           measured run time (default 10)
  --read-percent N      share of reads in the mix, 0 to 100 (default 80)
  --rows N              preloaded key population (default 10000)
  --help                print this text";

#[derive(Debug, Clone)]
struct Config {
    address: String,
    connections: usize,
    seconds: u64,
    read_percent: u64,
    rows: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            address: "127.0.0.1:54321".to_owned(),
            connections: 8,
            seconds: 10,
            read_percent: 80,
            rows: 10_000,
        }
    }
}

#[derive(Debug, Default)]
struct WorkerReport {
    read_nanos: Vec<u64>,
    write_nanos: Vec<u64>,
    conflicts: u64,
    failures: u64,
}

struct Connection {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
    next_request_id: u64,
}

enum Outcome {
    Ok,
    Conflict,
    Failure,
}

fn main() -> ExitCode {
    let config = match parse_args() {
        Ok(Some(config)) => config,
        Ok(None) => {
            println!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Err(message) => {
            eprintln!("{message}");
            eprintln!("{USAGE}");
            return ExitCode::FAILURE;
        }
    };

    match run(&config) {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("loadgen: {message}");
            ExitCode::FAILURE
        }
    }
}

fn parse_args() -> Result<Option<Config>, String> {
    let mut config = Config::default();
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        if flag == "--help" {
            return Ok(None);
        }
        let value = args
            .next()
            .ok_or_else(|| format!("{flag} needs a value"))?;
        match flag.as_str() {
            "--address" => config.address = value,
            "--connections" => config.connections = parse_number(&flag, &value)? as usize,
            "--seconds" => config.seconds = parse_number(&flag, &value)?,
            "--read-percent" => {
                let percent = parse_number(&flag, &value)?;
                if percent > 100 {
                    return Err("--read-percent must be between 0 and 100".to_owned());
                }
                config.read_percent = percent;
            }
            "--rows" => config.rows = parse_number(&flag, &value)?,
            other => return Err(format!("unknown option {other}")),
        }
    }
    if config.connections == 0 || config.seconds == 0 || config.rows == 0 {
        return Err("connections, seconds, and rows must all be positive".to_owned());
    }
    Ok(Some(config))
}

fn parse_number(flag: &str, value: &str) -> Result<u64, String> {
    value
        .parse::<u64>()
        .map_err(|_| format!("{flag} needs a whole number, got {value}"))
}

fn run(config: &Config) -> Result<(), String> {
    let mut setup = Connection::open(&config.address)?;
    prepare_table(&mut setup, config.rows)?;

    println!(
        "loadgen: {} connections, {}s, {}% reads, {} rows, {}",
        config.connections, config.seconds, config.read_percent, config.rows, config.address
    );

    let deadline = Instant::now() + Duration::from_secs(config.seconds);
    let started = Instant::now();
    let mut handles = Vec::with_capacity(config.connections);
    for worker in 0..config.connections {
        let config = config.clone();
        handles.push(thread::spawn(move || worker_loop(&config, worker, deadline)));
    }

    let mut report = WorkerReport::default();
    for handle in handles {
        let worker = handle
            .join()
            .map_err(|_| "a worker thread panicked".to_owned())??;
        report.read_nanos.extend(worker.read_nanos);
        report.write_nanos.extend(worker.write_nanos);
        report.conflicts += worker.conflicts;
        report.failures += worker.failures;
    }
    let elapsed = started.elapsed();

    print_report(&report, elapsed);
    Ok(())
}

fn prepare_table(setup: &mut Connection, rows: u64) -> Result<(), String> {
    expect_success(setup, "DROP TABLE IF EXISTS bench_kv")?;
    expect_success(
        setup,
        "CREATE TABLE bench_kv (id BIGINT PRIMARY KEY, val TEXT NOT NULL)",
    )?;
    let mut id = 0_u64;
    while id < rows {
        let batch_end = (id + 500).min(rows);
        let values = (id..batch_end)
            .map(|row| format!("({row}, 'seed {row}')"))
            .collect::<Vec<_>>()
            .join(", ");
        expect_success(
            setup,
            &format!("INSERT INTO bench_kv (id, val) VALUES {values}"),
        )?;
        id = batch_end;
    }
    Ok(())
}

fn expect_success(connection: &mut Connection, sql: &str) -> Result<(), String> {
    match connection.execute(sql)? {
        Outcome::Ok => Ok(()),
        Outcome::Conflict | Outcome::Failure => Err(format!("setup statement failed: {sql}")),
    }
}

fn worker_loop(
    config: &Config,
    worker: usize,
    deadline: Instant,
) -> Result<WorkerReport, String> {
    let mut connection = Connection::open(&config.address)?;
    let mut report = WorkerReport::default();
    let mut rng = 0x9e37_79b9_7f4a_7c15_u64 ^ ((worker as u64 + 1) << 17);

    while Instant::now() < deadline {
        rng = next_random(rng);
        let id = rng % config.rows;
        let is_read = rng % 100 < config.read_percent;
        let sql = if is_read {
            format!("SELECT val FROM bench_kv WHERE id = {id}")
        } else {
            format!("UPDATE bench_kv SET val = 'w{rng}' WHERE id = {id}")
        };

        let start = Instant::now();
        let outcome = connection.execute(&sql)?;
        let nanos = u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX);

        match outcome {
            Outcome::Ok => {
                if is_read {
                    report.read_nanos.push(nanos);
                } else {
                    report.write_nanos.push(nanos);
                }
            }
            Outcome::Conflict => report.conflicts += 1,
            Outcome::Failure => report.failures += 1,
        }
    }
    Ok(report)
}

fn next_random(mut state: u64) -> u64 {
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    state
}

impl Connection {
    fn open(address: &str) -> Result<Self, String> {
        let stream = TcpStream::connect(address)
            .map_err(|error| format!("cannot connect to {address}: {error}"))?;
        stream
            .set_nodelay(true)
            .map_err(|error| format!("cannot disable Nagle: {error}"))?;
        let writer = stream
            .try_clone()
            .map_err(|error| format!("cannot clone stream: {error}"))?;
        Ok(Self {
            reader: BufReader::new(stream),
            writer,
            next_request_id: 1,
        })
    }

    fn execute(&mut self, sql: &str) -> Result<Outcome, String> {
        let request_id = self.next_request_id;
        self.next_request_id += 1;
        let frame = serde_json::json!({
            "protocol_version": 1,
            "request_id": request_id,
            "request": { "type": "execute", "sql": sql },
        });
        let mut line = frame.to_string();
        line.push('\n');
        self.writer
            .write_all(line.as_bytes())
            .map_err(|error| format!("cannot send request: {error}"))?;

        let mut response = String::new();
        let bytes = self
            .reader
            .read_line(&mut response)
            .map_err(|error| format!("cannot read response: {error}"))?;
        if bytes == 0 {
            return Err("server closed the connection".to_owned());
        }
        let value: serde_json::Value = serde_json::from_str(&response)
            .map_err(|error| format!("bad response frame: {error}"))?;

        match value["response"]["type"].as_str() {
            Some("executed") => Ok(Outcome::Ok),
            Some("error") => {
                if value["response"]["error"]["code"] == "transaction_error" {
                    Ok(Outcome::Conflict)
                } else {
                    Ok(Outcome::Failure)
                }
            }
            _ => Err(format!("unexpected response: {}", response.trim_end())),
        }
    }
}

fn print_report(report: &WorkerReport, elapsed: Duration) {
    let total_ops = report.read_nanos.len() + report.write_nanos.len();
    let throughput = total_ops as f64 / elapsed.as_secs_f64();
    println!(
        "completed {total_ops} operations in {:.1}s ({throughput:.0} ops/s), {} conflicts, {} failures",
        elapsed.as_secs_f64(),
        report.conflicts,
        report.failures
    );
    print_latency_line("reads ", &report.read_nanos);
    print_latency_line("writes", &report.write_nanos);
}

fn print_latency_line(label: &str, nanos: &[u64]) {
    if nanos.is_empty() {
        println!("{label}  none");
        return;
    }
    let mut sorted = nanos.to_vec();
    sorted.sort_unstable();
    println!(
        "{label}  count {:>8}  p50 {:>9}  p90 {:>9}  p99 {:>9}  p99.9 {:>9}  max {:>9}",
        sorted.len(),
        format_nanos(percentile(&sorted, 0.50)),
        format_nanos(percentile(&sorted, 0.90)),
        format_nanos(percentile(&sorted, 0.99)),
        format_nanos(percentile(&sorted, 0.999)),
        format_nanos(sorted[sorted.len() - 1]),
    );
}

fn percentile(sorted: &[u64], quantile: f64) -> u64 {
    let position = (sorted.len() - 1) as f64 * quantile;
    sorted[position.round() as usize]
}

fn format_nanos(nanos: u64) -> String {
    if nanos >= 1_000_000_000 {
        format!("{:.2}s", nanos as f64 / 1_000_000_000.0)
    } else if nanos >= 1_000_000 {
        format!("{:.2}ms", nanos as f64 / 1_000_000.0)
    } else {
        format!("{:.1}us", nanos as f64 / 1_000.0)
    }
}
