//! The server binary.

use std::process::ExitCode;

use shardkv::{Config, Server};

const USAGE: &str = "\
shardkv-server - sharded in-memory key-value store

  --addr HOST:PORT        listen address               [127.0.0.1:7878]
  --shards N              store shards (power of two)  [16 x cores]
  --threads N             runtime worker threads       [cores]
  --max-frame BYTES       largest accepted frame       [8388608]
  --max-connections N     concurrent connection limit  [10000]
";

struct Args {
    addr: String,
    threads: Option<usize>,
    config: Config,
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        addr: "127.0.0.1:7878".into(),
        threads: None,
        config: Config::default(),
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        if flag == "--help" || flag == "-h" {
            print!("{USAGE}");
            std::process::exit(0);
        }
        let value = it
            .next()
            .ok_or_else(|| format!("missing value for {flag}"))?;
        let number = || {
            value
                .parse::<usize>()
                .map_err(|_| format!("{flag}: `{value}` is not a number"))
        };
        match flag.as_str() {
            "--addr" => args.addr = value,
            "--shards" => args.config.shards = number()?,
            "--threads" => args.threads = Some(number()?.max(1)),
            "--max-frame" => args.config.max_frame = number()?,
            "--max-connections" => args.config.max_connections = number()?,
            _ => return Err(format!("unknown option {flag}")),
        }
    }
    Ok(args)
}

fn main() -> ExitCode {
    let args = match parse_args() {
        Ok(args) => args,
        Err(e) => {
            eprintln!("error: {e}\n\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    let mut runtime = tokio::runtime::Builder::new_multi_thread();
    runtime.enable_all();
    if let Some(n) = args.threads {
        runtime.worker_threads(n);
    }
    let runtime = runtime.build().expect("failed to start the Tokio runtime");

    let result = runtime.block_on(async {
        let server = Server::bind(&args.addr, args.config.clone()).await?;
        eprintln!(
            "shardkv listening on {} ({} shards)",
            server.local_addr()?,
            server.store().shard_count()
        );
        server
            .run(async {
                let _ = tokio::signal::ctrl_c().await;
                eprintln!("shutting down");
            })
            .await
    });
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}
