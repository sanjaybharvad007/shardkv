//! Network load generator.
//!
//! Two modes:
//!
//! * **Closed loop** (default): every connection sends a batch of `--pipeline`
//!   requests, waits for all the replies, and repeats. This measures maximum
//!   throughput. Its latency figures are optimistic: a connection that is
//!   waiting on a slow reply sends nothing, so the slow periods are
//!   under-sampled (coordinated omission).
//! * **Open loop** (`--rate N`): requests are sent on a fixed schedule whether
//!   or not earlier replies have arrived, and latency is measured from each
//!   request's *scheduled* send time. Use this mode for latency numbers. The
//!   schedule is one small burst per connection every millisecond, driven
//!   by an OS thread and staggered across connections; see `ticker`.

use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Buf, BytesMut};
use hdrhistogram::Histogram;
use shardkv::protocol::{split_frame, Request, DEFAULT_MAX_FRAME};
use shardkv::Client;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

const USAGE: &str = "\
shardkv-bench - load generator for shardkv-server

  --addr HOST:PORT     server address                          [127.0.0.1:7878]
  --connections N      concurrent connections                  [32]
  --pipeline N         requests per batch (closed loop)        [1]
  --rate N             total requests/s; enables open loop     [0 = closed loop]
  --duration SECS      measured time                           [10]
  --warmup SECS        unmeasured time before it               [2]
  --keys N             key space, preloaded before the run     [100000]
  --value-size BYTES   value size                              [128]
  --read-pct P         percentage of GETs, the rest are SETs   [90]
  --label TEXT         label for the CSV row                   [run]
  --csv FILE           append a result row to FILE
";

#[derive(Clone)]
struct Opts {
    addr: String,
    connections: usize,
    pipeline: usize,
    rate: f64,
    duration: Duration,
    warmup: Duration,
    keys: u64,
    value_size: usize,
    read_pct: u64,
    label: String,
    csv: Option<String>,
}

fn parse_args() -> Result<Opts, String> {
    let mut o = Opts {
        addr: "127.0.0.1:7878".into(),
        connections: 32,
        pipeline: 1,
        rate: 0.0,
        duration: Duration::from_secs(10),
        warmup: Duration::from_secs(2),
        keys: 100_000,
        value_size: 128,
        read_pct: 90,
        label: "run".into(),
        csv: None,
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
        let num = || {
            value
                .parse::<f64>()
                .map_err(|_| format!("{flag}: `{value}` is not a number"))
        };
        match flag.as_str() {
            "--addr" => o.addr = value,
            "--connections" => o.connections = num()? as usize,
            "--pipeline" => o.pipeline = num()? as usize,
            "--rate" => o.rate = num()?,
            "--duration" => o.duration = Duration::from_secs_f64(num()?),
            "--warmup" => o.warmup = Duration::from_secs_f64(num()?),
            "--keys" => o.keys = num()? as u64,
            "--value-size" => o.value_size = num()? as usize,
            "--read-pct" => o.read_pct = num()? as u64,
            "--label" => o.label = value,
            "--csv" => o.csv = Some(value),
            _ => return Err(format!("unknown option {flag}")),
        }
    }
    if o.connections == 0 || o.pipeline == 0 || o.keys == 0 {
        return Err("--connections, --pipeline and --keys must be at least 1".into());
    }
    if o.read_pct > 100 {
        return Err("--read-pct must be between 0 and 100".into());
    }
    Ok(o)
}

/// xorshift64*
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
}

fn key(i: u64) -> [u8; 16] {
    let mut k = *b"key:000000000000";
    k[8..].copy_from_slice(&i.to_be_bytes());
    k
}

fn encode_random(rng: &mut Rng, o: &Opts, value: &[u8], out: &mut BytesMut) {
    let r = rng.next();
    let k = key(r % o.keys);
    if (r >> 40) % 100 < o.read_pct {
        Request::Get { key: &k }.encode(out);
    } else {
        Request::Set { key: &k, value }.encode(out);
    }
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Latencies in microseconds, 1 us to 60 s, three significant figures.
fn new_histogram() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 60_000_000, 3).expect("valid histogram bounds")
}

struct Timeline {
    measure_from: Instant,
    end: Instant,
}

/// Reads until `n` complete responses have been consumed from the stream.
async fn read_responses(
    sock: &mut (impl AsyncReadExt + Unpin),
    rbuf: &mut BytesMut,
    mut n: usize,
) -> Result<(), BoxError> {
    loop {
        while n > 0 {
            match split_frame(rbuf, DEFAULT_MAX_FRAME)? {
                Some(frame) => {
                    let consumed = frame.consumed;
                    rbuf.advance(consumed);
                    n -= 1;
                }
                None => break,
            }
        }
        if n == 0 {
            return Ok(());
        }
        if sock.read_buf(rbuf).await? == 0 {
            return Err("server closed the connection".into());
        }
    }
}

async fn closed_loop(o: Arc<Opts>, id: u64, t: Arc<Timeline>) -> Result<Histogram<u64>, BoxError> {
    let mut sock = TcpStream::connect(&o.addr).await?;
    sock.set_nodelay(true)?;
    let value = vec![b'v'; o.value_size];
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ (id + 1));
    let mut hist = new_histogram();
    let (mut wbuf, mut rbuf) = (BytesMut::new(), BytesMut::new());
    loop {
        let sent = Instant::now();
        if sent >= t.end {
            return Ok(hist);
        }
        wbuf.clear();
        for _ in 0..o.pipeline {
            encode_random(&mut rng, &o, &value, &mut wbuf);
        }
        sock.write_all(&wbuf).await?;
        read_responses(&mut sock, &mut rbuf, o.pipeline).await?;
        if sent >= t.measure_from {
            // Every request in the batch waited for the whole batch.
            let micros = sent.elapsed().as_micros() as u64;
            hist.record_n(micros.max(1), o.pipeline as u64)?;
        }
    }
}

async fn open_loop(
    o: Arc<Opts>,
    id: u64,
    t: Arc<Timeline>,
    mut ticks: mpsc::UnboundedReceiver<Instant>,
) -> Result<Histogram<u64>, BoxError> {
    let sock = TcpStream::connect(&o.addr).await?;
    sock.set_nodelay(true)?;
    let (mut rd, mut wr) = sock.into_split();
    let per_conn = o.rate / o.connections as f64;
    // Scheduled send time of each request in flight, in send order. Replies
    // come back in the same order, so the reader pairs them up one for one.
    let (due_tx, mut due_rx) = mpsc::unbounded_channel::<Instant>();

    let reader = tokio::spawn(async move {
        let mut hist = new_histogram();
        let mut rbuf = BytesMut::with_capacity(64 * 1024);
        loop {
            while let Some(frame) = split_frame(&rbuf, DEFAULT_MAX_FRAME)? {
                let consumed = frame.consumed;
                rbuf.advance(consumed);
                let scheduled = due_rx.try_recv().map_err(|_| "reply without a request")?;
                if scheduled >= t.measure_from {
                    hist.record((scheduled.elapsed().as_micros() as u64).max(1))?;
                }
            }
            if rd.read_buf(&mut rbuf).await? == 0 {
                return Ok::<_, BoxError>(hist); // server closed after our FIN
            }
        }
    });

    let value = vec![b'v'; o.value_size];
    let mut rng = Rng(0xD1B5_4A32_D192_ED03 ^ (id + 1));
    let mut wbuf = BytesMut::new();

    // One tick arrives per millisecond, carrying the ideal time of that tick.
    // By the end of tick k the connection must have sent
    // `k * per_conn / 1000` requests, and each request's scheduled time is
    // its tick's ideal time. If this task or the server falls behind, ticks
    // queue up with their original times, so the delay is measured, not hidden.
    let mut sent = 0u64;
    let mut k = 0u64;
    while let Some(scheduled) = ticks.recv().await {
        k += 1;
        let due = (k as f64 * per_conn / 1000.0) as u64;
        if due <= sent {
            continue;
        }
        wbuf.clear();
        for _ in sent..due {
            let _ = due_tx.send(scheduled);
            encode_random(&mut rng, &o, &value, &mut wbuf);
        }
        sent = due;
        wr.write_all(&wbuf).await?;
    }
    wr.shutdown().await?; // FIN: the server finishes its replies, then closes
    drop(due_tx);
    reader.await?
}

/// Drives the open-loop schedule from a dedicated OS thread.
///
/// Tokio's timers have 1 ms resolution and fire up to a millisecond late,
/// which would show up as a millisecond of fake latency on every request. An
/// OS sleep is accurate to roughly 0.1 ms. The thread wakes every 250 us and
/// ticks a quarter of the connections each time, so every connection gets one
/// tick per millisecond and the connections do not all write at once.
fn ticker(start: Instant, end: Instant, conns: Vec<mpsc::UnboundedSender<Instant>>) {
    const PHASES: usize = 4;
    let step = Duration::from_micros(1000 / PHASES as u64);
    for k in 0u32.. {
        let ideal = start + step * k;
        if ideal >= end {
            break;
        }
        std::thread::sleep(ideal.saturating_duration_since(Instant::now()));
        for conn in conns.iter().skip(k as usize % PHASES).step_by(PHASES) {
            let _ = conn.send(ideal);
        }
    }
    // Dropping the senders ends each connection's schedule.
}

async fn preload(o: &Opts) -> Result<(), BoxError> {
    let mut c = Client::connect(&o.addr).await?;
    let value = vec![b'v'; o.value_size];
    let mut i = 0;
    while i < o.keys {
        let end = (i + 512).min(o.keys);
        for k in i..end {
            c.queue_set(&key(k), &value)?;
        }
        c.flush().await?;
        i = end;
    }
    Ok(())
}

async fn run(o: Opts) -> Result<(), BoxError> {
    preload(&o).await?;
    let o = Arc::new(o);
    let start = Instant::now();
    let timeline = Arc::new(Timeline {
        measure_from: start + o.warmup,
        end: start + o.warmup + o.duration,
    });
    let mut tasks = Vec::with_capacity(o.connections);
    let mut tick_senders = Vec::new();
    for id in 0..o.connections as u64 {
        let (o, t) = (Arc::clone(&o), Arc::clone(&timeline));
        tasks.push(if o.rate > 0.0 {
            let (tx, rx) = mpsc::unbounded_channel();
            tick_senders.push(tx);
            tokio::spawn(open_loop(o, id, t, rx))
        } else {
            tokio::spawn(closed_loop(o, id, t))
        });
    }
    if o.rate > 0.0 {
        let end = timeline.end;
        std::thread::spawn(move || ticker(start, end, tick_senders));
    }
    let mut hist = new_histogram();
    for task in tasks {
        hist.add(task.await??)?;
    }

    let throughput = hist.len() as f64 / o.duration.as_secs_f64();
    let ms = |q: f64| hist.value_at_quantile(q) as f64 / 1000.0;
    let mode = if o.rate > 0.0 { "open" } else { "closed" };
    println!(
        "{mode} loop  conns={} pipeline={} read%={} value={}B",
        o.connections, o.pipeline, o.read_pct, o.value_size
    );
    println!(
        "throughput {throughput:>10.0} req/s   p50 {:.3} ms   p99 {:.3} ms   p99.9 {:.3} ms   max {:.3} ms",
        ms(0.50),
        ms(0.99),
        ms(0.999),
        hist.max() as f64 / 1000.0
    );
    if let Some(path) = &o.csv {
        use std::io::Write;
        let fresh = !std::path::Path::new(path).exists();
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        if fresh {
            writeln!(f, "label,mode,connections,pipeline,rate,read_pct,value_size,throughput,p50_ms,p99_ms,p999_ms,max_ms")?;
        }
        writeln!(
            f,
            "{},{mode},{},{},{},{},{},{throughput:.0},{:.3},{:.3},{:.3},{:.3}",
            o.label.replace(',', ";"),
            o.connections,
            o.pipeline,
            o.rate,
            o.read_pct,
            o.value_size,
            ms(0.50),
            ms(0.99),
            ms(0.999),
            hist.max() as f64 / 1000.0
        )?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    let opts = match parse_args() {
        Ok(o) => o,
        Err(e) => {
            eprintln!("error: {e}\n\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    match run(opts).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}
