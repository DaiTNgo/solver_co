use std::{
    future::Future,
    path::{Path, PathBuf},
    process,
    sync::Arc,
    thread::available_parallelism,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use clap::Parser;
use futures::future::join_all;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use tokio::time::Instant;

const BASE_URL: &str = "https://vuaco.app";

// ─── CLI ────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(name = "crawl", about = "Crawl vuaco.app tournament analysis data")]
struct Args {
    /// Output directory
    #[arg(short, long, default_value = "output")]
    output: PathBuf,

    /// Max parallel game workers (default: CPU core count)
    #[arg(short, long)]
    concurrency: Option<usize>,

    /// Skip games whose output file already exists
    #[arg(short, long, default_value_t = true)]
    skip_existing: bool,

    /// Rebuild game index cache from API instead of using local JSON
    #[arg(long, default_value_t = false)]
    refresh_index: bool,

    /// Path to cached game index JSON (default: <output>/_cache/games_index.json)
    #[arg(long)]
    index_cache: Option<PathBuf>,

    /// Only fetch/cache tournaments-editions-games index, skip analysis requests
    #[arg(long, default_value_t = false)]
    index_only: bool,

    /// Global API pacing interval in milliseconds (limit is 60 req / 60s)
    #[arg(long, default_value_t = 1200)]
    api_spacing_ms: u64,

    /// Disable API spacing delay (useful for load/concurrency testing)
    #[arg(long, default_value_t = false)]
    disable_api_spacing: bool,

    /// Poll interval in seconds for async analysis jobs
    #[arg(long, default_value_t = 5)]
    poll_interval_secs: u64,

    /// Optional chunk size. When set, chunks run sequentially but items inside a chunk can run concurrently.
    #[arg(long)]
    chunk_size: Option<usize>,

    /// Instance offset for sharding (0-based)
    #[arg(long, default_value_t = 0)]
    offset: usize,

    /// Total number of parallel instances (for sharding)
    #[arg(long, default_value_t = 1)]
    total_instances: usize,
}

// ─── API response types ──────────────────────────────────────────────

#[derive(Deserialize)]
struct Tournament {
    key: String,
}

#[derive(Deserialize)]
struct TournamentDetail {
    editions: Vec<Edition>,
}

#[derive(Deserialize)]
struct Edition {
    year: Value,
}

#[derive(Deserialize)]
struct GamesResponse {
    #[serde(alias = "game")]
    games: Vec<Game>,
}

#[derive(Deserialize)]
struct Game {
    slug: String,
}

#[derive(Serialize, Deserialize, Clone)]
struct CachedGame {
    key: String,
    year: String,
    game_id: String,
}

#[derive(Serialize, Deserialize)]
struct GameIndexCache {
    base_url: String,
    generated_unix: u64,
    games: Vec<CachedGame>,
}

#[derive(Deserialize)]
struct ChallengeResponse {
    challenge: String,
    difficulty: u32,
}

#[derive(Serialize)]
struct SubmitPayload<'a> {
    game_id: &'a str,
    challenge: &'a str,
    nonce: &'a str,
}

#[derive(Deserialize)]
struct SubmitResponse {
    status: Option<String>,
    job_id: Option<String>,
    result: Option<Value>,
}

#[derive(Deserialize)]
struct JobPollResponse {
    status: String,
    result: Option<Value>,
    error: Option<String>,
    progress: Option<f64>,
}

// ─── PoW solver (CPU-bound, call via spawn_blocking) ─────────────────

fn solve_pow(challenge: String, difficulty: u32) -> String {
    let mut nonce: u64 = 0;
    loop {
        let input = format!("{challenge}{nonce}");
        let hash = Sha256::digest(input.as_bytes());
        let mut zeros = 0u32;
        for &b in hash.as_slice() {
            if b == 0 {
                zeros += 8;
            } else {
                zeros += b.leading_zeros();
                break;
            }
        }
        if zeros >= difficulty {
            return nonce.to_string();
        }
        nonce += 1;
    }
}

async fn wait_for_api_slot(api_slot: &Arc<Mutex<Instant>>, spacing: Duration) -> Duration {
    if spacing == Duration::ZERO {
        return Duration::ZERO;
    }

    let sleep_dur = {
        let mut next = api_slot.lock().await;
        let now = Instant::now();
        let slot = if *next > now { *next } else { now };
        *next = slot + spacing;
        slot.duration_since(now)
    };

    if sleep_dur > Duration::ZERO {
        tokio::time::sleep(sleep_dur).await;
    }

    sleep_dur
}

async fn send_with_rate_limit_retry<F, Fut>(
    label: &str,
    api_slot: &Arc<Mutex<Instant>>,
    api_lane: &Arc<Mutex<()>>,
    use_api_lane: bool,
    spacing: Duration,
    mut send: F,
) -> Result<(reqwest::StatusCode, Vec<u8>)>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = reqwest::Result<reqwest::Response>>,
{
    const MAX_429_RETRIES: usize = 6;

    let mut attempt = 0usize;
    loop {
        let (status, bytes, retry_after, elapsed) = if use_api_lane {
            // Serialize API calls only when item-level waiting mode is enabled.
            let _lane = api_lane.lock().await;
            let delay = wait_for_api_slot(api_slot, spacing).await;
            if delay > Duration::ZERO {
                eprintln!("[api][delay] {label}: wait {}ms", delay.as_millis());
            }
            eprintln!("[api][call] {label}: attempt {}", attempt + 1);

            let started = Instant::now();
            let resp = send()
                .await
                .with_context(|| format!("{label}: send request"))?;
            let status = resp.status();
            let retry_after = resp
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .map(Duration::from_secs);
            let bytes = resp
                .bytes()
                .await
                .with_context(|| format!("{label}: read response body"))?
                .to_vec();
            let elapsed = started.elapsed();
            (status, bytes, retry_after, elapsed)
        } else {
            let delay = wait_for_api_slot(api_slot, spacing).await;
            if delay > Duration::ZERO {
                eprintln!("[api][delay] {label}: wait {}ms", delay.as_millis());
            }
            eprintln!("[api][call] {label}: attempt {}", attempt + 1);

            let started = Instant::now();
            let resp = send()
                .await
                .with_context(|| format!("{label}: send request"))?;
            let status = resp.status();
            let retry_after = resp
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .map(Duration::from_secs);
            let bytes = resp
                .bytes()
                .await
                .with_context(|| format!("{label}: read response body"))?
                .to_vec();
            let elapsed = started.elapsed();
            (status, bytes, retry_after, elapsed)
        };

        eprintln!(
            "[api][resp] {label}: {status} | {}ms | {} bytes",
            elapsed.as_millis(),
            bytes.len()
        );

        if status != reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Ok((status, bytes));
        }

        let body = String::from_utf8_lossy(&bytes);

        if attempt >= MAX_429_RETRIES {
            anyhow::bail!("{label} HTTP 429 after {MAX_429_RETRIES} retries: {body}");
        }

        let exp_secs = 2u64.saturating_pow((attempt as u32) + 1).min(30);
        let base_backoff = retry_after.unwrap_or(Duration::from_secs(exp_secs));
        let jitter_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| (d.as_millis() as u64) % 400)
            .unwrap_or(0);
        let wait = base_backoff + Duration::from_millis(jitter_ms);

        eprintln!(
            "[rate-limit] {label} got 429 (attempt {}/{}), backing off {:?}",
            attempt + 1,
            MAX_429_RETRIES,
            wait
        );

        tokio::time::sleep(wait).await;
        attempt += 1;
    }
}

// ─── Job polling ────────────────────────────────────────────────────

async fn poll_job(
    client: &Client,
    game_id: &str,
    job_id: &str,
    api_slot: Arc<Mutex<Instant>>,
    api_lane: Arc<Mutex<()>>,
    use_api_lane: bool,
    api_spacing: Duration,
    poll_interval: Duration,
) -> Result<Option<Value>> {
    let started = Instant::now();
    let timeout = Duration::from_secs(1800);

    loop {
        eprintln!(
            "[game:{game_id}] poll wait {}s for job {job_id}",
            poll_interval.as_secs()
        );
        tokio::time::sleep(poll_interval).await;

        if started.elapsed() > timeout {
            anyhow::bail!("analysis timed out (30 minutes)");
        }

        let url = format!("{BASE_URL}/api/analysis/{job_id}");
        let label = format!("[game:{game_id}] poll job {job_id}");
        let (status, bytes) = send_with_rate_limit_retry(
            &label,
            &api_slot,
            &api_lane,
            use_api_lane,
            api_spacing,
            || client.get(&url).send(),
        )
        .await?;
        if !status.is_success() {
            anyhow::bail!("poll HTTP {status}: {}", String::from_utf8_lossy(&bytes));
        }

        let poll: JobPollResponse =
            serde_json::from_slice(&bytes).context("parse poll response")?;

        match poll.status.as_str() {
            "done" => {
                eprintln!("[game:{game_id}] poll done for job {job_id}");
                return Ok(poll.result);
            }
            "error" => {
                anyhow::bail!("job error: {}", poll.error.unwrap_or_default());
            }
            _ => {
                eprintln!(
                    "[game:{game_id}] poll status={} progress={:?}",
                    poll.status, poll.progress
                );
            }
        }
    }
}

// ─── Metadata index (cache tournaments/editions/games) ──────────────

async fn fetch_game_index(
    client: &Client,
    api_slot: Arc<Mutex<Instant>>,
    api_lane: Arc<Mutex<()>>,
    use_api_lane: bool,
    api_spacing: Duration,
) -> Result<Vec<CachedGame>> {
    // 1) Fetch all tournament keys
    let tournaments: Vec<Tournament> = {
        let url = format!("{BASE_URL}/api/tournaments");
        eprintln!("[index] step 1: fetch tournaments list");
        let (status, bytes) = send_with_rate_limit_retry(
            "fetch tournaments",
            &api_slot,
            &api_lane,
            use_api_lane,
            api_spacing,
            || client.get(&url).send(),
        )
        .await?;
        if !status.is_success() {
            anyhow::bail!(
                "/tournaments HTTP {status}: {}",
                String::from_utf8_lossy(&bytes)
            );
        }
        serde_json::from_slice(&bytes).context("parse tournaments")?
    };

    // 2) Fetch editions for each tournament key concurrently
    let edition_futs = tournaments.into_iter().map(|t| {
        let client = client.clone();
        let api_slot = api_slot.clone();
        let api_lane = api_lane.clone();
        async move {
            let url = format!("{BASE_URL}/api/tournaments/{}", t.key);
            let label = format!("fetch tournament detail [{}]", t.key);
            let (status, bytes) = send_with_rate_limit_retry(
                &label,
                &api_slot,
                &api_lane,
                use_api_lane,
                api_spacing,
                || client.get(&url).send(),
            )
            .await?;
            if !status.is_success() {
                anyhow::bail!("HTTP {status}: {}", String::from_utf8_lossy(&bytes));
            }
            let detail: TournamentDetail = serde_json::from_slice(&bytes)?;
            Ok::<_, anyhow::Error>((t.key, detail.editions))
        }
    });
    let keys_editions: Vec<(String, Vec<Edition>)> = join_all(edition_futs)
        .await
        .into_iter()
        .filter_map(|r| r.map_err(|e| eprintln!("[editions] {e:#}")).ok())
        .collect();

    // 3) Fetch game lists for all key/year combos concurrently
    let game_futs = keys_editions
        .into_iter()
        .flat_map(|(key, editions)| {
            editions.into_iter().map(move |e| {
                let year = match &e.year {
                    Value::Number(n) => n.to_string(),
                    Value::String(s) => s.clone(),
                    v => v.to_string(),
                };
                (key.clone(), year)
            })
        })
        .map(|(key, year)| {
            let client = client.clone();
            let api_slot = api_slot.clone();
            let api_lane = api_lane.clone();
            async move {
                let url = format!("{BASE_URL}/api/tournaments/{key}/{year}");
                let label = format!("fetch tournament games [{key}/{year}]");
                let (status, bytes) = send_with_rate_limit_retry(
                    &label,
                    &api_slot,
                    &api_lane,
                    use_api_lane,
                    api_spacing,
                    || client.get(&url).send(),
                )
                .await?;
                if !status.is_success() {
                    anyhow::bail!(
                        "[{key}/{year}] HTTP {status}: {}",
                        String::from_utf8_lossy(&bytes)
                    );
                }
                let resp: GamesResponse = serde_json::from_slice(&bytes)?;
                Ok::<_, anyhow::Error>((key, year, resp.games))
            }
        });
    let all_games: Vec<(String, String, Vec<Game>)> = join_all(game_futs)
        .await
        .into_iter()
        .filter_map(|r| r.map_err(|e| eprintln!("[games] {e:#}")).ok())
        .collect();

    Ok(all_games
        .into_iter()
        .flat_map(|(key, year, games)| {
            games.into_iter().map(move |g| CachedGame {
                key: key.clone(),
                year: year.clone(),
                game_id: g.slug,
            })
        })
        .collect())
}

async fn load_or_fetch_game_index(
    client: &Client,
    cache_path: &Path,
    refresh: bool,
    api_slot: Arc<Mutex<Instant>>,
    api_lane: Arc<Mutex<()>>,
    use_api_lane: bool,
    api_spacing: Duration,
) -> Result<Vec<CachedGame>> {
    if !refresh && cache_path.exists() {
        match tokio::fs::read(cache_path).await {
            Ok(bytes) => match serde_json::from_slice::<GameIndexCache>(&bytes) {
                Ok(cache) => {
                    println!(
                        "using cached index: {} games ({})",
                        cache.games.len(),
                        cache_path.display()
                    );
                    return Ok(cache.games);
                }
                Err(e) => {
                    eprintln!(
                        "[cache] parse failed ({}), refetching: {e:#}",
                        cache_path.display()
                    );
                }
            },
            Err(e) => {
                eprintln!(
                    "[cache] read failed ({}), refetching: {e:#}",
                    cache_path.display()
                );
            }
        }
    }

    let games = fetch_game_index(client, api_slot, api_lane, use_api_lane, api_spacing).await?;

    if let Some(parent) = cache_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .context("create cache dir")?;
    }

    let generated_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let payload = GameIndexCache {
        base_url: BASE_URL.to_string(),
        generated_unix,
        games: games.clone(),
    };
    let bytes = serde_json::to_vec_pretty(&payload).context("serialize game index cache")?;
    tokio::fs::write(cache_path, &bytes)
        .await
        .context("write game index cache")?;

    println!(
        "saved fresh index: {} games ({})",
        games.len(),
        cache_path.display()
    );
    Ok(games)
}

// ─── Per-game pipeline ───────────────────────────────────────────────

async fn process_game(
    client: Arc<Client>,
    game_id: String,
    sem: Arc<tokio::sync::Semaphore>,
    api_slot: Arc<Mutex<Instant>>,
    api_lane: Arc<Mutex<()>>,
    use_api_lane: bool,
    api_spacing: Duration,
    poll_interval: Duration,
    output_path: PathBuf,
    skip_existing: bool,
) -> Result<()> {
    if skip_existing && output_path.exists() {
        eprintln!("[game:{game_id}] skip existing: {}", output_path.display());
        return Ok(());
    }

    let game_started = Instant::now();
    let _permit = sem.acquire().await?;
    eprintln!("[game:{game_id}] start");

    // Step 1: get challenge
    // Global API limiter (challenge/submit/poll share one lane)
    let ch = {
        let url = format!("{BASE_URL}/api/analysis-challenge");
        let challenge_label = format!("[game:{game_id}] fetch challenge");
        let (status, bytes) = send_with_rate_limit_retry(
            &challenge_label,
            &api_slot,
            &api_lane,
            use_api_lane,
            api_spacing,
            || client.get(&url).send(),
        )
        .await?;
        if !status.is_success() {
            anyhow::bail!(
                "challenge HTTP {status}: {}",
                String::from_utf8_lossy(&bytes)
            );
        }
        serde_json::from_slice::<ChallengeResponse>(&bytes).context("parse challenge")?
    };
    eprintln!(
        "[game:{game_id}] challenge received difficulty={}",
        ch.difficulty
    );

    let challenge_copy = ch.challenge.clone();
    let difficulty = ch.difficulty;

    // Step 2: solve PoW on a blocking thread so async runtime stays free
    let pow_started = Instant::now();
    eprintln!("[game:{game_id}] pow start (diff={difficulty})");
    let nonce = tokio::task::spawn_blocking(move || solve_pow(ch.challenge, difficulty))
        .await
        .context("spawn_blocking PoW")?;
    let pow_elapsed_ms = pow_started.elapsed().as_millis();
    eprintln!("[game:{game_id}] pow done in {}ms", pow_elapsed_ms);
    eprintln!(
        "[game:{game_id}] solve diff={difficulty} took {}ms (nonce={nonce})",
        pow_elapsed_ms
    );

    // Step 3: submit
    let submit_url = format!("{BASE_URL}/api/analysis");
    let submit_label = format!("[game:{game_id}] submit analysis");
    let (status, bytes) = send_with_rate_limit_retry(
        &submit_label,
        &api_slot,
        &api_lane,
        use_api_lane,
        api_spacing,
        || {
            client
                .post(&submit_url)
                .json(&SubmitPayload {
                    game_id: &game_id,
                    challenge: &challenge_copy,
                    nonce: &nonce,
                })
                .send()
        },
    )
    .await?;
    if !status.is_success() {
        anyhow::bail!("submit HTTP {status}: {}", String::from_utf8_lossy(&bytes));
    }
    let submit_raw_body = String::from_utf8_lossy(&bytes).to_string();
    let submit: SubmitResponse = serde_json::from_slice(&bytes).context("parse submit response")?;

    // Step 4: resolve result — immediate or via job polling
    let (result_source, raw_result) = if submit.status.as_deref() == Some("done") {
        eprintln!("[game:{game_id}] analysis done immediately");
        ("submit.done", submit.result)
    } else if let Some(job_id) = submit.job_id {
        eprintln!("[game:{game_id}] analysis queued job_id={job_id}");
        (
            "poll.done",
            poll_job(
                &client,
                &game_id,
                &job_id,
                api_slot.clone(),
                api_lane.clone(),
                use_api_lane,
                api_spacing,
                poll_interval,
            )
            .await?,
        )
    } else {
        anyhow::bail!(
            "unexpected submit response: {}",
            String::from_utf8_lossy(&bytes)
        );
    };

    let has_non_null_result = raw_result.is_some();
    if !has_non_null_result {
        let raw_value = match &raw_result {
            Some(v) => serde_json::to_string(v)
                .unwrap_or_else(|e| format!("<failed to serialize raw result: {e}>")),
            None => String::from("null"),
        };
        eprintln!("[game:{game_id}] NULL result from {result_source}: {raw_value}");
        if result_source == "submit.done" {
            eprintln!("[game:{game_id}] submit raw body: {submit_raw_body}");
        }
        eprintln!(
            "[game:{game_id}] elapsed before exit: {}ms",
            game_started.elapsed().as_millis()
        );
        eprintln!("[game:{game_id}] exiting application because null result was found");
        process::exit(2);
    }

    let result = raw_result.expect("checked non-null result above");

    if let Some(parent) = output_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .context("create output dir")?;
    }
    let out_bytes = serde_json::to_vec_pretty(&result).context("serialize result")?;
    tokio::fs::write(&output_path, &out_bytes)
        .await
        .context("write output")?;
    eprintln!("[game:{game_id}] saved: {}", output_path.display());
    eprintln!(
        "[game:{game_id}] done in {}ms",
        game_started.elapsed().as_millis()
    );

    Ok(())
}

// ─── Main ────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    anyhow::ensure!(args.total_instances > 0, "--total-instances must be >= 1");
    anyhow::ensure!(
        args.offset < args.total_instances,
        "--offset must be < --total-instances"
    );
    if !args.disable_api_spacing {
        anyhow::ensure!(
            args.api_spacing_ms >= 1000,
            "--api-spacing-ms must be >= 1000 for a 60 req/60s limit"
        );
    }
    anyhow::ensure!(
        args.poll_interval_secs >= 2,
        "--poll-interval-secs must be >= 2"
    );
    if let Some(chunk_size) = args.chunk_size {
        anyhow::ensure!(chunk_size > 0, "--chunk-size must be >= 1");
    }

    let concurrency = args
        .concurrency
        .unwrap_or_else(|| available_parallelism().map(|n| n.get()).unwrap_or(4));
    let use_api_lane = args.chunk_size.is_none();

    let client = Arc::new(
        Client::builder()
            .user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
            .default_headers({
                let mut h = reqwest::header::HeaderMap::new();
                h.insert("referer", "https://vuaco.app/tournaments".parse().unwrap());
                h.insert("origin", "https://vuaco.app".parse().unwrap());
                h
            })
            .build()?
    );
    let sem = Arc::new(tokio::sync::Semaphore::new(concurrency));

    let api_slot = Arc::new(Mutex::new(Instant::now()));
    let api_lane = Arc::new(Mutex::new(()));
    let api_spacing = if args.disable_api_spacing {
        Duration::ZERO
    } else {
        Duration::from_millis(args.api_spacing_ms)
    };
    let poll_interval = Duration::from_secs(args.poll_interval_secs);

    let index_cache_path = args
        .index_cache
        .clone()
        .unwrap_or_else(|| args.output.join("_cache").join("games_index.json"));

    let game_index = load_or_fetch_game_index(
        client.as_ref(),
        &index_cache_path,
        args.refresh_index,
        api_slot.clone(),
        api_lane.clone(),
        use_api_lane,
        api_spacing,
    )
    .await?;

    let chunk_size_display = args
        .chunk_size
        .map(|v| v.to_string())
        .unwrap_or_else(|| "off".to_string());
    let api_spacing_display = if args.disable_api_spacing {
        String::from("off")
    } else {
        format!("{}ms", args.api_spacing_ms)
    };

    println!(
        "concurrency: {concurrency} | chunk-size: {} | instance: {}/{} | api-spacing: {} | poll: {}s | index: {}",
        chunk_size_display,
        args.offset + 1,
        args.total_instances,
        api_spacing_display,
        args.poll_interval_secs,
        index_cache_path.display()
    );

    if args.disable_api_spacing {
        println!("mode: api-spacing disabled for concurrency testing");
    }

    if use_api_lane {
        println!("mode: no --chunk-size => items wait each other (strict sequential items)");
    } else {
        println!(
            "mode: --chunk-size enabled => chunks wait each other; items in chunk do not item-wait"
        );
    }

    if args.index_only {
        println!(
            "index-only: cached {} games, no analysis/challenge requests sent",
            game_index.len()
        );
        return Ok(());
    }

    // 4. Select this shard and skip files that already exist before any challenge call
    let mut skipped_existing_count = 0usize;
    let total_games = game_index.len();
    let queued_games: Vec<(String, PathBuf)> = game_index
        .into_iter()
        .enumerate()
        .filter(|(i, _)| i % args.total_instances == args.offset)
        .filter_map(|(_, g)| {
            let out = args
                .output
                .join(&g.key)
                .join(&g.year)
                .join(format!("{}.json", g.game_id));

            if args.skip_existing && out.exists() {
                skipped_existing_count += 1;
                None
            } else {
                Some((g.game_id, out))
            }
        })
        .collect();

    println!(
        "indexed: {total_games} | queued: {} | skipped-existing: {}",
        queued_games.len(),
        skipped_existing_count
    );

    if let Some(chunk_size) = args.chunk_size {
        // Chunk mode: chunks are sequential; items inside each chunk can run concurrently.
        let chunk_count = (queued_games.len() + chunk_size - 1) / chunk_size;
        for (chunk_idx, chunk) in queued_games.chunks(chunk_size).enumerate() {
            let chunk_started = Instant::now();
            println!(
                "[chunk {}/{}] start: {} items",
                chunk_idx + 1,
                chunk_count,
                chunk.len()
            );

            let tasks: Vec<_> = chunk
                .iter()
                .cloned()
                .map(|(game_id, out)| {
                    let client = client.clone();
                    let sem = sem.clone();
                    let api_slot = api_slot.clone();
                    let api_lane = api_lane.clone();
                    let skip = args.skip_existing;
                    tokio::spawn(async move {
                        if let Err(e) = process_game(
                            client,
                            game_id.clone(),
                            sem,
                            api_slot,
                            api_lane,
                            false,
                            api_spacing,
                            poll_interval,
                            out,
                            skip,
                        )
                        .await
                        {
                            eprintln!("✗ [{game_id}] {e:#}");
                        }
                    })
                })
                .collect();

            join_all(tasks).await;
            println!(
                "[chunk {}/{}] done in {}ms",
                chunk_idx + 1,
                chunk_count,
                chunk_started.elapsed().as_millis()
            );
        }
    } else {
        // No chunk-size: items run strictly one-by-one.
        let total_items = queued_games.len();
        for (idx, (game_id, out)) in queued_games.into_iter().enumerate() {
            println!("[item {}/{}] start: {}", idx + 1, total_items, game_id);

            if let Err(e) = process_game(
                client.clone(),
                game_id.clone(),
                sem.clone(),
                api_slot.clone(),
                api_lane.clone(),
                true,
                api_spacing,
                poll_interval,
                out,
                args.skip_existing,
            )
            .await
            {
                eprintln!("✗ [{game_id}] {e:#}");
            }
        }
    }

    Ok(())
}
