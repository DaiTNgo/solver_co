use std::{
    path::{Path, PathBuf},
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

// ─── Job polling ────────────────────────────────────────────────────

async fn poll_job(client: &Client, job_id: &str) -> Result<Option<Value>> {
    let started = Instant::now();
    let timeout = Duration::from_secs(600);
    let stall_limit = Duration::from_secs(60);
    let mut last_progress: Option<f64> = None;
    let mut last_progress_change = Instant::now();

    loop {
        tokio::time::sleep(Duration::from_secs(2)).await;

        if started.elapsed() > timeout {
            anyhow::bail!("analysis timed out (10 minutes)");
        }

        let resp = client
            .get(format!("{BASE_URL}/api/analysis/{job_id}"))
            .send()
            .await
            .context("poll job")?;
        let status = resp.status();
        let bytes = resp.bytes().await.context("poll job body")?;
        if !status.is_success() {
            anyhow::bail!("poll HTTP {status}: {}", String::from_utf8_lossy(&bytes));
        }

        let poll: JobPollResponse =
            serde_json::from_slice(&bytes).context("parse poll response")?;

        match poll.status.as_str() {
            "done" => {
                return Ok(poll
                    .result
                    .filter(|v| v.as_array().is_some_and(|a| !a.is_empty())));
            }
            "error" => {
                anyhow::bail!("job error: {}", poll.error.unwrap_or_default());
            }
            _ => {}
        }

        if poll.progress != last_progress {
            last_progress = poll.progress;
            last_progress_change = Instant::now();
        } else if last_progress_change.elapsed() > stall_limit {
            anyhow::bail!("analysis stalled (no progress for 60s)");
        }
    }
}

// ─── Metadata index (cache tournaments/editions/games) ──────────────

async fn fetch_game_index(client: &Client) -> Result<Vec<CachedGame>> {
    // 1) Fetch all tournament keys
    let tournaments: Vec<Tournament> = client
        .get(format!("{BASE_URL}/api/tournaments"))
        .send()
        .await
        .context("fetch /tournaments")?
        .json()
        .await
        .context("parse tournaments")?;

    // 2) Fetch editions for each tournament key concurrently
    let edition_futs = tournaments.into_iter().map(|t| {
        let client = client.clone();
        async move {
            let resp = client
                .get(format!("{BASE_URL}/api/tournaments/{}", t.key))
                .send()
                .await?;
            let status = resp.status();
            let bytes = resp.bytes().await?;
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
            async move {
                let url = format!("{BASE_URL}/api/tournaments/{key}/{year}");
                let resp = client.get(&url).send().await?;
                let status = resp.status();
                let bytes = resp.bytes().await?;
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

    let games = fetch_game_index(client).await?;

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
    challenge_slot: Arc<Mutex<Instant>>,
    output_path: PathBuf,
    skip_existing: bool,
) -> Result<()> {
    if skip_existing && output_path.exists() {
        return Ok(());
    }

    let _permit = sem.acquire().await?;

    // Step 1: get challenge
    // Reserve a time slot (1100ms apart) so we never exceed 55 req/min
    let ch = {
        let sleep_dur = {
            let mut next = challenge_slot.lock().await;
            let now = Instant::now();
            let slot = if *next > now { *next } else { now };
            *next = slot + Duration::from_millis(1100);
            slot.duration_since(now)
        };
        if sleep_dur > Duration::ZERO {
            tokio::time::sleep(sleep_dur).await;
        }
        let resp = client
            .get(format!("{BASE_URL}/api/analysis-challenge"))
            .send()
            .await
            .context("fetch challenge")?;
        let status = resp.status();
        let bytes = resp.bytes().await.context("fetch challenge body")?;
        if !status.is_success() {
            anyhow::bail!(
                "challenge HTTP {status}: {}",
                String::from_utf8_lossy(&bytes)
            );
        }
        serde_json::from_slice::<ChallengeResponse>(&bytes).context("parse challenge")?
    };

    let challenge_copy = ch.challenge.clone();

    // Step 2: solve PoW on a blocking thread so async runtime stays free
    let nonce = tokio::task::spawn_blocking(move || solve_pow(ch.challenge, ch.difficulty))
        .await
        .context("spawn_blocking PoW")?;

    // Step 3: submit
    let resp = client
        .post(format!("{BASE_URL}/api/analysis"))
        .json(&SubmitPayload {
            game_id: &game_id,
            challenge: &challenge_copy,
            nonce: &nonce,
        })
        .send()
        .await
        .context("submit solution")?;
    let status = resp.status();
    let bytes = resp.bytes().await.context("read submit response")?;
    if !status.is_success() {
        anyhow::bail!("submit HTTP {status}: {}", String::from_utf8_lossy(&bytes));
    }
    let submit: SubmitResponse = serde_json::from_slice(&bytes).context("parse submit response")?;

    // Step 4: resolve result — immediate or via job polling
    let result = if submit.status.as_deref() == Some("done") {
        submit
            .result
            .filter(|v| v.as_array().is_some_and(|a| !a.is_empty()))
    } else if let Some(job_id) = submit.job_id {
        poll_job(&client, &job_id).await?
    } else {
        anyhow::bail!(
            "unexpected submit response: {}",
            String::from_utf8_lossy(&bytes)
        );
    };

    // Only save when there is actual analysis data
    let Some(result) = result else {
        return Ok(());
    };

    if let Some(parent) = output_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .context("create output dir")?;
    }
    let out_bytes = serde_json::to_vec_pretty(&result).context("serialize result")?;
    tokio::fs::write(&output_path, &out_bytes)
        .await
        .context("write output")?;

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

    let concurrency = args
        .concurrency
        .unwrap_or_else(|| available_parallelism().map(|n| n.get()).unwrap_or(4));

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

    // Slot-based rate limiter: space challenge requests 1100ms apart (≈55/min)
    let challenge_slot = Arc::new(Mutex::new(Instant::now()));

    let index_cache_path = args
        .index_cache
        .clone()
        .unwrap_or_else(|| args.output.join("_cache").join("games_index.json"));

    let game_index =
        load_or_fetch_game_index(client.as_ref(), &index_cache_path, args.refresh_index).await?;

    println!(
        "concurrency: {concurrency} | instance: {}/{} | index: {}",
        args.offset + 1,
        args.total_instances,
        index_cache_path.display()
    );

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

    // 5. Process queued games with bounded concurrency (semaphore)
    let tasks: Vec<_> = queued_games
        .into_iter()
        .map(|(game_id, out)| {
            let client = client.clone();
            let sem = sem.clone();
            let challenge_slot = challenge_slot.clone();
            let skip = args.skip_existing;
            tokio::spawn(async move {
                if let Err(e) =
                    process_game(client, game_id.clone(), sem, challenge_slot, out, skip).await
                {
                    eprintln!("✗ [{game_id}] {e:#}");
                }
            })
        })
        .collect();

    join_all(tasks).await;
    Ok(())
}
