use dioxus::prelude::*;
use serde::{Deserialize, Serialize};

#[cfg(feature = "server")]
use shared::download::{
    AutoDownloadEvent, DownloadEvent, DownloadProgress, DownloadableGroup, SearchState,
};

#[cfg(feature = "server")]
use dioxus::logger::tracing::{info, warn};
#[cfg(feature = "server")]
use std::sync::Arc;
#[cfg(feature = "server")]
use std::time::Duration;

#[cfg(feature = "server")]
use crate::globals::{get_or_create_user_channel, register_user_task, unregister_user_task};
#[cfg(feature = "server")]
use crate::services::{available_download_backends, download_backend, navidrome_client_for_user};
#[cfg(feature = "server")]
use crate::AuthSession;

#[cfg(feature = "server")]
use super::download::monitor::DownloadMonitor;

#[cfg(feature = "server")]
use soulbeet::NavidromeClient;
#[cfg(feature = "server")]
use std::collections::HashMap;
#[cfg(feature = "server")]
use tokio::sync::{RwLock, Semaphore};

/// Score threshold — same as auto_download
#[cfg(feature = "server")]
const AUTO_SELECT_SCORE_THRESHOLD: f64 = 0.7;
#[cfg(feature = "server")]
const SEARCH_TIMEOUT: Duration = Duration::from_secs(120);
#[cfg(feature = "server")]
const SEARCH_POLL_INTERVAL: Duration = Duration::from_secs(2);
/// Delay between queuing individual tracks to avoid flooding slskd
#[cfg(feature = "server")]
const BATCH_DELAY: Duration = Duration::from_millis(500);
/// Maximum concurrent searches against slskd to avoid overloading it.
/// Downloads/monitors can run concurrently beyond this limit.
#[cfg(feature = "server")]
const MAX_CONCURRENT_SEARCHES: usize = 3;
/// Delay after a track's download monitor completes before querying Navidrome
#[cfg(feature = "server")]
const POST_IMPORT_SETTLE_DELAY: Duration = Duration::from_secs(5);
/// How many times to retry Navidrome song lookup after download completes
#[cfg(feature = "server")]
const NAVIDROME_LOOKUP_RETRIES: u32 = 3;
/// Delay between Navidrome lookup retries
#[cfg(feature = "server")]
const NAVIDROME_RETRY_DELAY: Duration = Duration::from_secs(10);
/// Minimum cooldown between triggering Navidrome library scans
#[cfg(feature = "server")]
const SCAN_COOLDOWN: Duration = Duration::from_secs(30);

/// A single track parsed from a TuneMyMusic CSV export.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CsvTrackEntry {
    pub track_name: String,
    pub artist: String,
    pub album: String,
    /// Playlist name from column 4 (may be empty if not present)
    pub playlist_name: String,
    /// Row index from the original CSV (for UI display)
    pub row_index: usize,
}

/// Result of parsing a CSV — returned to the UI for preview before import.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CsvParseResult {
    pub tracks: Vec<CsvTrackEntry>,
    /// Unique playlist names found in the CSV
    pub playlists: Vec<String>,
    pub total_rows: usize,
    pub skipped_rows: usize,
}

/// Request to start a batch import from parsed CSV data.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CsvBatchImportRequest {
    pub tracks: Vec<CsvTrackEntry>,
    pub folder_id: String,
    pub folder_path: String,
}

/// Status of a single track in the import batch.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum CsvTrackStatus {
    Pending,
    Searching,
    Accepted { batch_id: String },
    Failed { error: String },
}

/// Progress update for the entire CSV import batch.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CsvImportProgress {
    pub total: usize,
    pub completed: usize,
    pub failed: usize,
    pub current_track: Option<CsvTrackEntry>,
    pub statuses: Vec<(CsvTrackEntry, CsvTrackStatus)>,
    /// Playlists that will be created after downloads complete
    pub playlists: Vec<String>,
    pub done: bool,
}

/// Parse a TuneMyMusic CSV on the server and return structured track data.
///
/// TuneMyMusic CSV format (comma-separated):
/// `Track name,Artist name,Album name,Playlist name`
///
/// The first row is a header. We extract track name, artist, and album.
#[post("/api/csv/parse", _: AuthSession)]
pub async fn parse_csv(csv_content: String) -> Result<CsvParseResult, ServerFnError> {
    let mut tracks = Vec::new();
    let mut skipped = 0;
    let lines: Vec<&str> = csv_content.lines().collect();

    if lines.is_empty() {
        return Err(super::server_error("CSV file is empty"));
    }

    // Detect header row — skip it if it looks like a header
    let start_idx = if looks_like_header(lines[0]) { 1 } else { 0 };

    for (i, line) in lines.iter().enumerate().skip(start_idx) {
        let fields = parse_csv_line(line);

        if fields.len() < 3 {
            skipped += 1;
            continue;
        }

        let track_name = fields[0].trim().to_string();
        let artist = fields[1].trim().to_string();
        let album = fields[2].trim().to_string();
        let playlist_name = fields.get(3).map(|s| s.trim().to_string()).unwrap_or_default();

        if track_name.is_empty() || artist.is_empty() {
            skipped += 1;
            continue;
        }

        tracks.push(CsvTrackEntry {
            track_name,
            artist,
            album,
            playlist_name,
            row_index: i,
        });
    }

    let total_rows = lines.len().saturating_sub(start_idx);

    // Collect unique playlist names (preserving order of first appearance)
    let mut playlists: Vec<String> = Vec::new();
    let mut seen_playlists = std::collections::HashSet::new();
    for t in &tracks {
        if !t.playlist_name.is_empty() && seen_playlists.insert(t.playlist_name.clone()) {
            playlists.push(t.playlist_name.clone());
        }
    }

    info!(
        "CSV parsed: {} tracks extracted, {} rows skipped out of {} data rows, {} playlists found",
        tracks.len(),
        skipped,
        total_rows,
        playlists.len()
    );

    Ok(CsvParseResult {
        tracks,
        playlists,
        total_rows,
        skipped_rows: skipped,
    })
}

/// Shared context for a CSV import batch. Created once, wrapped in Arc, and shared
/// across all concurrent track processing tasks.
#[cfg(feature = "server")]
struct CsvImportContext {
    backend_ids: Vec<String>,
    folder_path: std::path::PathBuf,
    username: String,
    tx: tokio::sync::broadcast::Sender<DownloadEvent>,
    cancellation_token: tokio_util::sync::CancellationToken,
    /// Navidrome client — None if Navidrome is not configured/reachable.
    /// Playlist creation is skipped gracefully when this is None.
    navidrome: Option<Arc<NavidromeClient>>,
    /// Cache of playlist_name (lowercased) → playlist_id.
    /// Pre-populated with existing Navidrome playlists; new ones added on demand.
    playlist_cache: RwLock<HashMap<String, String>>,
    /// Limits concurrent searches against slskd to avoid flooding.
    search_semaphore: Semaphore,
    /// Tracks last scan trigger time to avoid spamming Navidrome with scan requests.
    last_scan: tokio::sync::Mutex<Option<tokio::time::Instant>>,
    total_tracks: usize,
}

#[cfg(feature = "server")]
impl CsvImportContext {
    /// Ensure a playlist exists in Navidrome, creating it if needed.
    /// Returns the playlist_id, or None if Navidrome is unavailable or creation fails.
    /// Uses a double-checked locking pattern to avoid redundant API calls.
    async fn ensure_playlist(&self, playlist_name: &str) -> Option<String> {
        if playlist_name.is_empty() {
            return None;
        }
        let client = self.navidrome.as_ref()?;
        let cache_key = playlist_name.to_lowercase();

        // Fast path: check read-locked cache
        {
            let cache = self.playlist_cache.read().await;
            if let Some(id) = cache.get(&cache_key) {
                return Some(id.clone());
            }
        }

        // Slow path: acquire write lock and double-check (another task may have created it)
        let mut cache = self.playlist_cache.write().await;
        if let Some(id) = cache.get(&cache_key) {
            return Some(id.clone());
        }

        // Create new empty playlist in Navidrome
        match client.create_playlist(playlist_name, &[]).await {
            Ok(pl) => {
                info!(
                    "Created Navidrome playlist '{}' (id: {})",
                    playlist_name, pl.id
                );
                cache.insert(cache_key, pl.id.clone());
                Some(pl.id)
            }
            Err(e) => {
                warn!(
                    "Failed to create Navidrome playlist '{}': {}",
                    playlist_name, e
                );
                None
            }
        }
    }

    /// Trigger a Navidrome library scan if enough time has passed since the last one.
    /// Serialized via mutex to prevent concurrent scan triggers.
    /// Returns true if a scan was triggered and completed.
    async fn trigger_scan_if_needed(&self) -> bool {
        let client = match self.navidrome.as_ref() {
            Some(c) => c,
            None => return false,
        };

        let mut last = self.last_scan.lock().await;

        // Respect cooldown to avoid flooding Navidrome
        if let Some(last_time) = *last {
            if last_time.elapsed() < SCAN_COOLDOWN {
                return false;
            }
        }

        if let Err(e) = client.start_scan().await {
            warn!("Failed to trigger Navidrome scan: {}", e);
            return false;
        }

        *last = Some(tokio::time::Instant::now());
        drop(last); // Release lock while waiting for scan

        // Poll scan status until complete (max 2 minutes)
        let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
        loop {
            tokio::time::sleep(Duration::from_secs(3)).await;
            match client.get_scan_status().await {
                Ok(true) if tokio::time::Instant::now() < deadline => continue,
                _ => break,
            }
        }
        true
    }

    /// Search Navidrome for a track by title + artist, with retries and scan triggers.
    /// Returns the song_id if found.
    async fn find_song_in_navidrome(&self, track: &CsvTrackEntry) -> Option<String> {
        let client = self.navidrome.as_ref()?;
        let query = format!("{} {}", track.artist, track.track_name);
        let title_lower = track.track_name.to_lowercase();
        let artist_lower = track.artist.to_lowercase();

        for attempt in 0..NAVIDROME_LOOKUP_RETRIES {
            if self.cancellation_token.is_cancelled() {
                return None;
            }

            match client.search(&query).await {
                Ok(results) => {
                    // Best match: title substring match
                    let song = results
                        .song
                        .iter()
                        .find(|s| {
                            let s_title = s.title.to_lowercase();
                            s_title.contains(&title_lower) || title_lower.contains(&s_title)
                        })
                        .or_else(|| {
                            // Fallback: artist substring match
                            results.song.iter().find(|s| {
                                s.artist
                                    .as_deref()
                                    .map(|a| {
                                        let a_lower = a.to_lowercase();
                                        a_lower.contains(&artist_lower)
                                            || artist_lower.contains(&a_lower)
                                    })
                                    .unwrap_or(false)
                            })
                        })
                        .or_else(|| results.song.first());

                    if let Some(s) = song {
                        return Some(s.id.clone());
                    }
                }
                Err(e) => {
                    warn!(
                        "Navidrome search attempt {}/{} failed for '{}': {}",
                        attempt + 1,
                        NAVIDROME_LOOKUP_RETRIES,
                        track.track_name,
                        e
                    );
                }
            }

            // After first failed lookup, trigger a scan so Navidrome indexes new files
            if attempt == 0 {
                info!(
                    "Song '{}' not found in Navidrome, triggering scan and retrying",
                    track.track_name
                );
                self.trigger_scan_if_needed().await;
            }

            if attempt < NAVIDROME_LOOKUP_RETRIES - 1 {
                tokio::time::sleep(NAVIDROME_RETRY_DELAY).await;
            }
        }

        warn!(
            "Could not find '{}' by '{}' in Navidrome after {} attempts",
            track.track_name, track.artist, NAVIDROME_LOOKUP_RETRIES
        );
        None
    }

    /// Process a single CSV track through the full pipeline:
    /// ensure playlist → search → score → download → monitor → add to playlist.
    ///
    /// The search phase is gated by `search_semaphore` to avoid flooding slskd.
    /// The download monitor phase runs freely (lightweight polling).
    /// Playlist operations are safe for concurrent access via the RwLock cache.
    async fn process_track(self: &Arc<Self>, track: &CsvTrackEntry, idx: usize) {
        let query_desc = format!("{} - {}", &track.artist, &track.track_name);
        let batch_id = uuid::Uuid::new_v4().to_string();

        if self.cancellation_token.is_cancelled() {
            info!("CSV import cancelled, skipping '{}'", query_desc);
            return;
        }

        info!(
            "CSV import [{}/{}]: processing '{}'",
            idx + 1,
            self.total_tracks,
            query_desc
        );

        // --- Phase 1: Ensure playlist exists (lightweight, no semaphore needed) ---
        let playlist_id = self.ensure_playlist(&track.playlist_name).await;

        // --- Phase 2: Search + score + queue download (semaphore-gated) ---
        let permit = match self.search_semaphore.acquire().await {
            Ok(p) => p,
            Err(_) => {
                warn!("Search semaphore closed, aborting '{}'", query_desc);
                return;
            }
        };

        if self.cancellation_token.is_cancelled() {
            drop(permit);
            return;
        }

        let _ = self
            .tx
            .send(DownloadEvent::AutoDownload(AutoDownloadEvent::Searching {
                batch_id: batch_id.clone(),
                query: query_desc.clone(),
                backend_count: self.backend_ids.len(),
            }));

        // Collect available backends
        let mut backends = Vec::new();
        for id in &self.backend_ids {
            match download_backend(Some(id)).await {
                Ok(b) => backends.push((id.clone(), b)),
                Err(e) => warn!("Backend {} unavailable: {}", id, e),
            }
        }

        if backends.is_empty() {
            let _ = self.tx.send(DownloadEvent::AutoDownload(
                AutoDownloadEvent::Failed {
                    batch_id: batch_id.clone(),
                    error: "No download backends available".to_string(),
                },
            ));
            drop(permit);
            return;
        }

        // Build search query
        let search_track = shared::metadata::Track {
            id: format!("csv-import-{}", track.row_index),
            title: track.track_name.clone(),
            artist: track.artist.clone(),
            album_id: None,
            album_title: if track.album.is_empty() {
                None
            } else {
                Some(track.album.clone())
            },
            release_date: None,
            duration: None,
            mbid: None,
            release_mbid: None,
        };

        // Search all backends in parallel (within the semaphore gate)
        let search_futures: Vec<_> = backends
            .iter()
            .map(|(id, backend)| {
                let id = id.clone();
                let backend = Arc::clone(backend);
                let tracks_clone = vec![search_track.clone()];
                async move {
                    let search_id = match backend.start_search(None, &tracks_clone).await {
                        Ok(sid) => sid,
                        Err(e) => {
                            warn!("Backend {} search start failed: {}", id, e);
                            return (id, Vec::<DownloadableGroup>::new());
                        }
                    };

                    let deadline = tokio::time::Instant::now() + SEARCH_TIMEOUT;
                    loop {
                        if tokio::time::Instant::now() >= deadline {
                            warn!("Backend {} search timed out", id);
                            break;
                        }
                        tokio::time::sleep(SEARCH_POLL_INTERVAL).await;

                        match backend.poll_search(&search_id).await {
                            Ok(result) => match result.state {
                                SearchState::Completed | SearchState::TimedOut => {
                                    return (id, result.groups);
                                }
                                SearchState::NotFound => return (id, Vec::new()),
                                SearchState::InProgress => continue,
                            },
                            Err(e) => {
                                warn!("Backend {} poll error: {}", id, e);
                                return (id, Vec::new());
                            }
                        }
                    }
                    (id, Vec::new())
                }
            })
            .collect();

        let results = futures::future::join_all(search_futures).await;

        // Merge and sort by score
        let mut all_groups: Vec<DownloadableGroup> = results
            .into_iter()
            .flat_map(|(_, groups)| groups)
            .collect();

        if all_groups.is_empty() {
            let _ = self.tx.send(DownloadEvent::AutoDownload(
                AutoDownloadEvent::Failed {
                    batch_id: batch_id.clone(),
                    error: "No results found".to_string(),
                },
            ));
            warn!("CSV import: no results for '{}'", query_desc);
            drop(permit);
            return;
        }

        all_groups.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let best_score = all_groups[0].score;

        let _ = self.tx.send(DownloadEvent::AutoDownload(
            AutoDownloadEvent::ScoringResults {
                batch_id: batch_id.clone(),
                result_count: all_groups.len(),
                best_score,
            },
        ));

        // Lower threshold for CSV import — user explicitly requested these tracks
        if best_score < AUTO_SELECT_SCORE_THRESHOLD * 0.7 {
            let _ = self.tx.send(DownloadEvent::AutoDownload(
                AutoDownloadEvent::Failed {
                    batch_id: batch_id.clone(),
                    error: format!(
                        "Best match score {:.2} too low for '{}'",
                        best_score, query_desc
                    ),
                },
            ));
            warn!(
                "CSV import: score {:.2} too low for '{}'",
                best_score, query_desc
            );
            drop(permit);
            return;
        }

        let picked = all_groups.remove(0);

        let _ = self.tx.send(DownloadEvent::AutoDownload(
            AutoDownloadEvent::PickedSource {
                batch_id: batch_id.clone(),
                source: picked.source.clone(),
                score: picked.score,
                quality: picked.quality.clone(),
                track_count: picked.items.len(),
            },
        ));

        info!(
            "CSV import [{}/{}]: picked '{}' (score {:.2}, {}) for '{}'",
            idx + 1,
            self.total_tracks,
            picked.source,
            picked.score,
            picked.quality,
            query_desc
        );

        // Create target directory
        if let Err(e) = tokio::fs::create_dir_all(&self.folder_path).await {
            let _ = self.tx.send(DownloadEvent::AutoDownload(
                AutoDownloadEvent::Failed {
                    batch_id: batch_id.clone(),
                    error: format!("Failed to create target directory: {}", e),
                },
            ));
            drop(permit);
            return;
        }

        // Queue download
        let items = picked.items.clone();
        let backend = match download_backend(None).await {
            Ok(b) => b,
            Err(e) => {
                let _ = self.tx.send(DownloadEvent::AutoDownload(
                    AutoDownloadEvent::Failed {
                        batch_id: batch_id.clone(),
                        error: format!("Download backend not available: {}", e),
                    },
                ));
                drop(permit);
                return;
            }
        };

        let queued = match backend.download(items).await {
            Ok(q) => q,
            Err(e) => {
                let _ = self.tx.send(DownloadEvent::AutoDownload(
                    AutoDownloadEvent::Failed {
                        batch_id: batch_id.clone(),
                        error: format!("Download queue failed: {}", e),
                    },
                ));
                drop(permit);
                return;
            }
        };

        // Release search semaphore — download is queued, next search can start
        drop(permit);

        let batch_label = format!("{} - {}", track.artist, track.track_name);
        let (failed, successful): (Vec<_>, Vec<_>) =
            queued.iter().cloned().partition(|d| d.error.is_some());

        if !failed.is_empty() {
            let failed_entries: Vec<DownloadProgress> = failed
                .iter()
                .map(|d| {
                    DownloadProgress::failed(
                        d.id.clone(),
                        d.source.clone(),
                        d.item.clone(),
                        d.error.clone().unwrap_or_default(),
                    )
                    .with_batch(batch_id.clone(), batch_label.clone())
                })
                .collect();
            let _ = self.tx.send(DownloadEvent::Progress(failed_entries));
        }

        if successful.is_empty() {
            let _ = self.tx.send(DownloadEvent::AutoDownload(
                AutoDownloadEvent::Failed {
                    batch_id: batch_id.clone(),
                    error: "All downloads failed to queue".to_string(),
                },
            ));
            return;
        }

        let _ = self.tx.send(DownloadEvent::AutoDownload(
            AutoDownloadEvent::Downloading {
                batch_id: batch_id.clone(),
            },
        ));

        let queued_entries: Vec<DownloadProgress> = successful
            .iter()
            .map(|d| {
                DownloadProgress::queued(
                    d.id.clone(),
                    d.source.clone(),
                    d.item.clone(),
                    d.size,
                )
                .with_batch(batch_id.clone(), batch_label.clone())
            })
            .collect();
        let _ = self.tx.send(DownloadEvent::Progress(queued_entries));

        let download_sources: Vec<String> =
            successful.iter().map(|d| d.source.clone()).collect();
        let download_filenames: Vec<String> =
            successful.iter().map(|d| d.item.clone()).collect();

        // --- Phase 3: Monitor download (runs without semaphore) ---
        let task_cancellation = register_user_task(&self.username).await;

        let mut monitor = DownloadMonitor::new(
            download_sources,
            download_filenames,
            self.folder_path.clone(),
            self.tx.clone(),
            task_cancellation,
            self.username.clone(),
            Some(batch_id.clone()),
            Some(batch_label),
        );
        monitor.run().await;
        unregister_user_task(&self.username).await;

        // --- Phase 4: Add to playlist after download + beets import ---
        if let Some(ref pl_id) = playlist_id {
            if self.cancellation_token.is_cancelled() {
                return;
            }

            // Give beets import + Navidrome indexing time to settle
            tokio::time::sleep(POST_IMPORT_SETTLE_DELAY).await;

            if let Some(song_id) = self.find_song_in_navidrome(track).await {
                if let Some(client) = self.navidrome.as_ref() {
                    match client
                        .update_playlist_songs(pl_id, &[song_id.clone()])
                        .await
                    {
                        Ok(()) => {
                            info!(
                                "Added '{}' to playlist '{}' (song_id: {})",
                                track.track_name, track.playlist_name, song_id
                            );
                        }
                        Err(e) => {
                            warn!(
                                "Failed to add '{}' to playlist '{}': {}",
                                track.track_name, track.playlist_name, e
                            );
                        }
                    }
                }
            }
        }

        info!(
            "CSV import [{}/{}]: completed '{}'",
            idx + 1,
            self.total_tracks,
            query_desc
        );
    }
}

/// Start a batch import: spawns concurrent background tasks that each process a CSV track
/// through the full pipeline: ensure playlist → search → score → download → monitor →
/// add to playlist.
///
/// Concurrency is controlled by a semaphore (`MAX_CONCURRENT_SEARCHES`) to avoid
/// overloading slskd. Download monitors run freely beyond the semaphore limit.
/// Returns immediately with initial status; progress delivered via WebSocket events.
#[post("/api/csv/import", auth: AuthSession)]
pub async fn import_csv_batch(
    req: CsvBatchImportRequest,
) -> Result<CsvImportProgress, ServerFnError> {
    let username = auth.0.username.clone();
    let user_id = auth.0.sub.clone();
    let total = req.tracks.len();

    info!(
        "User {} starting CSV import of {} tracks to folder {}",
        username, total, req.folder_path
    );

    // Validate backends are available
    let backend_ids: Vec<String> = available_download_backends()
        .iter()
        .map(|(id, _)| id.to_string())
        .collect();

    if backend_ids.is_empty() {
        return Err(super::server_error("No download backends available"));
    }

    // Try to get Navidrome client — playlists only work if Navidrome is reachable.
    // Graceful degradation: if unavailable, tracks still download, playlists are skipped.
    let navidrome = match navidrome_client_for_user(&user_id).await {
        Ok(client) => {
            info!("Navidrome connected — playlist creation enabled");
            Some(client)
        }
        Err(e) => {
            warn!(
                "Navidrome not available ({}). Tracks will download but playlists won't be created.",
                e
            );
            None
        }
    };

    // Pre-populate playlist cache with existing Navidrome playlists to avoid
    // recreating playlists that already exist
    let initial_cache: HashMap<String, String> = if let Some(ref client) = navidrome {
        match client.get_playlists().await {
            Ok(playlists) => {
                let map: HashMap<String, String> = playlists
                    .into_iter()
                    .map(|pl| (pl.name.to_lowercase(), pl.id))
                    .collect();
                info!("Pre-loaded {} existing Navidrome playlists", map.len());
                map
            }
            Err(e) => {
                warn!("Failed to pre-fetch Navidrome playlists: {}", e);
                HashMap::new()
            }
        }
    } else {
        HashMap::new()
    };

    // Collect unique playlist names for the response
    let playlist_names: Vec<String> = {
        let mut names = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for t in &req.tracks {
            if !t.playlist_name.is_empty() && seen.insert(t.playlist_name.clone()) {
                names.push(t.playlist_name.clone());
            }
        }
        names
    };

    let tracks = req.tracks.clone();
    let statuses: Vec<(CsvTrackEntry, CsvTrackStatus)> = req
        .tracks
        .iter()
        .map(|t| (t.clone(), CsvTrackStatus::Pending))
        .collect();

    let (tx, _) = get_or_create_user_channel(&username).await;
    let cancellation_token = register_user_task(&username).await;

    let ctx = Arc::new(CsvImportContext {
        backend_ids,
        folder_path: std::path::PathBuf::from(&req.folder_path),
        username: username.clone(),
        tx,
        cancellation_token,
        navidrome,
        playlist_cache: RwLock::new(initial_cache),
        search_semaphore: Semaphore::new(MAX_CONCURRENT_SEARCHES),
        last_scan: tokio::sync::Mutex::new(None),
        total_tracks: total,
    });

    // Spawn the batch coordinator — manages all per-track tasks
    let batch_username = username.clone();
    tokio::spawn(async move {
        let mut handles = Vec::with_capacity(tracks.len());

        for (idx, track) in tracks.iter().enumerate() {
            if ctx.cancellation_token.is_cancelled() {
                info!(
                    "CSV import cancelled at track {}/{}",
                    idx + 1,
                    tracks.len()
                );
                break;
            }

            let ctx = Arc::clone(&ctx);
            let track = track.clone();

            handles.push(tokio::spawn(async move {
                ctx.process_track(&track, idx).await;
            }));

            // Small stagger between spawning tasks to spread out semaphore acquisition.
            // The semaphore provides the real concurrency control; this just prevents
            // thundering herd on startup.
            tokio::time::sleep(BATCH_DELAY).await;
        }

        // Wait for all track tasks to complete (or fail gracefully)
        for (idx, handle) in handles.into_iter().enumerate() {
            match handle.await {
                Ok(()) => {}
                Err(e) => {
                    // JoinError means the task panicked — log and continue
                    warn!("CSV import track {} task panicked: {}", idx + 1, e);
                }
            }
        }

        unregister_user_task(&batch_username).await;

        info!(
            "CSV import batch fully complete for user {}: all {} tracks processed",
            batch_username, total
        );
    });

    // Return immediately — downloads + playlist creation happen in background
    Ok(CsvImportProgress {
        total,
        completed: total,
        failed: 0,
        current_track: None,
        statuses,
        playlists: playlist_names,
        done: false,
    })
}

/// Parse a single CSV line, handling quoted fields properly.
#[cfg(feature = "server")]
fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            '"' if in_quotes => {
                // Check for escaped quote ""
                if chars.peek() == Some(&'"') {
                    current.push('"');
                    chars.next();
                } else {
                    in_quotes = false;
                }
            }
            '"' if !in_quotes && current.is_empty() => {
                in_quotes = true;
            }
            ',' if !in_quotes => {
                fields.push(current.clone());
                current.clear();
            }
            _ => {
                current.push(c);
            }
        }
    }
    fields.push(current);
    fields
}

/// Heuristic to detect a header row.
#[cfg(feature = "server")]
fn looks_like_header(line: &str) -> bool {
    let lower = line.to_lowercase();
    // TuneMyMusic headers typically contain these column names
    lower.contains("track name")
        || lower.contains("artist name")
        || lower.contains("album name")
        || lower.contains("playlist")
        || (lower.contains("track") && lower.contains("artist"))
        || (lower.contains("song") && lower.contains("artist"))
        || (lower.contains("title") && lower.contains("artist"))
}
