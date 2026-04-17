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

/// Start a batch import: spawns a background task that processes each CSV track
/// through the full search → score → download → import pipeline.
/// Returns immediately with initial status; progress delivered via WebSocket.
#[post("/api/csv/import", auth: AuthSession)]
pub async fn import_csv_batch(
    req: CsvBatchImportRequest,
) -> Result<CsvImportProgress, ServerFnError> {
    let username = auth.0.username.clone();
    let total = req.tracks.len();

    info!(
        "User {} starting CSV import of {} tracks to folder {}",
        username, total, req.folder_path
    );

    // Validate backends are available before accepting
    let backend_ids: Vec<String> = available_download_backends()
        .iter()
        .map(|(id, _)| id.to_string())
        .collect();

    if backend_ids.is_empty() {
        return Err(super::server_error("No download backends available"));
    }

    let folder_path = req.folder_path.clone();
    let tracks = req.tracks.clone();
    let task_username = username.clone();
    let user_id = auth.0.sub.clone();

    // Collect unique playlist names from the batch
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

    // Build initial status response
    let statuses: Vec<(CsvTrackEntry, CsvTrackStatus)> = req
        .tracks
        .iter()
        .map(|t| (t.clone(), CsvTrackStatus::Pending))
        .collect();

    // Clone playlist_names for the return value (original moves into spawned task)
    let return_playlists = playlist_names.clone();

    // Spawn background task for the entire batch
    tokio::spawn(async move {
        let (tx, _) = get_or_create_user_channel(&task_username).await;

        for (idx, track) in tracks.iter().enumerate() {
            let query_desc = format!("{} - {}", &track.artist, &track.track_name);
            let batch_id = uuid::Uuid::new_v4().to_string();

            info!(
                "CSV import [{}/{}]: searching for '{}'",
                idx + 1,
                tracks.len(),
                query_desc
            );

            // Send Searching event
            let _ = tx.send(DownloadEvent::AutoDownload(AutoDownloadEvent::Searching {
                batch_id: batch_id.clone(),
                query: query_desc.clone(),
                backend_count: backend_ids.len(),
            }));

            // Collect backends
            let mut backends = Vec::new();
            for id in &backend_ids {
                match download_backend(Some(id)).await {
                    Ok(b) => backends.push((id.clone(), b)),
                    Err(e) => {
                        warn!("Backend {} unavailable: {}", id, e);
                    }
                }
            }

            if backends.is_empty() {
                let _ = tx.send(DownloadEvent::AutoDownload(AutoDownloadEvent::Failed {
                    batch_id: batch_id.clone(),
                    error: "No download backends available".to_string(),
                }));
                continue;
            }

            // Build search query for this track
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
            let search_tracks = vec![search_track];

            // Search all backends in parallel
            let search_futures: Vec<_> = backends
                .iter()
                .map(|(id, backend)| {
                    let id = id.clone();
                    let backend = Arc::clone(backend);
                    let tracks_clone = search_tracks.clone();
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
                                    SearchState::NotFound => {
                                        return (id, Vec::new());
                                    }
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

            // Merge and sort results by score
            let mut all_groups: Vec<DownloadableGroup> = results
                .into_iter()
                .flat_map(|(_backend_id, groups)| groups)
                .collect();

            if all_groups.is_empty() {
                let _ = tx.send(DownloadEvent::AutoDownload(AutoDownloadEvent::Failed {
                    batch_id: batch_id.clone(),
                    error: "No results found".to_string(),
                }));
                warn!("CSV import: no results for '{}'", query_desc);
                if idx < tracks.len() - 1 {
                    tokio::time::sleep(BATCH_DELAY).await;
                }
                continue;
            }

            all_groups.sort_by(|a, b| {
                b.score
                    .partial_cmp(&a.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });

            let best_score = all_groups[0].score;

            let _ = tx.send(DownloadEvent::AutoDownload(
                AutoDownloadEvent::ScoringResults {
                    batch_id: batch_id.clone(),
                    result_count: all_groups.len(),
                    best_score,
                },
            ));

            // For CSV import we use a lower threshold — accept more results automatically
            // since the user has explicitly requested these tracks
            if best_score < AUTO_SELECT_SCORE_THRESHOLD * 0.7 {
                let _ = tx.send(DownloadEvent::AutoDownload(AutoDownloadEvent::Failed {
                    batch_id: batch_id.clone(),
                    error: format!(
                        "Best match score {:.2} too low for '{}'",
                        best_score, query_desc
                    ),
                }));
                warn!(
                    "CSV import: score {:.2} too low for '{}'",
                    best_score, query_desc
                );
                if idx < tracks.len() - 1 {
                    tokio::time::sleep(BATCH_DELAY).await;
                }
                continue;
            }

            // Pick best source
            let picked = all_groups.remove(0);

            let _ = tx.send(DownloadEvent::AutoDownload(
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
                tracks.len(),
                picked.source,
                picked.score,
                picked.quality,
                query_desc
            );

            let batch_label = format!("{} - {}", track.artist, track.track_name);

            // Create target directory
            let target_path_buf = std::path::Path::new(&folder_path).to_path_buf();
            if let Err(e) = tokio::fs::create_dir_all(&target_path_buf).await {
                let _ = tx.send(DownloadEvent::AutoDownload(AutoDownloadEvent::Failed {
                    batch_id: batch_id.clone(),
                    error: format!("Failed to create target directory: {}", e),
                }));
                continue;
            }

            // Queue download
            let items = picked.items.clone();
            let backend = match download_backend(None).await {
                Ok(b) => b,
                Err(e) => {
                    let _ = tx.send(DownloadEvent::AutoDownload(AutoDownloadEvent::Failed {
                        batch_id: batch_id.clone(),
                        error: format!("Download backend not available: {}", e),
                    }));
                    continue;
                }
            };

            let queued = match backend.download(items).await {
                Ok(q) => q,
                Err(e) => {
                    let _ = tx.send(DownloadEvent::AutoDownload(AutoDownloadEvent::Failed {
                        batch_id: batch_id.clone(),
                        error: format!("Download queue failed: {}", e),
                    }));
                    continue;
                }
            };

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
                let _ = tx.send(DownloadEvent::Progress(failed_entries));
            }

            if successful.is_empty() {
                let _ = tx.send(DownloadEvent::AutoDownload(AutoDownloadEvent::Failed {
                    batch_id: batch_id.clone(),
                    error: "All downloads failed to queue".to_string(),
                }));
                if idx < tracks.len() - 1 {
                    tokio::time::sleep(BATCH_DELAY).await;
                }
                continue;
            }

            // Send Downloading event
            let _ = tx.send(DownloadEvent::AutoDownload(AutoDownloadEvent::Downloading {
                batch_id: batch_id.clone(),
            }));

            let queued_entries: Vec<DownloadProgress> = successful
                .iter()
                .map(|d| {
                    DownloadProgress::queued(d.id.clone(), d.source.clone(), d.item.clone(), d.size)
                        .with_batch(batch_id.clone(), batch_label.clone())
                })
                .collect();
            let _ = tx.send(DownloadEvent::Progress(queued_entries));

            let download_sources: Vec<String> =
                successful.iter().map(|d| d.source.clone()).collect();
            let download_filenames: Vec<String> =
                successful.iter().map(|d| d.item.clone()).collect();

            // Spawn monitor for this track's downloads
            let monitor_tx = tx.clone();
            let monitor_username = task_username.clone();
            let monitor_batch_id = batch_id.clone();
            let monitor_batch_label = batch_label.clone();
            let monitor_target = target_path_buf.clone();

            let task_cancellation = register_user_task(&monitor_username).await;

            tokio::spawn(async move {
                let mut monitor = DownloadMonitor::new(
                    download_sources,
                    download_filenames,
                    monitor_target,
                    monitor_tx,
                    task_cancellation,
                    monitor_username.clone(),
                    Some(monitor_batch_id),
                    Some(monitor_batch_label),
                );
                monitor.run().await;
                unregister_user_task(&monitor_username).await;
            });

            // Delay before processing next track
            if idx < tracks.len() - 1 {
                tokio::time::sleep(BATCH_DELAY).await;
            }
        }

        info!(
            "CSV import batch complete for user {}: {} tracks processed",
            task_username,
            tracks.len()
        );

        // --- Playlist creation phase ---
        // After all downloads are queued, wait for Navidrome to scan, then create playlists
        if !playlist_names.is_empty() {
            info!(
                "Starting playlist creation phase: {} playlists to create",
                playlist_names.len()
            );

            // Wait for downloads to settle and beets to import before searching Navidrome.
            // DownloadMonitor handles beets import + Navidrome scan, but we need to give
            // it time to finish. We wait a generous amount since tracks process in parallel.
            let wait_secs = 30 + (tracks.len() as u64 * 5);
            let wait_secs = wait_secs.min(600); // cap at 10 minutes
            info!(
                "Waiting {}s for downloads + beets import to settle before playlist creation",
                wait_secs
            );
            tokio::time::sleep(Duration::from_secs(wait_secs)).await;

            // Trigger a Navidrome library scan and wait for it to complete
            match navidrome_client_for_user(&user_id).await {
                Ok(client) => {
                    if let Err(e) = client.start_scan().await {
                        warn!("Failed to trigger Navidrome scan: {}", e);
                    } else {
                        // Poll scan status until complete (max 2 minutes)
                        let scan_deadline =
                            tokio::time::Instant::now() + Duration::from_secs(120);
                        loop {
                            tokio::time::sleep(Duration::from_secs(3)).await;
                            match client.get_scan_status().await {
                                Ok(true) if tokio::time::Instant::now() < scan_deadline => {
                                    continue;
                                }
                                _ => break,
                            }
                        }
                    }

                    // Fetch existing playlists to avoid duplicates
                    let existing_playlists = client.get_playlists().await.unwrap_or_default();

                    // Build playlist → tracks mapping
                    let mut playlist_tracks: std::collections::HashMap<String, Vec<&CsvTrackEntry>> =
                        std::collections::HashMap::new();
                    for track in &tracks {
                        if !track.playlist_name.is_empty() {
                            playlist_tracks
                                .entry(track.playlist_name.clone())
                                .or_default()
                                .push(track);
                        }
                    }

                    for (playlist_name, csv_tracks) in &playlist_tracks {
                        info!(
                            "Creating playlist '{}' with {} tracks",
                            playlist_name,
                            csv_tracks.len()
                        );

                        // Search Navidrome for each track's song ID
                        let mut song_ids: Vec<String> = Vec::new();
                        for csv_track in csv_tracks {
                            // Search by "artist title" to get best match
                            let query =
                                format!("{} {}", csv_track.artist, csv_track.track_name);
                            match client.search(&query).await {
                                Ok(results) => {
                                    // Find best matching song by comparing title + artist
                                    let title_lower = csv_track.track_name.to_lowercase();
                                    let artist_lower = csv_track.artist.to_lowercase();
                                    if let Some(song) = results.song.iter().find(|s| {
                                        s.title.to_lowercase().contains(&title_lower)
                                            || title_lower.contains(&s.title.to_lowercase())
                                    }).or_else(|| {
                                        // Fallback: match by artist if title didn't match
                                        results.song.iter().find(|s| {
                                            s.artist
                                                .as_deref()
                                                .map(|a| a.to_lowercase().contains(&artist_lower))
                                                .unwrap_or(false)
                                        })
                                    }).or_else(|| {
                                        // Last resort: take the first result
                                        results.song.first()
                                    }) {
                                        song_ids.push(song.id.clone());
                                    } else {
                                        warn!(
                                            "No Navidrome match for '{}' by '{}' in playlist '{}'",
                                            csv_track.track_name,
                                            csv_track.artist,
                                            playlist_name
                                        );
                                    }
                                }
                                Err(e) => {
                                    warn!(
                                        "Navidrome search failed for '{}': {}",
                                        csv_track.track_name, e
                                    );
                                }
                            }
                            // Small delay between searches
                            tokio::time::sleep(Duration::from_millis(200)).await;
                        }

                        if song_ids.is_empty() {
                            warn!(
                                "No songs found in Navidrome for playlist '{}', skipping",
                                playlist_name
                            );
                            continue;
                        }

                        // Check if playlist already exists
                        let existing = existing_playlists
                            .iter()
                            .find(|p| p.name.eq_ignore_ascii_case(playlist_name));

                        if let Some(existing_pl) = existing {
                            // Add songs to existing playlist
                            match client
                                .update_playlist_songs(&existing_pl.id, &song_ids)
                                .await
                            {
                                Ok(()) => {
                                    info!(
                                        "Updated existing playlist '{}' with {} songs",
                                        playlist_name,
                                        song_ids.len()
                                    );
                                }
                                Err(e) => {
                                    warn!(
                                        "Failed to update playlist '{}': {}",
                                        playlist_name, e
                                    );
                                }
                            }
                        } else {
                            // Create new playlist
                            match client
                                .create_playlist(playlist_name, &song_ids)
                                .await
                            {
                                Ok(pl) => {
                                    info!(
                                        "Created playlist '{}' (id: {}) with {} songs",
                                        playlist_name,
                                        pl.id,
                                        song_ids.len()
                                    );
                                }
                                Err(e) => {
                                    warn!(
                                        "Failed to create playlist '{}': {}",
                                        playlist_name, e
                                    );
                                }
                            }
                        }
                    }

                    info!("Playlist creation phase complete");
                }
                Err(e) => {
                    warn!(
                        "Could not get Navidrome client for playlist creation: {}. \
                         Playlists will need to be created manually.",
                        e
                    );
                }
            }
        }
    });

    // Return immediately — downloads happen in background
    Ok(CsvImportProgress {
        total,
        completed: total,
        failed: 0,
        current_track: None,
        statuses,
        playlists: return_playlists,
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
