use api::{
    CsvBatchImportRequest, CsvImportProgress, CsvParseResult, CsvTrackStatus,
};
use dioxus::prelude::*;

#[derive(Debug, Clone, PartialEq)]
enum ImportState {
    /// Waiting for user to upload a CSV
    Upload,
    /// CSV parsed, showing preview table
    Preview(CsvParseResult),
    /// Import in progress
    Importing,
    /// Import complete
    Done(CsvImportProgress),
    /// Error state
    Error(String),
}

#[component]
pub fn ImportPage() -> Element {
    let mut state = use_signal(|| ImportState::Upload);
    let csv_content = use_signal(|| String::new());
    let folder_id = use_signal(|| String::new());
    let folder_path = use_signal(|| String::new());

    // Load user folders for the target folder picker
    let folders_resource = use_resource(|| async { api::get_user_folders().await });

    rsx! {
        div { class: "fixed top-1/4 -left-10 w-64 h-64 bg-purple-500/10 rounded-full blur-[100px] pointer-events-none" }
        div { class: "fixed bottom-1/4 -right-10 w-64 h-64 bg-beet-leaf/10 rounded-full blur-[100px] pointer-events-none" }

        div { class: "space-y-6 text-white w-full max-w-5xl z-10 mx-auto",
            div { class: "text-center mb-6",
                h1 { class: "text-4xl font-bold text-beet-accent mb-2 font-display",
                    "Import Library"
                }
                p { class: "text-gray-400 font-mono text-sm",
                    "Import your music library from a TuneMyMusic CSV export"
                }
            }

            match &*state.read() {
                ImportState::Upload => rsx! {
                    UploadSection {
                        csv_content,
                        state,
                        folder_id,
                        folder_path,
                        folders_resource,
                    }
                },
                ImportState::Preview(parse_result) => rsx! {
                    PreviewSection {
                        parse_result: parse_result.clone(),
                        state,
                        folder_id: folder_id(),
                        folder_path: folder_path(),
                    }
                },
                ImportState::Importing => rsx! {
                    div { class: "flex flex-col items-center gap-4 py-12",
                        div { class: "animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-beet-accent" }
                        p { class: "text-gray-300 font-mono",
                            "Importing tracks... Each track is being searched and downloaded."
                        }
                        p { class: "text-gray-500 font-mono text-sm",
                            "Progress will appear in the Downloads panel →"
                        }
                    }
                },
                ImportState::Done(progress) => rsx! {
                    DoneSection {
                        progress: progress.clone(),
                        state,
                    }
                },
                ImportState::Error(msg) => rsx! {
                    div { class: "bg-red-500/10 border border-red-500/30 rounded-lg p-6 text-center",
                        p { class: "text-red-400 font-mono", "{msg}" }
                        button {
                            class: "mt-4 px-4 py-2 bg-beet-accent/20 hover:bg-beet-accent/30 text-beet-accent rounded transition-colors cursor-pointer font-mono text-sm",
                            onclick: move |_| state.set(ImportState::Upload),
                            "← Try Again"
                        }
                    }
                },
            }
        }
    }
}

#[component]
fn UploadSection(
    csv_content: Signal<String>,
    state: Signal<ImportState>,
    folder_id: Signal<String>,
    folder_path: Signal<String>,
    folders_resource: Resource<Result<Vec<api::models::folder::Folder>, ServerFnError>>,
) -> Element {
    let mut parsing = use_signal(|| false);

    let handle_parse = move |_| {
        let content = csv_content();
        if content.is_empty() {
            state.set(ImportState::Error("No CSV content provided".into()));
            return;
        }
        if folder_path().is_empty() {
            state.set(ImportState::Error(
                "Please select a target folder first".into(),
            ));
            return;
        }
        parsing.set(true);
        spawn(async move {
            match api::parse_csv(content).await {
                Ok(result) => {
                    if result.tracks.is_empty() {
                        state.set(ImportState::Error(
                            "No valid tracks found in CSV. Check the format: Track name, Artist name, Album name".into()
                        ));
                    } else {
                        state.set(ImportState::Preview(result));
                    }
                }
                Err(e) => {
                    state.set(ImportState::Error(format!("Failed to parse CSV: {}", e)));
                }
            }
            parsing.set(false);
        });
    };

    rsx! {
        // Folder selector
        div { class: "bg-beet-panel border border-white/10 rounded-lg p-6 space-y-4",
            h2 { class: "text-lg font-bold text-white font-display", "1. Select Target Folder" }
            p { class: "text-gray-400 text-sm font-mono",
                "Choose which library folder to import tracks into."
            }
            match &*folders_resource.read() {
                Some(Ok(folders)) if !folders.is_empty() => rsx! {
                    div { class: "flex flex-wrap gap-2",
                        for f in folders.iter() {
                            button {
                                key: "{f.id}",
                                class: if folder_id() == f.id {
                                    "px-4 py-2 rounded-lg font-mono text-sm bg-beet-accent text-black font-bold cursor-pointer transition-colors"
                                } else {
                                    "px-4 py-2 rounded-lg font-mono text-sm bg-white/5 hover:bg-white/10 text-gray-300 cursor-pointer transition-colors border border-white/10"
                                },
                                onclick: {
                                    let fid = f.id.clone();
                                    let fpath = f.path.clone();
                                    move |_| {
                                        folder_id.set(fid.clone());
                                        folder_path.set(fpath.clone());
                                    }
                                },
                                "{f.name}"
                            }
                        }
                    }
                },
                Some(Ok(_)) => rsx! {
                    p { class: "text-yellow-400 text-sm font-mono",
                        "No folders configured. Go to Settings → Library to add one."
                    }
                },
                Some(Err(_)) => rsx! {
                    p { class: "text-red-400 text-sm font-mono", "Failed to load folders." }
                },
                None => rsx! {
                    p { class: "text-gray-500 text-sm font-mono animate-pulse", "Loading folders..." }
                },
            }
        }

        // CSV input — file picker via JS eval + textarea fallback
        div { class: "bg-beet-panel border border-white/10 rounded-lg p-6 space-y-4",
            h2 { class: "text-lg font-bold text-white font-display", "2. Load CSV File" }
            p { class: "text-gray-400 text-sm font-mono",
                "Export your library from TuneMyMusic as CSV (comma-separated)."
            }

            // File picker button using JS eval
            div { class: "flex gap-3",
                button {
                    class: "px-4 py-2 bg-white/5 hover:bg-white/10 text-gray-300 rounded-lg transition-colors cursor-pointer font-mono text-sm border border-white/10 flex items-center gap-2",
                    onclick: move |_| {
                        spawn(async move {
                            let result = document::eval(r#"
                                return new Promise((resolve) => {
                                    const input = document.createElement('input');
                                    input.type = 'file';
                                    input.accept = '.csv,text/csv';
                                    input.onchange = () => {
                                        const file = input.files[0];
                                        if (file) {
                                            const reader = new FileReader();
                                            reader.onload = () => resolve(reader.result);
                                            reader.readAsText(file);
                                        } else {
                                            resolve('');
                                        }
                                    };
                                    input.click();
                                });
                            "#);
                            if let Ok(content) = result.await {
                                if let Some(text) = content.as_str() {
                                    if !text.is_empty() {
                                        csv_content.set(text.to_string());
                                    }
                                }
                            }
                        });
                    },
                    svg {
                        class: "w-4 h-4",
                        fill: "none",
                        stroke: "currentColor",
                        view_box: "0 0 24 24",
                        path {
                            stroke_linecap: "round",
                            stroke_linejoin: "round",
                            stroke_width: "1.5",
                            d: "M3 16.5v2.25A2.25 2.25 0 005.25 21h13.5A2.25 2.25 0 0021 18.75V16.5m-13.5-9L12 3m0 0l4.5 4.5M12 3v13.5",
                        }
                    }
                    "Choose CSV File"
                }
                if !csv_content().is_empty() {
                    span { class: "text-beet-accent font-mono text-sm self-center",
                        "✓ {csv_content().lines().count()} lines loaded"
                    }
                }
            }

            // Textarea fallback for paste
            div { class: "space-y-2",
                p { class: "text-gray-500 text-xs font-mono", "Or paste CSV content directly:" }
                textarea {
                    class: "w-full h-32 bg-black/30 border border-white/10 rounded-lg p-3 text-gray-300 font-mono text-xs resize-y focus:outline-none focus:border-beet-accent/50 placeholder-gray-600",
                    placeholder: "Track name,Artist name,Album name,Playlist name\nBohemian Rhapsody,Queen,A Night at the Opera,My Playlist\n...",
                    value: "{csv_content}",
                    oninput: move |e| csv_content.set(e.value()),
                }
            }

            // Parse button
            if !csv_content().is_empty() && !folder_path().is_empty() {
                button {
                    class: "w-full py-3 bg-beet-accent hover:bg-beet-accent/80 text-black font-bold rounded-lg transition-colors cursor-pointer font-mono disabled:opacity-50 disabled:cursor-not-allowed",
                    disabled: parsing(),
                    onclick: handle_parse,
                    if parsing() {
                        "Parsing..."
                    } else {
                        "Parse & Preview"
                    }
                }
            }
        }
    }
}

#[component]
fn PreviewSection(
    parse_result: CsvParseResult,
    state: Signal<ImportState>,
    folder_id: String,
    folder_path: String,
) -> Element {
    let track_count = parse_result.tracks.len();
    let tracks_for_import = use_signal(|| parse_result.tracks.clone());

    let start_import = move |_| {
        let tracks = tracks_for_import();
        let fid = folder_id.clone();
        let fpath = folder_path.clone();
        state.set(ImportState::Importing);
        spawn(async move {
            match api::import_csv_batch(CsvBatchImportRequest {
                tracks,
                folder_id: fid,
                folder_path: fpath,
            })
            .await
            {
                Ok(progress) => {
                    state.set(ImportState::Done(progress));
                }
                Err(e) => {
                    state.set(ImportState::Error(format!("Import failed: {}", e)));
                }
            }
        });
    };

    rsx! {
        div { class: "bg-beet-panel border border-white/10 rounded-lg p-6 space-y-4",
            div { class: "flex items-center justify-between",
                h2 { class: "text-lg font-bold text-white font-display",
                    "Preview ({track_count} tracks)"
                }
                div { class: "flex gap-2",
                    button {
                        class: "px-4 py-2 bg-white/5 hover:bg-white/10 text-gray-300 rounded-lg transition-colors cursor-pointer font-mono text-sm border border-white/10",
                        onclick: move |_| state.set(ImportState::Upload),
                        "← Back"
                    }
                    button {
                        class: "px-6 py-2 bg-beet-accent hover:bg-beet-accent/80 text-black font-bold rounded-lg transition-colors cursor-pointer font-mono text-sm",
                        onclick: start_import,
                        "Import All {track_count} Tracks"
                    }
                }
            }

            if parse_result.skipped_rows > 0 {
                p { class: "text-yellow-400/80 text-xs font-mono",
                    "⚠ {parse_result.skipped_rows} rows skipped (missing track name or artist)"
                }
            }

            if !parse_result.playlists.is_empty() {
                div { class: "flex items-center gap-2 flex-wrap",
                    span { class: "text-gray-400 text-xs font-mono", "Playlists:" }
                    for pl in parse_result.playlists.iter() {
                        span { class: "px-2 py-0.5 bg-purple-500/20 text-purple-300 text-xs font-mono rounded",
                            "{pl}"
                        }
                    }
                    p { class: "text-gray-500 text-xs font-mono w-full mt-1",
                        "These playlists will be auto-created in Navidrome after downloads complete"
                    }
                }
            }

            // Track table
            div { class: "overflow-x-auto max-h-[60vh] overflow-y-auto",
                table { class: "w-full text-sm font-mono",
                    thead { class: "sticky top-0 bg-beet-panel",
                        tr { class: "text-gray-500 uppercase text-xs tracking-wider border-b border-white/10",
                            th { class: "text-left py-2 px-3", "#" }
                            th { class: "text-left py-2 px-3", "Track" }
                            th { class: "text-left py-2 px-3", "Artist" }
                            th { class: "text-left py-2 px-3", "Album" }
                            if !parse_result.playlists.is_empty() {
                                th { class: "text-left py-2 px-3", "Playlist" }
                            }
                        }
                    }
                    tbody {
                        for (i, track) in parse_result.tracks.iter().enumerate() {
                            tr { class: "border-b border-white/5 hover:bg-white/5 transition-colors",
                                key: "{track.row_index}",
                                td { class: "py-2 px-3 text-gray-600", "{i + 1}" }
                                td { class: "py-2 px-3 text-white", "{track.track_name}" }
                                td { class: "py-2 px-3 text-gray-300", "{track.artist}" }
                                td { class: "py-2 px-3 text-gray-500", "{track.album}" }
                                if !parse_result.playlists.is_empty() {
                                    td { class: "py-2 px-3 text-purple-300/80", "{track.playlist_name}" }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn DoneSection(progress: CsvImportProgress, state: Signal<ImportState>) -> Element {
    rsx! {
        div { class: "bg-beet-panel border border-white/10 rounded-lg p-6 space-y-4",
            div { class: "text-center space-y-2",
                h2 { class: "text-2xl font-bold text-beet-accent font-display", "Import Started" }
                p { class: "text-gray-300 font-mono text-sm",
                    "{progress.total} tracks queued for search & download"
                }
                if !progress.playlists.is_empty() {
                    div { class: "mt-2 flex items-center justify-center gap-2 flex-wrap",
                        span { class: "text-gray-400 text-sm font-mono", "Playlists to create:" }
                        for pl in progress.playlists.iter() {
                            span { class: "px-2 py-0.5 bg-purple-500/20 text-purple-300 text-xs font-mono rounded",
                                "{pl}"
                            }
                        }
                    }
                    p { class: "text-gray-500 font-mono text-xs mt-1",
                        "Playlists will be created in Navidrome automatically after downloads finish"
                    }
                }
                p { class: "text-gray-500 font-mono text-xs",
                    "Track progress in the Downloads panel →"
                }
            }

            // Results breakdown
            if !progress.statuses.is_empty() {
                div { class: "overflow-x-auto max-h-[50vh] overflow-y-auto mt-4",
                    table { class: "w-full text-sm font-mono",
                        thead { class: "sticky top-0 bg-beet-panel",
                            tr { class: "text-gray-500 uppercase text-xs tracking-wider border-b border-white/10",
                                th { class: "text-left py-2 px-3", "Track" }
                                th { class: "text-left py-2 px-3", "Artist" }
                                th { class: "text-left py-2 px-3", "Status" }
                            }
                        }
                        tbody {
                            for (track, status) in progress.statuses.iter() {
                                tr { class: "border-b border-white/5",
                                    key: "{track.row_index}",
                                    td { class: "py-2 px-3 text-white", "{track.track_name}" }
                                    td { class: "py-2 px-3 text-gray-300", "{track.artist}" }
                                    td { class: "py-2 px-3",
                                        match status {
                                            CsvTrackStatus::Accepted { .. } => rsx! {
                                                span { class: "text-green-400", "✓ Queued" }
                                            },
                                            CsvTrackStatus::Failed { error } => rsx! {
                                                span { class: "text-red-400", title: "{error}", "✗ Failed" }
                                            },
                                            CsvTrackStatus::Searching => rsx! {
                                                span { class: "text-yellow-400", "⏳ Searching" }
                                            },
                                            CsvTrackStatus::Pending => rsx! {
                                                span { class: "text-gray-500", "⏳ Pending" }
                                            },
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            div { class: "flex justify-center pt-4",
                button {
                    class: "px-6 py-2 bg-beet-accent/20 hover:bg-beet-accent/30 text-beet-accent rounded-lg transition-colors cursor-pointer font-mono text-sm",
                    onclick: move |_| state.set(ImportState::Upload),
                    "Import Another CSV"
                }
            }
        }
    }
}
