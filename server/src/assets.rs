use axum::{body::Body, http::{header, Response, StatusCode}};
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "../frontend/out/"]
struct FrontendAssets;

pub async fn handler(uri: axum::http::Uri) -> Response<Body> {
    let path = uri.path().trim_start_matches('/');
    let path = if path.is_empty() { "index.html" } else { path };
    serve(path)
}

fn serve(path: &str) -> Response<Body> {
    match FrontendAssets::get(path) {
        Some(content) => {
            // Hashed _next/static/ assets are immutable; everything else should revalidate.
            let cache = if path.starts_with("_next/static/") {
                "public, max-age=31536000, immutable"
            } else {
                "no-cache"
            };
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, mime_for(path))
                .header(header::CACHE_CONTROL, cache)
                .body(Body::from(content.data.into_owned()))
                .unwrap()
        }
        None => {
            // SPA fallback: unknown paths serve index.html so client-side routing works.
            match FrontendAssets::get("index.html") {
                Some(content) => Response::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
                    .header(header::CACHE_CONTROL, "no-cache")
                    .body(Body::from(content.data.into_owned()))
                    .unwrap(),
                None => Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::from("frontend not built — run: cd frontend && npm run build"))
                    .unwrap(),
            }
        }
    }
}

fn mime_for(path: &str) -> &'static str {
    if path.ends_with(".html") { "text/html; charset=utf-8" }
    else if path.ends_with(".js") || path.ends_with(".mjs") { "application/javascript" }
    else if path.ends_with(".css") { "text/css" }
    else if path.ends_with(".json") { "application/json" }
    else if path.ends_with(".svg") { "image/svg+xml" }
    else if path.ends_with(".png") { "image/png" }
    else if path.ends_with(".ico") { "image/x-icon" }
    else if path.ends_with(".woff2") { "font/woff2" }
    else if path.ends_with(".woff") { "font/woff" }
    else { "application/octet-stream" }
}
