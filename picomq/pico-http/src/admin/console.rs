use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};

use crate::common::error_response;

#[derive(rust_embed::RustEmbed)]
#[folder = "_console/"]
#[allow_missing = true]
struct Console;

const HINT: &str = "<!doctype html><html><body style=\"font-family: sans-serif\">\
<h3>PicoMQ admin</h3>\
<p>This binary was built without the console. Build it with\
<code> cd console && npm install && npm run build</code> and recompile,\
or use the Docker image. The <code>/admin</code> API is available.</p>\
</body></html>";

const INDEX: &str = "index.html";

pub(super) async fn serve(uri: Uri, headers: HeaderMap) -> Response {
    let path = uri.path().trim_start_matches('/');

    if let Some(response) = asset(path, &headers) {
        return response;
    }
    if matches!(path.split('/').next(), Some("admin" | "assets")) {
        return error_response(StatusCode::NOT_FOUND, format!("no such path /{path}"));
    }

    asset(INDEX, &headers).unwrap_or_else(|| {
        ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], HINT).into_response()
    })
}

fn asset(path: &str, headers: &HeaderMap) -> Option<Response> {
    let file = Console::get(path)?;
    let compressed = accepts_gzip(headers)
        .then(|| Console::get(&format!("{path}.gz")))
        .flatten();

    let mut response = match compressed {
        Some(compressed) => (
            [(header::CONTENT_ENCODING, "gzip")],
            Body::from(compressed.data),
        )
            .into_response(),
        None => Body::from(file.data).into_response(),
    };

    let headers = response.headers_mut();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(mime(path)));
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static(cache(path)));
    headers.insert(header::VARY, HeaderValue::from_static("accept-encoding"));
    Some(response)
}

fn mime(path: &str) -> &'static str {
    match path.rsplit_once('.').map(|(_, extension)| extension) {
        Some("html") => "text/html; charset=utf-8",
        Some("js") => "text/javascript",
        Some("css") => "text/css",
        Some("svg") => "image/svg+xml",
        Some("woff2") => "font/woff2",
        Some("map" | "json") => "application/json",
        _ => "application/octet-stream",
    }
}

fn cache(path: &str) -> &'static str {
    if path.starts_with("assets/") {
        "public, max-age=31536000, immutable"
    } else {
        "no-cache"
    }
}

fn accepts_gzip(headers: &HeaderMap) -> bool {
    headers
        .get_all(header::ACCEPT_ENCODING)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|coding| {
            let mut parts = coding.split(';').map(str::trim);
            let gzip = parts
                .next()
                .is_some_and(|name| name.eq_ignore_ascii_case("gzip"));
            let refused = parts.any(|parameter| {
                parameter
                    .strip_prefix("q=")
                    .and_then(|weight| weight.parse::<f32>().ok())
                    .is_some_and(|weight| weight == 0.0)
            });
            gzip && !refused
        })
}
