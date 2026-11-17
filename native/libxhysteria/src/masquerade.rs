use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue, Method, Response, StatusCode, header};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, io, time::SystemTime};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub status: u16,
    pub headers: HashMap<String, String>,
    pub body: String,
}

pub struct Prepared {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

#[derive(Serialize)]
pub struct RequestHead<'request> {
    pub method: &'request str,
    pub uri: String,
    pub headers: HashMap<String, Vec<String>>,
    pub remote: &'request str,
    pub content_length: i64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResponseHead {
    status: u16,
    headers: HashMap<String, Vec<String>>,
}

pub fn encode_headers(headers: &HeaderMap) -> io::Result<HashMap<String, Vec<String>>> {
    headers.iter().try_fold(
        HashMap::<String, Vec<String>>::new(),
        |mut result, (name, value)| {
            result
                .entry(name.as_str().into())
                .or_default()
                .push(value.to_str().map_err(io::Error::other)?.into());
            Ok(result)
        },
    )
}

pub fn decode_headers(headers: HashMap<String, Vec<String>>) -> io::Result<HeaderMap> {
    let mut result = HeaderMap::new();
    for (name, values) in headers {
        let name = HeaderName::from_bytes(name.as_bytes()).map_err(io::Error::other)?;
        if matches!(
            name.as_str(),
            "connection"
                | "proxy-connection"
                | "keep-alive"
                | "transfer-encoding"
                | "upgrade"
                | "te"
        ) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "connection-specific HTTP/3 header",
            ));
        }
        for value in values {
            result.append(
                name.clone(),
                HeaderValue::from_str(&value).map_err(io::Error::other)?,
            );
        }
    }
    Ok(result)
}

pub fn decode_response(data: &[u8]) -> io::Result<Response<()>> {
    let head: ResponseHead = serde_json::from_slice(data).map_err(io::Error::other)?;
    let mut response = Response::new(());
    *response.status_mut() = StatusCode::from_u16(head.status).map_err(io::Error::other)?;
    *response.headers_mut() = decode_headers(head.headers)?;
    Ok(response)
}

pub fn content_length(headers: &HeaderMap) -> io::Result<Option<u64>> {
    let mut length = None;
    for value in headers.get_all(header::CONTENT_LENGTH) {
        let value = value.to_str().map_err(io::Error::other)?;
        if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid HTTP Content-Length",
            ));
        }
        let value = value.parse::<u64>().map_err(io::Error::other)?;
        if value > i64::MAX as u64 || length.is_some_and(|previous| previous != value) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "conflicting HTTP Content-Length",
            ));
        }
        length = Some(value);
    }
    Ok(length)
}

impl Prepared {
    pub fn new(config: Option<Config>) -> io::Result<Self> {
        let config = config.unwrap_or_else(|| Config {
            status: 404,
            headers: HashMap::from([
                ("content-type".into(), "text/plain; charset=utf-8".into()),
                ("x-content-type-options".into(), "nosniff".into()),
            ]),
            body: "404 page not found\n".into(),
        });
        if !(200..=999).contains(&config.status) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "native masquerade requires a final HTTP status",
            ));
        }
        let status = StatusCode::from_u16(config.status).map_err(io::Error::other)?;
        let mut headers = HeaderMap::with_capacity(config.headers.len() + 2);
        for (name, value) in config.headers {
            let name = HeaderName::from_bytes(name.as_bytes()).map_err(io::Error::other)?;
            if matches!(
                name.as_str(),
                "connection"
                    | "proxy-connection"
                    | "keep-alive"
                    | "transfer-encoding"
                    | "upgrade"
                    | "te"
                    | "trailer"
            ) || headers.contains_key(&name)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "unsupported native HTTP/3 masquerade header",
                ));
            }
            let value = HeaderValue::from_str(&value).map_err(io::Error::other)?;
            if name == header::CONTENT_LENGTH
                && value
                    .to_str()
                    .ok()
                    .and_then(|value| value.parse::<u64>().ok())
                    != Some(config.body.len() as u64)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "native masquerade Content-Length does not match the body",
                ));
            }
            headers.insert(name, value);
        }
        let body = if matches!(status, StatusCode::NO_CONTENT | StatusCode::NOT_MODIFIED) {
            if status == StatusCode::NO_CONTENT {
                headers.remove(header::CONTENT_LENGTH);
            }
            Bytes::new()
        } else {
            headers
                .entry(header::CONTENT_LENGTH)
                .or_insert(HeaderValue::from(config.body.len()));
            Bytes::from(config.body)
        };
        Ok(Self {
            status,
            headers,
            body,
        })
    }

    pub fn response(&self, method: &Method) -> (Response<()>, Bytes) {
        let mut response = Response::new(());
        *response.status_mut() = self.status;
        *response.headers_mut() = self.headers.clone();
        response
            .headers_mut()
            .entry(header::DATE)
            .or_insert_with(|| {
                HeaderValue::from_str(&httpdate::fmt_http_date(SystemTime::now())).unwrap()
            });
        let body = if method == Method::HEAD {
            Bytes::new()
        } else {
            self.body.clone()
        };
        (response, body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bridge_preserves_repeated_header_values() {
        let mut headers = HeaderMap::new();
        headers.append("set-cookie", HeaderValue::from_static("first=1"));
        headers.append("set-cookie", HeaderValue::from_static("second=2"));
        let encoded = encode_headers(&headers).unwrap();
        assert_eq!(encoded["set-cookie"], ["first=1", "second=2"]);
        assert_eq!(decode_headers(encoded).unwrap(), headers);
        let response =
            decode_response(br#"{"status":201,"headers":{"set-cookie":["first=1","second=2"]}}"#)
                .unwrap();
        assert_eq!(response.status(), 201);
        assert_eq!(response.headers().get_all("set-cookie").iter().count(), 2);
    }

    #[test]
    fn bridge_rejects_invalid_metadata_and_header_injection() {
        for data in [
            r#"{"status":99,"headers":{}}"#,
            r#"{"status":1000,"headers":{}}"#,
            r#"{"status":200,"headers":{"bad name":["value"]}}"#,
            r#"{"status":200,"headers":{"test":["a\r\nb"]}}"#,
            r#"{"status":200,"headers":{"connection":["keep-alive"]}}"#,
            r#"{"status":200,"headers":{},"unknown":true}"#,
        ] {
            assert!(decode_response(data.as_bytes()).is_err(), "accepted {data}");
        }
    }

    #[test]
    fn bridge_validates_content_length_without_overflow_or_ambiguity() {
        for values in [
            vec!["-1"],
            vec!["+1"],
            vec![" 1"],
            vec!["1, 1"],
            vec![""],
            vec!["9223372036854775808"],
            vec!["1", "2"],
        ] {
            let mut headers = HeaderMap::new();
            for value in values {
                headers.append(
                    header::CONTENT_LENGTH,
                    HeaderValue::from_str(value).unwrap(),
                );
            }
            assert!(content_length(&headers).is_err());
        }
        let mut headers = HeaderMap::new();
        assert_eq!(content_length(&headers).unwrap(), None);
        headers.append(header::CONTENT_LENGTH, HeaderValue::from_static("42"));
        headers.append(header::CONTENT_LENGTH, HeaderValue::from_static("42"));
        assert_eq!(content_length(&headers).unwrap(), Some(42));
    }

    fn configuration(status: u16) -> Config {
        Config {
            status,
            headers: HashMap::from([("content-type".into(), "text/plain".into())]),
            body: "masquerade response".into(),
        }
    }

    #[test]
    fn responses_share_body_and_head_preserves_headers() {
        let prepared = Prepared::new(Some(configuration(201))).unwrap();
        let (response, first) = prepared.response(&Method::GET);
        let (_, second) = prepared.response(&Method::POST);
        assert_eq!(response.status(), 201);
        assert_eq!(
            response.headers()[header::CONTENT_LENGTH],
            first.len().to_string()
        );
        assert_eq!(first.as_ptr(), second.as_ptr());
        assert!(
            httpdate::parse_http_date(response.headers()[header::DATE].to_str().unwrap()).is_ok()
        );
        let (head, body) = prepared.response(&Method::HEAD);
        assert!(body.is_empty());
        assert_eq!(
            head.headers()[header::CONTENT_LENGTH],
            response.headers()[header::CONTENT_LENGTH]
        );
    }

    #[test]
    fn no_content_and_not_modified_do_not_send_body() {
        for status in [204, 304] {
            let mut config = configuration(status);
            config
                .headers
                .insert("content-length".into(), config.body.len().to_string());
            let prepared = Prepared::new(Some(config)).unwrap();
            let (response, body) = prepared.response(&Method::GET);
            assert!(body.is_empty());
            assert_eq!(response.status(), status);
            if status == 204 {
                assert!(!response.headers().contains_key(header::CONTENT_LENGTH));
            }
        }
    }

    #[test]
    fn default_404_matches_go_not_found_response() {
        let prepared = Prepared::new(None).unwrap();
        let (response, body) = prepared.response(&Method::GET);
        assert_eq!(response.status(), 404);
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "text/plain; charset=utf-8"
        );
        assert_eq!(response.headers()["x-content-type-options"], "nosniff");
        assert_eq!(&body[..], b"404 page not found\n");
    }

    #[test]
    fn invalid_headers_and_status_fail_before_listening() {
        for status in [0, 101, 199, 1000] {
            assert!(Prepared::new(Some(configuration(status))).is_err());
        }
        for (name, value) in [
            ("connection", "keep-alive"),
            ("transfer-encoding", "chunked"),
            ("trailer", "test"),
            ("content-length", "1"),
            ("content-length", "-1"),
            ("bad name", "value"),
            ("test", "a\r\nb"),
        ] {
            let mut config = configuration(200);
            config.headers.insert(name.into(), value.into());
            assert!(
                Prepared::new(Some(config)).is_err(),
                "accepted {name}: {value}"
            );
        }
        let mut config = configuration(200);
        config
            .headers
            .insert("Content-Type".into(), "text/html".into());
        assert!(Prepared::new(Some(config)).is_err());
    }
}
