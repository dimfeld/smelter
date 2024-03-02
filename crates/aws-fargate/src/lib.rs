use thiserror::Error;

#[cfg(feature = "spawner")]
pub mod spawner;
#[cfg(feature = "worker")]
pub mod worker;

const INPUT_LOCATION_VAR: &str = "SMELTER_INPUT_LOCATION";
const OUTPUT_LOCATION_VAR: &str = "SMELTER_OUTPUT_LOCATION";
const SUBTASK_ID_VAR: &str = "SMELTER_SUBTASK_ID";
const OTEL_CONTEXT_VAR: &str = "SMELTER_OTEL_CONTEXT";

#[derive(Debug, Error)]
#[error("{0}")]
struct AwsError(String);

impl AwsError {
    fn from<E: std::fmt::Debug>(value: E) -> Self {
        Self(format!("{value:?}"))
    }
}

pub fn parse_s3_url(path: &str) -> Option<(String, String)> {
    let u = url::Url::parse(path).ok()?;
    if u.scheme() != "s3" {
        return None;
    }

    let bucket = u.host_str()?.to_string();
    let path = u.path();

    if path.len() <= 1 {
        return None;
    }

    // Remove leading slash
    let path = path[1..].to_string();

    Some((bucket, path))
}

#[cfg(test)]
mod test {

    mod parse_s3_url {
        use crate::parse_s3_url;

        #[test]
        fn success() {
            let input = "s3://bucket/path/and/file.json";
            let (bucket, path) = parse_s3_url(input).unwrap();
            assert_eq!(bucket, "bucket");
            assert_eq!(path, "path/and/file.json");
        }

        #[test]
        fn invalid_scheme() {
            let input = "http://bucket/path/and/file.json";
            assert!(parse_s3_url(input).is_none());
        }

        #[test]
        fn no_bucket() {
            let input = "s3:///path/and/file.json";
            assert!(parse_s3_url(input).is_none());
        }

        #[test]
        fn no_path() {
            let input = "s3://bucket";
            assert!(parse_s3_url(input).is_none());
        }
    }
}
