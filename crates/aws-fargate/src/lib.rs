#[cfg(feature = "spawner")]
pub mod spawner;
#[cfg(feature = "worker")]
pub mod worker;

const INPUT_LOCATION_VAR: &str = "INPUT_LOCATION";
const OUTPUT_LOCATION_VAR: &str = "OUTPUT_LOCATION";

pub fn parse_s3_url(path: &str) -> Option<(String, String)> {
    let u = url::Url::parse(path).ok()?;
    if u.scheme() != "s3" {
        return None;
    }

    let bucket = u.host_str()?.to_string();
    let path = u.path().to_string();

    Some((bucket, path))
}
