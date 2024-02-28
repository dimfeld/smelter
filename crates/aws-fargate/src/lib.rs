#[cfg(feature = "spawner")]
pub mod spawner;
#[cfg(feature = "worker")]
pub mod worker;

const INPUT_LOCATION_VAR: &str = "INPUT_LOCATION";
const OUTPUT_LOCATION_VAR: &str = "OUTPUT_LOCATION";
