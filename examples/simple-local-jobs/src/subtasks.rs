use rand::Rng;
use smelter_local_jobs::LocalWorkerInfo;
use smelter_worker::{WorkerOutput, WorkerResult};

pub async fn generate_random() {
    let stats = smelter_worker::stats::track_system_stats();
    println!("generate-random starting...");
    let info = LocalWorkerInfo::from_env().unwrap();

    let sleep_time = rand::thread_rng().gen_range(1..3);
    tokio::time::sleep(std::time::Duration::from_secs(sleep_time)).await;

    let value: u32 = rand::thread_rng().gen_range(1..10);

    let output = WorkerOutput {
        result: WorkerResult::Ok(value.to_string()),
        stats: stats.finish().await,
    };

    info.write_output(output).await.unwrap();

    println!("generate-random done");
}

pub async fn add_values() {
    let stats = smelter_worker::stats::track_system_stats();
    println!("add-values starting...");

    let info = LocalWorkerInfo::from_env().unwrap();
    let input = info.read_input::<Vec<usize>>().await.unwrap();

    let sleep_time = rand::thread_rng().gen_range(1..6);
    tokio::time::sleep(std::time::Duration::from_secs(sleep_time)).await;

    let summed = input.input.iter().sum::<usize>();
    let plusses = input
        .input
        .iter()
        .map(|i| format!("{}", i))
        .collect::<Vec<_>>()
        .join(" + ");
    let result = format!("{plusses} = {summed}");

    let output = WorkerOutput {
        result: WorkerResult::Ok(result),
        stats: stats.finish().await,
    };

    info.write_output(output).await.unwrap();

    println!("add-values done");
}
