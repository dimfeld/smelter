use rand::Rng;
use smelter_local_jobs::LocalWorkerInfo;
use smelter_worker::WorkerResult;

pub async fn generate_random() {
    println!("generate-random starting...");
    let info = LocalWorkerInfo::from_env().unwrap();

    let sleep_time = rand::thread_rng().gen_range(1..3);
    tokio::time::sleep(std::time::Duration::from_secs(sleep_time)).await;

    let value: u32 = rand::thread_rng().gen_range(1..10);
    info.write_output(WorkerResult::Ok(value.to_string()))
        .await
        .unwrap();

    println!("generate-random done");
}

pub async fn add_values() {
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
    info.write_output(WorkerResult::Ok(result)).await.unwrap();

    println!("add-values done");
}
