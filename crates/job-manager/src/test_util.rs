use once_cell::sync::Lazy;

pub static TRACING: Lazy<()> = Lazy::new(|| {
    if std::env::var("TEST_LOG").is_ok() {
        configure_tracing();
    }
});

fn configure_tracing() {
    use tracing_subscriber::layer::SubscriberExt;
    let tree = tracing_tree::HierarchicalLayer::new(2)
        .with_targets(true)
        .with_bracketed_fields(true);

    let subscriber = tracing_subscriber::Registry::default().with(tree);

    tracing::subscriber::set_global_default(subscriber).unwrap();
}

pub fn setup_test_tracing() {
    Lazy::force(&TRACING);
}
