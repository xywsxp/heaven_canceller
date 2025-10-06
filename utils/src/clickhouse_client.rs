use clickhouse::Client;
use std::sync::OnceLock;

pub struct ClickHouseClient {
    client: Client,
}

impl ClickHouseClient {
    fn new() -> Self {
        let url = std::env::var("CLICKHOUSE_URL").expect("CLICKHOUSE_URL environment variable is required");
        let user = std::env::var("CLICKHOUSE_USER").expect("CLICKHOUSE_USER environment variable is required");
        let database = std::env::var("CLICKHOUSE_DATABASE").expect("CLICKHOUSE_DATABASE environment variable is required");
        let password = std::env::var("CLICKHOUSE_PASSWORD").expect("CLICKHOUSE_PASSWORD environment variable is required");
        
        let client = Client::default()
            .with_url(&url)
            .with_user(&user)
            .with_database(&database)
            .with_password(&password)
            .with_option("async_insert", "1")
            .with_option("wait_for_async_insert", "0")
            .with_option("enable_http_compression", "1");
        
        Self { client }
    }

    pub fn instance() -> &'static ClickHouseClient {
        static INSTANCE: OnceLock<ClickHouseClient> = OnceLock::new();
        INSTANCE.get_or_init(|| ClickHouseClient::new())
    }

    pub fn client(&self) -> &Client {
        &self.client
    }
}
