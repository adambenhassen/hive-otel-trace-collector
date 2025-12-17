use crate::config::Config;
use super::clickhouse::ClickHouseExporter;
use super::loki::LokiExporter;
use super::kafka::KafkaExporter;

pub struct Exporters {
    pub clickhouse: Option<ClickHouseExporter>,
    pub loki: Option<LokiExporter>,
    pub kafka: Option<KafkaExporter>,
}

impl Exporters {
    pub async fn init(config: &Config) -> (Self, Vec<tokio::task::JoinHandle<()>>) {
        let mut handles = Vec::new();

        let clickhouse = ClickHouseExporter::try_init(config).await.map(|(exporter, h)| {
            handles.extend(h);
            exporter
        });

        let loki = LokiExporter::try_init(config).map(|(exporter, h)| {
            handles.extend(h);
            exporter
        });

        let kafka = KafkaExporter::try_init(config).map(|(exporter, h)| {
            handles.extend(h);
            exporter
        });

        (Self { clickhouse, loki, kafka }, handles)
    }

    pub fn shutdown(&self) {
        if let Some(ref ch) = self.clickhouse {
            ch.batcher.shutdown();
        }
        if let Some(ref lk) = self.loki {
            lk.batcher.shutdown();
        }
        if let Some(ref kf) = self.kafka {
            kf.batcher.shutdown();
        }
    }
}
