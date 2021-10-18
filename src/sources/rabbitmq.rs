use crate::config::{DataType, SourceConfig, SourceContext, SourceDescription};
use lapin::{Connection, ConnectionProperties};
use serde::{Deserialize, Serialize};
use std::default::Default;

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RabbitMQConfig {
    host: String,
    port: u16,
    vhost: String,
    auth: RabbitMQAuthConfig,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RabbitMQAuthConfig {
    login: String,
    password: String,
}

impl RabbitMQConfig {
    fn connection_str(&self) -> String {
        format!(
            "{}:{}@{}:{}{}",
            self.auth.login, self.auth.password, self.host, self.port, self.vhost
        )
    }
}

inventory::submit! {
    SourceDescription::new::<RabbitMQConfig>("rabbitmq")
}

impl_generate_config_from_default!(RabbitMQConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "rabbitmq")]
impl SourceConfig for RabbitMQConfig {
    async fn build(&self, _cx: SourceContext) -> crate::Result<super::Source> {
        let consumer = create_consumer(self).await?;
        info!("Hello RabbitMQ :)");
        info!("consumer: {}", consumer);
        Ok(Box::pin(rabbitmq_source()))
    }

    fn output_type(&self) -> DataType {
        DataType::Log
    }

    fn source_type(&self) -> &'static str {
        "rabbitmq"
    }
}

async fn rabbitmq_source() -> Result<(), ()> {
    Ok(())
}

async fn create_consumer(config: &RabbitMQConfig) -> crate::Result<String> {
    println!("{}", config.connection_str());
    let res = Connection::connect(
        &config.connection_str(),
        ConnectionProperties::default().with_default_executor(8),
    )
    .await;
    Ok("isso".to_string())
}
