use tonic::transport::Channel;

// 导入生成的 protobuf 代码
pub mod misaka_network {
    tonic::include_proto!("misaka_network");
}

use misaka_network::{
    misaka_network_service_client::MisakaNetworkServiceClient, EmitMisakaSignalRequest,
    EmitMisakaSignalResponse, MisakaSignal,
};

pub struct GrpcClient {
    client: MisakaNetworkServiceClient<Channel>,
}

impl GrpcClient {
    pub async fn new(server_url: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let client = MisakaNetworkServiceClient::connect(server_url.to_string()).await?;
        Ok(Self { client })
    }

    pub async fn emit_signal(
        &self,
        telepath_name: &str,
        signal: MisakaSignal,
    ) -> Result<EmitMisakaSignalResponse, Box<dyn std::error::Error>> {
        let mut client = self.client.clone();
        let request = tonic::Request::new(EmitMisakaSignalRequest {
            telepath_name: telepath_name.to_string(),
            signal: Some(signal),
        });

        let response = client.emit_misaka_signal(request).await?;
        Ok(response.into_inner())
    }
}
