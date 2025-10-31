// Protobuf 生成的代码会在编译时由 build.rs 生成
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/misaka_network_v2.rs"));
}

pub mod client;

pub use client::{AckPolicy, MisakaNetwork, TelepathConfig};
pub use proto::*;
