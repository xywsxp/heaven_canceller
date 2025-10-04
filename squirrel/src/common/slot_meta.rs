use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct SlotMeta {
    pub slot: u64,
    pub offset: Option<u64>,
    pub size: u64,
}