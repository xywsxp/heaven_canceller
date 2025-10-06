use arrow::datatypes::FieldRef;
use arrow::record_batch::RecordBatch;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_arrow::schema::SchemaLike;
use serde_arrow::schema::TracingOptions;
use serde_arrow::{from_record_batch, to_record_batch};

use clickhouse::Row;

// pumpfun_trade_event_v2
#[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
pub struct PumpfunTradeEventV2 {
    pub signature: String,
    pub slot: u64,
    pub transaction_index: u32,
    pub instruction_index: u32,
    pub mint: String,
    pub sol_amount: u64,
    pub token_amount: u64,
    pub is_buy: u8,
    pub user: String,
    pub timestamp: u32,
    pub virtual_sol_reserves: u64,
    pub virtual_token_reserves: u64,
    pub real_sol_reserves: u64,
    pub real_token_reserves: u64,
    pub fee_recipient: String,
    pub fee_basis_points: u64,
    pub fee: u64,
    pub creator: String,
    pub creator_fee_basis_points: u64,
    pub creator_fee: u64,
    pub track_volume: u8,
    pub total_unclaimed_tokens: u64,
    pub total_claimed_tokens: u64,
    pub current_sol_volume: u64,
    pub last_update_timestamp: i64,
}

// pumpfun_create_event_v2
#[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
pub struct PumpfunCreateEventV2 {
    pub signature: String,
    pub slot: u64,
    pub transaction_index: u32,
    pub instruction_index: u32,
    pub name: String,
    pub symbol: String,
    pub uri: String,
    pub mint: String,
    pub bonding_curve: String,
    pub user: String,
    pub creator: String,
    pub timestamp: u32,
    pub virtual_token_reserves: u64,
    pub virtual_sol_reserves: u64,
    pub real_token_reserves: u64,
    pub token_total_supply: u64,
}

// pumpfun_migrate_event_v2
#[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
pub struct PumpfunMigrateEventV2 {
    pub signature: String,
    pub slot: u64,
    pub transaction_index: u32,
    pub instruction_index: u32,
    pub user: String,
    pub mint: String,
    pub mint_amount: u64,
    pub sol_amount: u64,
    pub pool_migration_fee: u64,
    pub bonding_curve: String,
    pub timestamp: u32,
    pub pool: String,
}

// pumpfun_amm_buy_event_v2
#[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
pub struct PumpfunAmmBuyEventV2 {
    pub signature: String,
    pub slot: u64,
    pub transaction_index: u32,
    pub instruction_index: u32,
    pub base_mint: String,
    pub quote_mint: String,
    pub timestamp: u32,
    pub base_amount_out: u64,
    pub max_quote_amount_in: u64,
    pub user_base_token_reserves: u64,
    pub user_quote_token_reserves: u64,
    pub pool_base_token_reserves: u64,
    pub pool_quote_token_reserves: u64,
    pub quote_amount_in: u64,
    pub lp_fee_basis_points: u64,
    pub lp_fee: u64,
    pub protocol_fee_basis_points: u64,
    pub protocol_fee: u64,
    pub quote_amount_in_with_lp_fee: u64,
    pub user_quote_amount_in: u64,
    pub pool: String,
    pub user: String,
    pub user_base_token_account: String,
    pub user_quote_token_account: String,
    pub protocol_fee_recipient: String,
    pub protocol_fee_recipient_token_account: String,
    pub coin_creator: String,
    pub coin_creator_fee_basis_points: u64,
    pub coin_creator_fee: u64,
    pub track_volume: u8,
    pub total_unclaimed_tokens: u64,
    pub total_claimed_tokens: u64,
    pub current_sol_volume: u64,
    pub last_update_timestamp: i64,
    pub is_main_pool: u8,
}

// pumpfun_amm_sell_event_v2
#[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
pub struct PumpfunAmmSellEventV2 {
    pub signature: String,
    pub slot: u64,
    pub transaction_index: u32,
    pub instruction_index: u32,
    pub base_mint: String,
    pub quote_mint: String,
    pub timestamp: u32,
    pub base_amount_in: u64,
    pub min_quote_amount_out: u64,
    pub user_base_token_reserves: u64,
    pub user_quote_token_reserves: u64,
    pub pool_base_token_reserves: u64,
    pub pool_quote_token_reserves: u64,
    pub quote_amount_out: u64,
    pub lp_fee_basis_points: u64,
    pub lp_fee: u64,
    pub protocol_fee_basis_points: u64,
    pub protocol_fee: u64,
    pub quote_amount_out_without_lp_fee: u64,
    pub user_quote_amount_out: u64,
    pub pool: String,
    pub user: String,
    pub user_base_token_account: String,
    pub user_quote_token_account: String,
    pub protocol_fee_recipient: String,
    pub protocol_fee_recipient_token_account: String,
    pub coin_creator: String,
    pub coin_creator_fee_basis_points: u64,
    pub coin_creator_fee: u64,
    pub is_main_pool: u8,
}

// pumpfun_amm_create_pool_event_v2
#[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
pub struct PumpfunAmmCreatePoolEventV2 {
    pub signature: String,
    pub slot: u64,
    pub transaction_index: u32,
    pub instruction_index: u32,
    pub timestamp: u32,
    pub index: u32,
    pub creator: String,
    pub base_mint: String,
    pub quote_mint: String,
    pub base_mint_decimals: u32,
    pub quote_mint_decimals: u32,
    pub base_amount_in: u64,
    pub quote_amount_in: u64,
    pub pool_base_amount: u64,
    pub pool_quote_amount: u64,
    pub minimum_liquidity: u64,
    pub initial_liquidity: u64,
    pub lp_token_amount_out: u64,
    pub pool_bump: u32,
    pub pool: String,
    pub lp_mint: String,
    pub user_base_token_account: String,
    pub user_quote_token_account: String,
    pub coin_creator: String,
    pub is_main_pool: u8,
}

// pumpfun_amm_deposit_event_v2
#[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
pub struct PumpfunAmmDepositEventV2 {
    pub signature: String,
    pub slot: u64,
    pub transaction_index: u32,
    pub instruction_index: u32,
    pub base_mint: String,
    pub quote_mint: String,
    pub timestamp: u32,
    pub lp_token_amount_out: u64,
    pub max_base_amount_in: u64,
    pub max_quote_amount_in: u64,
    pub user_base_token_reserves: u64,
    pub user_quote_token_reserves: u64,
    pub pool_base_token_reserves: u64,
    pub pool_quote_token_reserves: u64,
    pub base_amount_in: u64,
    pub quote_amount_in: u64,
    pub lp_mint_supply: u64,
    pub pool: String,
    pub user: String,
    pub user_base_token_account: String,
    pub user_quote_token_account: String,
    pub user_pool_token_account: String,
    pub is_main_pool: u8,
}

// pumpfun_amm_withdraw_event_v2
#[derive(Debug, Row, Serialize, Deserialize, PartialEq)]
pub struct PumpfunAmmWithdrawEventV2 {
    pub signature: String,
    pub slot: u64,
    pub transaction_index: u32,
    pub instruction_index: u32,
    pub base_mint: String,
    pub quote_mint: String,
    pub timestamp: u32,
    pub lp_token_amount_in: u64,
    pub min_base_amount_out: u64,
    pub min_quote_amount_out: u64,
    pub user_base_token_reserves: u64,
    pub user_quote_token_reserves: u64,
    pub pool_base_token_reserves: u64,
    pub pool_quote_token_reserves: u64,
    pub base_amount_out: u64,
    pub quote_amount_out: u64,
    pub lp_mint_supply: u64,
    pub pool: String,
    pub user: String,
    pub user_base_token_account: String,
    pub user_quote_token_account: String,
    pub user_pool_token_account: String,
    pub is_main_pool: u8,
}

pub fn vec_to_arrow_batch<T: Serialize + for<'de> Deserialize<'de>>(data: &Vec<T>) -> RecordBatch {
    let fields = Vec::<FieldRef>::from_type::<T>(TracingOptions::default()).expect("schema tracing failed");
    to_record_batch(&fields, data).expect("Failed to convert Vec<T> to Arrow RecordBatch")
}

/// 将 Arrow RecordBatch 转换为 Vec<T>
pub fn arrow_batch_to_vec<T: DeserializeOwned>(batch: &RecordBatch) -> Vec<T> {
    from_record_batch(batch).expect("Failed to convert Arrow RecordBatch to Vec<T>")
}
