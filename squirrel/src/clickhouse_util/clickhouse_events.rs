// clickhouse_events.rs
// 自动生成：所有 pumpfun 相关表的结构体和批量插入函数

use clickhouse::Row;
use serde::{Deserialize, Serialize};

// pumpfun_trade_event_v2
#[derive(Debug, Row, Serialize, Deserialize)]
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
#[derive(Debug, Row, Serialize, Deserialize)]
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
#[derive(Debug, Row, Serialize, Deserialize)]
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
#[derive(Debug, Row, Serialize, Deserialize)]
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
#[derive(Debug, Row, Serialize, Deserialize)]
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
#[derive(Debug, Row, Serialize, Deserialize)]
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
#[derive(Debug, Row, Serialize, Deserialize)]
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
#[derive(Debug, Row, Serialize, Deserialize)]
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

// 修改 ClickHouseTable 枚举为带结构体参数的变体

// #[derive(Debug)]
// pub enum ClickHouseTable {
//     PumpfunTradeEventV2(PumpfunTradeEventV2),
//     PumpfunCreateEventV2(PumpfunCreateEventV2),
//     PumpfunMigrateEventV2(PumpfunMigrateEventV2),
//     PumpfunAmmBuyEventV2(PumpfunAmmBuyEventV2),
//     PumpfunAmmSellEventV2(PumpfunAmmSellEventV2),
//     PumpfunAmmCreatePoolEventV2(PumpfunAmmCreatePoolEventV2),
//     PumpfunAmmDepositEventV2(PumpfunAmmDepositEventV2),
//     PumpfunAmmWithdrawEventV2(PumpfunAmmWithdrawEventV2),
// }


