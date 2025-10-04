use crate::clickhouse_util::clickhouse_events::{
    PumpfunAmmBuyEventV2, PumpfunAmmCreatePoolEventV2, PumpfunAmmDepositEventV2,
    PumpfunAmmSellEventV2, PumpfunAmmWithdrawEventV2, PumpfunCreateEventV2, PumpfunMigrateEventV2,
    PumpfunTradeEventV2,
};
use common::cached_bs58::global_bs58;
use proto_lib::transaction::solana::Transaction;
pub struct TransactionConverter;

impl TransactionConverter {
    pub fn convert(
        tx: &Transaction,
        pumpfun_trade_event_rows: &mut Vec<PumpfunTradeEventV2>,
        pumpfun_create_event_rows: &mut Vec<PumpfunCreateEventV2>,
        pumpfun_migrate_event_rows: &mut Vec<PumpfunMigrateEventV2>,
        pumpfun_amm_buy_event_rows: &mut Vec<PumpfunAmmBuyEventV2>,
        pumpfun_amm_sell_event_rows: &mut Vec<PumpfunAmmSellEventV2>,
        pumpfun_amm_create_pool_event_rows: &mut Vec<PumpfunAmmCreatePoolEventV2>,
        pumpfun_amm_deposit_event_rows: &mut Vec<PumpfunAmmDepositEventV2>,
        pumpfun_amm_withdraw_event_rows: &mut Vec<PumpfunAmmWithdrawEventV2>,
    ) {
        let mut stack: Vec<&proto_lib::transaction::solana::Instruction> = Vec::new();
        let mut index = 0;
        for instr in &tx.instructions {
            if is_event(instr) {
                // 当前是event，出栈拿到前一个instruction
                if let Some(prev_instr) = stack.pop() {
                    // 这里根据event类型和prevInstr组装ClickHouseTable
                    match instr.r#type.as_str() {
                        "PumpFunTradeEvent" => {
                            if let (Some(parsed_event), Some(_parsed_instr)) =
                                (&instr.parsed, &prev_instr.parsed)
                            {
                                if let proto_lib::transaction::solana::instruction::Parsed::PumpfunTradeEvent(trade_event) = parsed_event {
                                    let event_v2 = PumpfunTradeEventV2 {
                                        signature: global_bs58().encode_64(&tx.signature),
                                        slot: tx.slot,
                                        transaction_index: tx.index as u32,
                                        instruction_index: index as u32,
                                        mint: global_bs58().encode_32(&trade_event.mint),
                                        sol_amount: trade_event.sol_amount,
                                        token_amount: trade_event.token_amount,
                                        is_buy: trade_event.is_buy as u8,
                                        user: global_bs58().encode_32(&trade_event.user),
                                        timestamp: trade_event.timestamp as u32,
                                        virtual_sol_reserves: trade_event.virtual_sol_reserves,
                                        virtual_token_reserves: trade_event.virtual_token_reserves,
                                        real_sol_reserves: trade_event.real_sol_reserves,
                                        real_token_reserves: trade_event.real_token_reserves,
                                        fee_recipient: global_bs58().encode_32(&trade_event.fee_recipient),
                                        fee_basis_points: trade_event.fee_basis_points,
                                        fee: trade_event.fee,
                                        creator: global_bs58().encode_32(&trade_event.creator),
                                        creator_fee_basis_points: trade_event.creator_fee_basis_points,
                                        creator_fee: trade_event.creator_fee,
                                        track_volume: trade_event.track_volume as u8,
                                        total_unclaimed_tokens: trade_event.total_unclaimed_tokens,
                                        total_claimed_tokens: trade_event.total_claimed_tokens,
                                        current_sol_volume: trade_event.current_sol_volume,
                                        last_update_timestamp: trade_event.last_update_timestamp,
                                    };
                                    pumpfun_trade_event_rows.push(event_v2);
                                }
                            }
                        }
                        "PumpFunCreateEvent" => {
                            if let (Some(parsed_event), Some(_parsed_instr)) =
                                (&instr.parsed, &prev_instr.parsed)
                            {
                                if let proto_lib::transaction::solana::instruction::Parsed::PumpfunCreateEvent(create_event) = parsed_event {
                                    let event_v2 = PumpfunCreateEventV2 {
                                        signature: global_bs58().encode_64(&tx.signature),
                                        slot: tx.slot,
                                        transaction_index: tx.index as u32,
                                        instruction_index: index as u32,
                                        name: create_event.name.clone(),
                                        symbol: create_event.symbol.clone(),
                                        uri: create_event.uri.clone(),
                                        mint: global_bs58().encode_32(&create_event.mint),
                                        bonding_curve: global_bs58().encode_32(&create_event.bonding_curve),
                                        user: global_bs58().encode_32(&create_event.user),
                                        creator: global_bs58().encode_32(&create_event.creator),
                                        timestamp: create_event.timestamp as u32,
                                        virtual_token_reserves: create_event.virtual_token_reserves,
                                        virtual_sol_reserves: create_event.virtual_sol_reserves,
                                        real_token_reserves: create_event.real_token_reserves,
                                        token_total_supply: create_event.token_total_supply,
                                    };
                                    pumpfun_create_event_rows.push(event_v2);
                                }
                            }
                        }
                        "PumpFunMigrateEvent" => {
                            if let (Some(parsed_event), Some(_parsed_instr)) =
                                (&instr.parsed, &prev_instr.parsed)
                            {
                                if let proto_lib::transaction::solana::instruction::Parsed::PumpfunMigrationEvent(migrate_event) = parsed_event {
                                    let event_v2 = PumpfunMigrateEventV2 {
                                        signature: global_bs58().encode_64(&tx.signature),
                                        slot: tx.slot,
                                        transaction_index: tx.index as u32,
                                        instruction_index: index as u32,
                                        user: global_bs58().encode_32(&migrate_event.user),
                                        mint: global_bs58().encode_32(&migrate_event.mint),
                                        mint_amount: migrate_event.mint_amount,
                                        sol_amount: migrate_event.sol_amount,
                                        pool_migration_fee: migrate_event.pool_migration_fee,
                                        bonding_curve: global_bs58().encode_32(&migrate_event.bonding_curve),
                                        timestamp: migrate_event.timestamp as u32,
                                        pool: global_bs58().encode_32(&migrate_event.pool),
                                    };
                                    pumpfun_migrate_event_rows.push(event_v2);
                                }
                            }
                        }
                        "PumpFunAmmBuyEvent" => {
                            if let (Some(parsed_event), Some(parsed_instr)) =
                                (&instr.parsed, &prev_instr.parsed)
                            {
                                if let (
                                    proto_lib::transaction::solana::instruction::Parsed::PumpfunAmmBuyEvent(buy_event),
                                    proto_lib::transaction::solana::instruction::Parsed::PumpfunAmmBuy(buy_instr)
                                ) = (parsed_event, parsed_instr) {
                                    if let Some(accounts) = &buy_instr.accounts {
                                        let event_v2 = PumpfunAmmBuyEventV2 {
                                            signature: global_bs58().encode_64(&tx.signature),
                                            slot: tx.slot,
                                            transaction_index: tx.index as u32,
                                            instruction_index: index as u32,
                                            base_mint: global_bs58().encode_32(&accounts.base_mint),
                                            quote_mint: global_bs58().encode_32(&accounts.quote_mint),
                                            timestamp: buy_event.timestamp as u32,
                                            base_amount_out: buy_event.base_amount_out,
                                            max_quote_amount_in: buy_event.max_quote_amount_in,
                                            user_base_token_reserves: buy_event.user_base_token_reserves,
                                            user_quote_token_reserves: buy_event.user_quote_token_reserves,
                                            pool_base_token_reserves: buy_event.pool_base_token_reserves,
                                            pool_quote_token_reserves: buy_event.pool_quote_token_reserves,
                                            quote_amount_in: buy_event.quote_amount_in,
                                            lp_fee_basis_points: buy_event.lp_fee_basis_points,
                                            lp_fee: buy_event.lp_fee,
                                            protocol_fee_basis_points: buy_event.protocol_fee_basis_points,
                                            protocol_fee: buy_event.protocol_fee,
                                            quote_amount_in_with_lp_fee: buy_event.quote_amount_in_with_lp_fee,
                                            user_quote_amount_in: buy_event.user_quote_amount_in,
                                            pool: global_bs58().encode_32(&accounts.pool),
                                            user: global_bs58().encode_32(&accounts.user),
                                            user_base_token_account: global_bs58().encode_32(&accounts.user_base_token_account),
                                            user_quote_token_account: global_bs58().encode_32(&accounts.user_quote_token_account),
                                            protocol_fee_recipient: global_bs58().encode_32(&accounts.protocol_fee_recipient),
                                            protocol_fee_recipient_token_account: global_bs58().encode_32(&accounts.protocol_fee_recipient_token_account),
                                            coin_creator: global_bs58().encode_32(&buy_event.coin_creator),
                                            coin_creator_fee_basis_points: buy_event.coin_creator_fee_basis_points,
                                            coin_creator_fee: buy_event.coin_creator_fee,
                                            track_volume: buy_event.track_volume as u8,
                                            total_unclaimed_tokens: buy_event.total_unclaimed_tokens,
                                            total_claimed_tokens: buy_event.total_claimed_tokens,
                                            current_sol_volume: buy_event.current_sol_volume,
                                            last_update_timestamp: buy_event.last_update_timestamp,
                                            is_main_pool: buy_instr.is_main_pool as u8,
                                        };
                                        pumpfun_amm_buy_event_rows.push(event_v2);
                                    }
                                }
                            }
                        }
                        "PumpFunAmmSellEvent" => {
                            if let (Some(parsed_event), Some(parsed_instr)) =
                                (&instr.parsed, &prev_instr.parsed)
                            {
                                if let (
                                    proto_lib::transaction::solana::instruction::Parsed::PumpfunAmmSellEvent(sell_event),
                                    proto_lib::transaction::solana::instruction::Parsed::PumpfunAmmSell(sell_instr)
                                ) = (parsed_event, parsed_instr) {
                                    if let Some(accounts) = &sell_instr.accounts {
                                        let event_v2 = PumpfunAmmSellEventV2 {
                                            signature: global_bs58().encode_64(&tx.signature),
                                            slot: tx.slot,
                                            transaction_index: tx.index as u32,
                                            instruction_index: index as u32,
                                            base_mint: global_bs58().encode_32(&accounts.base_mint),
                                            quote_mint: global_bs58().encode_32(&accounts.quote_mint),
                                            timestamp: sell_event.timestamp as u32,
                                            base_amount_in: sell_event.base_amount_in,
                                            min_quote_amount_out: sell_event.min_quote_amount_out,
                                            user_base_token_reserves: sell_event.user_base_token_reserves,
                                            user_quote_token_reserves: sell_event.user_quote_token_reserves,
                                            pool_base_token_reserves: sell_event.pool_base_token_reserves,
                                            pool_quote_token_reserves: sell_event.pool_quote_token_reserves,
                                            quote_amount_out: sell_event.quote_amount_out,
                                            lp_fee_basis_points: sell_event.lp_fee_basis_points,
                                            lp_fee: sell_event.lp_fee,
                                            protocol_fee_basis_points: sell_event.protocol_fee_basis_points,
                                            protocol_fee: sell_event.protocol_fee,
                                            quote_amount_out_without_lp_fee: sell_event.quote_amount_out_without_lp_fee,
                                            user_quote_amount_out: sell_event.user_quote_amount_out,
                                            pool: global_bs58().encode_32(&accounts.pool),
                                            user: global_bs58().encode_32(&accounts.user),
                                            user_base_token_account: global_bs58().encode_32(&accounts.user_base_token_account),
                                            user_quote_token_account: global_bs58().encode_32(&accounts.user_quote_token_account),
                                            protocol_fee_recipient: global_bs58().encode_32(&accounts.protocol_fee_recipient),
                                            protocol_fee_recipient_token_account: global_bs58().encode_32(&accounts.protocol_fee_recipient_token_account),
                                            coin_creator: global_bs58().encode_32(&sell_event.coin_creator),
                                            coin_creator_fee_basis_points: sell_event.coin_creator_fee_basis_points,
                                            coin_creator_fee: sell_event.coin_creator_fee,
                                            is_main_pool: sell_instr.is_main_pool as u8,
                                        };
                                        pumpfun_amm_sell_event_rows.push(event_v2);
                                    }
                                }
                            }
                        }
                        "PumpFunAmmDepositEvent" => {
                            if let (Some(parsed_event), Some(parsed_instr)) =
                                (&instr.parsed, &prev_instr.parsed)
                            {
                                if let (
                                    proto_lib::transaction::solana::instruction::Parsed::PumpfunAmmDepositEvent(deposit_event),
                                    proto_lib::transaction::solana::instruction::Parsed::PumpfunAmmDeposit(deposit_instr)
                                ) = (parsed_event, parsed_instr) {
                                    if let Some(accounts) = &deposit_instr.accounts {
                                        let event_v2 = PumpfunAmmDepositEventV2 {
                                            signature: global_bs58().encode_64(&tx.signature),
                                            slot: tx.slot,
                                            transaction_index: tx.index as u32,
                                            instruction_index: index as u32,
                                            base_mint: global_bs58().encode_32(&accounts.base_mint),
                                            quote_mint: global_bs58().encode_32(&accounts.quote_mint),
                                            timestamp: deposit_event.timestamp as u32,
                                            lp_token_amount_out: deposit_event.lp_token_amount_out,
                                            max_base_amount_in: deposit_event.max_base_amount_in,
                                            max_quote_amount_in: deposit_event.max_quote_amount_in,
                                            user_base_token_reserves: deposit_event.user_base_token_reserves,
                                            user_quote_token_reserves: deposit_event.user_quote_token_reserves,
                                            pool_base_token_reserves: deposit_event.pool_base_token_reserves,
                                            pool_quote_token_reserves: deposit_event.pool_quote_token_reserves,
                                            base_amount_in: deposit_event.base_amount_in,
                                            quote_amount_in: deposit_event.quote_amount_in,
                                            lp_mint_supply: deposit_event.lp_mint_supply,
                                            pool: global_bs58().encode_32(&accounts.pool),
                                            user: global_bs58().encode_32(&accounts.user),
                                            user_base_token_account: global_bs58().encode_32(&accounts.user_base_token_account),
                                            user_quote_token_account: global_bs58().encode_32(&accounts.user_quote_token_account),
                                            user_pool_token_account: global_bs58().encode_32(&accounts.user_pool_token_account),
                                            is_main_pool: deposit_instr.is_main_pool as u8,
                                        };
                                        pumpfun_amm_deposit_event_rows.push(event_v2);
                                    }
                                }
                            }
                        }
                        "PumpFunAmmWithdrawEvent" => {
                            if let (Some(parsed_event), Some(parsed_instr)) =
                                (&instr.parsed, &prev_instr.parsed)
                            {
                                if let (
                                    proto_lib::transaction::solana::instruction::Parsed::PumpfunAmmWithdrawEvent(withdraw_event),
                                    proto_lib::transaction::solana::instruction::Parsed::PumpfunAmmWithdraw(withdraw_instr)
                                ) = (parsed_event, parsed_instr) {
                                    if let Some(accounts) = &withdraw_instr.accounts {
                                        let event_v2 = PumpfunAmmWithdrawEventV2 {
                                            signature: global_bs58().encode_64(&tx.signature),
                                            slot: tx.slot,
                                            transaction_index: tx.index as u32,
                                            instruction_index: index as u32,
                                            base_mint: global_bs58().encode_32(&accounts.base_mint),
                                            quote_mint: global_bs58().encode_32(&accounts.quote_mint),
                                            timestamp: withdraw_event.timestamp as u32,
                                            lp_token_amount_in: withdraw_event.lp_token_amount_in,
                                            min_base_amount_out: withdraw_event.min_base_amount_out,
                                            min_quote_amount_out: withdraw_event.min_quote_amount_out,
                                            user_base_token_reserves: withdraw_event.user_base_token_reserves,
                                            user_quote_token_reserves: withdraw_event.user_quote_token_reserves,
                                            pool_base_token_reserves: withdraw_event.pool_base_token_reserves,
                                            pool_quote_token_reserves: withdraw_event.pool_quote_token_reserves,
                                            base_amount_out: withdraw_event.base_amount_out,
                                            quote_amount_out: withdraw_event.quote_amount_out,
                                            lp_mint_supply: withdraw_event.lp_mint_supply,
                                            pool: global_bs58().encode_32(&accounts.pool),
                                            user: global_bs58().encode_32(&accounts.user),
                                            user_base_token_account: global_bs58().encode_32(&accounts.user_base_token_account),
                                            user_quote_token_account: global_bs58().encode_32(&accounts.user_quote_token_account),
                                            user_pool_token_account: global_bs58().encode_32(&accounts.user_pool_token_account),
                                            is_main_pool: withdraw_instr.is_main_pool as u8,
                                        };
                                        pumpfun_amm_withdraw_event_rows.push(event_v2);
                                    }
                                }
                            }
                        }
                        "PumpFunAmmCreatePoolEvent" => {
                            if let (Some(parsed_event), Some(parsed_instr)) =
                                (&instr.parsed, &prev_instr.parsed)
                            {
                                if let (
                                    proto_lib::transaction::solana::instruction::Parsed::PumpfunAmmCreatePoolEvent(create_event),
                                    proto_lib::transaction::solana::instruction::Parsed::PumpfunAmmCreatePool(create_instr)
                                ) = (parsed_event, parsed_instr) {
                                    if let Some(accounts) = &create_instr.accounts {
                                        let event_v2 = PumpfunAmmCreatePoolEventV2 {
                                            signature: global_bs58().encode_64(&tx.signature),
                                            slot: tx.slot,
                                            transaction_index: tx.index as u32,
                                            instruction_index: index as u32,
                                            timestamp: create_event.timestamp as u32,
                                            index: create_event.index,
                                            creator: global_bs58().encode_32(&accounts.creator),
                                            base_mint: global_bs58().encode_32(&accounts.base_mint),
                                            quote_mint: global_bs58().encode_32(&accounts.quote_mint),
                                            base_mint_decimals: create_event.base_mint_decimals,
                                            quote_mint_decimals: create_event.quote_mint_decimals,
                                            base_amount_in: create_event.base_amount_in,
                                            quote_amount_in: create_event.quote_amount_in,
                                            pool_base_amount: create_event.pool_base_amount,
                                            pool_quote_amount: create_event.pool_quote_amount,
                                            minimum_liquidity: create_event.minimum_liquidity,
                                            initial_liquidity: create_event.initial_liquidity,
                                            lp_token_amount_out: create_event.lp_token_amount_out,
                                            pool_bump: create_event.pool_bump,
                                            pool: global_bs58().encode_32(&accounts.pool),
                                            lp_mint: global_bs58().encode_32(&accounts.lp_mint),
                                            user_base_token_account: global_bs58().encode_32(&accounts.user_base_token_account),
                                            user_quote_token_account: global_bs58().encode_32(&accounts.user_quote_token_account),
                                            coin_creator: global_bs58().encode_32(&create_event.coin_creator),
                                            is_main_pool: create_instr.is_main_pool as u8,
                                        };
                                        pumpfun_amm_create_pool_event_rows.push(event_v2);
                                    }
                                }
                            }
                        }
                        // 其它 PumpFunAmmXXXEvent 可用同样方式补全
                        _ => {}
                    }
                }
            } else {
                // 不是event，入栈
                stack.push(instr);
            }
            index += 1;
        }
    }
}

// 判断是否为event类型
fn is_event(instr: &proto_lib::transaction::solana::Instruction) -> bool {
    matches!(
        instr.r#type.as_str(),
        "PumpFunTradeEvent"
            | "PumpFunCreateEvent"
            | "PumpFunMigrateEvent"
            | "PumpFunAmmBuyEvent"
            | "PumpFunAmmSellEvent"
            | "PumpFunAmmDepositEvent"
            | "PumpFunAmmWithdrawEvent"
            | "PumpFunAmmCreatePoolEvent"
    )
}
