use utils::clickhouse_events::*;

#[test]
fn test_vec_to_arrow_and_back() {
    let events = vec![
        PumpfunTradeEventV2 {
            signature: "sig1".to_string(),
            slot: 1,
            transaction_index: 0,
            instruction_index: 0,
            mint: "mint1".to_string(),
            sol_amount: 100,
            token_amount: 200,
            is_buy: 1,
            user: "user1".to_string(),
            timestamp: 123456,
            virtual_sol_reserves: 10,
            virtual_token_reserves: 20,
            real_sol_reserves: 30,
            real_token_reserves: 40,
            fee_recipient: "fee1".to_string(),
            fee_basis_points: 5,
            fee: 6,
            creator: "creator1".to_string(),
            creator_fee_basis_points: 7,
            creator_fee: 8,
            track_volume: 1,
            total_unclaimed_tokens: 9,
            total_claimed_tokens: 10,
            current_sol_volume: 11,
            last_update_timestamp: 123456789,
        },
    ];
    let batch = vec_to_arrow_batch(&events);
    let restored: Vec<PumpfunTradeEventV2> = arrow_batch_to_vec(&batch);
    assert_eq!(events, restored);
}

#[test]
fn test_vec_to_arrow_and_back_create() {
    let events = vec![PumpfunCreateEventV2 {
        signature: "sig2".to_string(),
        slot: 2,
        transaction_index: 1,
        instruction_index: 1,
        name: "name2".to_string(),
        symbol: "symbol2".to_string(),
        uri: "uri2".to_string(),
        mint: "mint2".to_string(),
        bonding_curve: "curve2".to_string(),
        user: "user2".to_string(),
        creator: "creator2".to_string(),
        timestamp: 654321,
        virtual_token_reserves: 21,
        virtual_sol_reserves: 22,
        real_token_reserves: 23,
        token_total_supply: 24,
    }];
    let batch = vec_to_arrow_batch(&events);
    let restored: Vec<PumpfunCreateEventV2> = arrow_batch_to_vec(&batch);
    assert_eq!(events, restored);
}

#[test]
fn test_vec_to_arrow_and_back_migrate() {
    let events = vec![PumpfunMigrateEventV2 {
        signature: "sig3".to_string(),
        slot: 3,
        transaction_index: 2,
        instruction_index: 2,
        user: "user3".to_string(),
        mint: "mint3".to_string(),
        mint_amount: 31,
        sol_amount: 32,
        pool_migration_fee: 33,
        bonding_curve: "curve3".to_string(),
        timestamp: 333333,
        pool: "pool3".to_string(),
    }];
    let batch = vec_to_arrow_batch(&events);
    let restored: Vec<PumpfunMigrateEventV2> = arrow_batch_to_vec(&batch);
    assert_eq!(events, restored);
}

#[test]
fn test_vec_to_arrow_and_back_buy() {
    let events = vec![PumpfunAmmBuyEventV2 {
        signature: "sig4".to_string(),
        slot: 4,
        transaction_index: 3,
        instruction_index: 3,
        base_mint: "base4".to_string(),
        quote_mint: "quote4".to_string(),
        timestamp: 444444,
        base_amount_out: 41,
        max_quote_amount_in: 42,
        user_base_token_reserves: 43,
        user_quote_token_reserves: 44,
        pool_base_token_reserves: 45,
        pool_quote_token_reserves: 46,
        quote_amount_in: 47,
        lp_fee_basis_points: 48,
        lp_fee: 49,
        protocol_fee_basis_points: 50,
        protocol_fee: 51,
        quote_amount_in_with_lp_fee: 52,
        user_quote_amount_in: 53,
        pool: "pool4".to_string(),
        user: "user4".to_string(),
        user_base_token_account: "uba4".to_string(),
        user_quote_token_account: "uqa4".to_string(),
        protocol_fee_recipient: "pfr4".to_string(),
        protocol_fee_recipient_token_account: "pfra4".to_string(),
        coin_creator: "cc4".to_string(),
        coin_creator_fee_basis_points: 54,
        coin_creator_fee: 55,
        track_volume: 1,
        total_unclaimed_tokens: 56,
        total_claimed_tokens: 57,
        current_sol_volume: 58,
        last_update_timestamp: 44444444,
        is_main_pool: 1,
    }];
    let batch = vec_to_arrow_batch(&events);
    let restored: Vec<PumpfunAmmBuyEventV2> = arrow_batch_to_vec(&batch);
    assert_eq!(events, restored);
}

#[test]
fn test_vec_to_arrow_and_back_sell() {
    let events = vec![PumpfunAmmSellEventV2 {
        signature: "sig5".to_string(),
        slot: 5,
        transaction_index: 4,
        instruction_index: 4,
        base_mint: "base5".to_string(),
        quote_mint: "quote5".to_string(),
        timestamp: 555555,
        base_amount_in: 61,
        min_quote_amount_out: 62,
        user_base_token_reserves: 63,
        user_quote_token_reserves: 64,
        pool_base_token_reserves: 65,
        pool_quote_token_reserves: 66,
        quote_amount_out: 67,
        lp_fee_basis_points: 68,
        lp_fee: 69,
        protocol_fee_basis_points: 70,
        protocol_fee: 71,
        quote_amount_out_without_lp_fee: 72,
        user_quote_amount_out: 73,
        pool: "pool5".to_string(),
        user: "user5".to_string(),
        user_base_token_account: "uba5".to_string(),
        user_quote_token_account: "uqa5".to_string(),
        protocol_fee_recipient: "pfr5".to_string(),
        protocol_fee_recipient_token_account: "pfra5".to_string(),
        coin_creator: "cc5".to_string(),
        coin_creator_fee_basis_points: 74,
        coin_creator_fee: 75,
        is_main_pool: 1,
    }];
    let batch = vec_to_arrow_batch(&events);
    let restored: Vec<PumpfunAmmSellEventV2> = arrow_batch_to_vec(&batch);
    assert_eq!(events, restored);
}

#[test]
fn test_vec_to_arrow_and_back_create_pool() {
    let events = vec![PumpfunAmmCreatePoolEventV2 {
        signature: "sig6".to_string(),
        slot: 6,
        transaction_index: 5,
        instruction_index: 5,
        timestamp: 666666,
        index: 81,
        creator: "creator6".to_string(),
        base_mint: "base6".to_string(),
        quote_mint: "quote6".to_string(),
        base_mint_decimals: 82,
        quote_mint_decimals: 83,
        base_amount_in: 84,
        quote_amount_in: 85,
        pool_base_amount: 86,
        pool_quote_amount: 87,
        minimum_liquidity: 88,
        initial_liquidity: 89,
        lp_token_amount_out: 90,
        pool_bump: 91,
        pool: "pool6".to_string(),
        lp_mint: "lpm6".to_string(),
        user_base_token_account: "uba6".to_string(),
        user_quote_token_account: "uqa6".to_string(),
        coin_creator: "cc6".to_string(),
        is_main_pool: 1,
    }];
    let batch = vec_to_arrow_batch(&events);
    let restored: Vec<PumpfunAmmCreatePoolEventV2> = arrow_batch_to_vec(&batch);
    assert_eq!(events, restored);
}

#[test]
fn test_vec_to_arrow_and_back_deposit() {
    let events = vec![PumpfunAmmDepositEventV2 {
        signature: "sig7".to_string(),
        slot: 7,
        transaction_index: 6,
        instruction_index: 6,
        base_mint: "base7".to_string(),
        quote_mint: "quote7".to_string(),
        timestamp: 777777,
        lp_token_amount_out: 101,
        max_base_amount_in: 102,
        max_quote_amount_in: 103,
        user_base_token_reserves: 104,
        user_quote_token_reserves: 105,
        pool_base_token_reserves: 106,
        pool_quote_token_reserves: 107,
        base_amount_in: 108,
        quote_amount_in: 109,
        lp_mint_supply: 110,
        pool: "pool7".to_string(),
        user: "user7".to_string(),
        user_base_token_account: "uba7".to_string(),
        user_quote_token_account: "uqa7".to_string(),
        user_pool_token_account: "upa7".to_string(),
        is_main_pool: 1,
    }];
    let batch = vec_to_arrow_batch(&events);
    let restored: Vec<PumpfunAmmDepositEventV2> = arrow_batch_to_vec(&batch);
    assert_eq!(events, restored);
}

#[test]
fn test_vec_to_arrow_and_back_withdraw() {
    let events = vec![PumpfunAmmWithdrawEventV2 {
        signature: "sig8".to_string(),
        slot: 8,
        transaction_index: 7,
        instruction_index: 7,
        base_mint: "base8".to_string(),
        quote_mint: "quote8".to_string(),
        timestamp: 888888,
        lp_token_amount_in: 111,
        min_base_amount_out: 112,
        min_quote_amount_out: 113,
        user_base_token_reserves: 114,
        user_quote_token_reserves: 115,
        pool_base_token_reserves: 116,
        pool_quote_token_reserves: 117,
        base_amount_out: 118,
        quote_amount_out: 119,
        lp_mint_supply: 120,
        pool: "pool8".to_string(),
        user: "user8".to_string(),
        user_base_token_account: "uba8".to_string(),
        user_quote_token_account: "uqa8".to_string(),
        user_pool_token_account: "upa8".to_string(),
        is_main_pool: 1,
    }];
    let batch = vec_to_arrow_batch(&events);
    let restored: Vec<PumpfunAmmWithdrawEventV2> = arrow_batch_to_vec(&batch);
    assert_eq!(events, restored);
}
