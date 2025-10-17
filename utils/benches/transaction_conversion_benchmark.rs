use criterion::{criterion_group, criterion_main, Criterion};
use proto_lib::transaction::solana::{self, Transaction};
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use utils::clickhouse_events;
use utils::convert_transaction::TransactionConverter;

const SEED: u64 = 42;

// Helper function to generate random 32-byte array
fn random_bytes_32(rng: &mut StdRng) -> Vec<u8> {
    (0..32).map(|_| rng.random::<u8>()).collect()
}

// 1. PumpFun Trade
fn create_pumpfun_trade_tx(rng: &mut StdRng) -> Transaction {
    let mut tx = Transaction::default();
    tx.slot = rng.random_range(100000..200000);
    tx.index = rng.random_range(0..1000);
    tx.signature = (0..64).map(|_| rng.random::<u8>()).collect();
    
    let instr = solana::Instruction {
        r#type: "PumpFunBuy".to_string(),
        parsed: Some(solana::instruction::Parsed::PumpfunBuy(
            proto_lib::transaction::pumpfun::instructions::Buy {
                amount: rng.random(),
                max_sol_cost: rng.random(),
                track_volume: Some(true),
                accounts: Some(proto_lib::transaction::pumpfun::instructions::BuyAccounts {
                    global_account: random_bytes_32(rng),
                    fee_recipient: random_bytes_32(rng),
                    mint: random_bytes_32(rng),
                    bonding_curve: random_bytes_32(rng),
                    associated_bonding_curve: random_bytes_32(rng),
                    associated_user: random_bytes_32(rng),
                    user: random_bytes_32(rng),
                    system_program: random_bytes_32(rng),
                    token_program: random_bytes_32(rng),
                    creator_vault: random_bytes_32(rng),
                    event_authority: random_bytes_32(rng),
                    program: random_bytes_32(rng),
                    global_volume_accumulator: random_bytes_32(rng),
                    user_volume_accumulator: random_bytes_32(rng),
                    fee_config: random_bytes_32(rng),
                    fee_program: random_bytes_32(rng),
                }),
            }
        )),
    };
    
    let event = solana::Instruction {
        r#type: "PumpFunTradeEvent".to_string(),
        parsed: Some(solana::instruction::Parsed::PumpfunTradeEvent(
            proto_lib::transaction::pumpfun::events::TradeEvent {
                mint: random_bytes_32(rng),
                sol_amount: rng.random(),
                token_amount: rng.random(),
                is_buy: true,
                user: random_bytes_32(rng),
                timestamp: rng.random(),
                virtual_sol_reserves: rng.random(),
                virtual_token_reserves: rng.random(),
                real_sol_reserves: rng.random(),
                real_token_reserves: rng.random(),
                fee_recipient: random_bytes_32(rng),
                fee_basis_points: rng.random(),
                fee: rng.random(),
                creator: random_bytes_32(rng),
                creator_fee_basis_points: rng.random(),
                creator_fee: rng.random(),
                track_volume: true,
                total_unclaimed_tokens: rng.random(),
                total_claimed_tokens: rng.random(),
                current_sol_volume: rng.random(),
                last_update_timestamp: rng.random(),
            }
        )),
    };
    
    tx.instructions = vec![instr, event];
    tx
}

// 2. PumpFun Create
fn create_pumpfun_create_tx(rng: &mut StdRng) -> Transaction {
    let mut tx = Transaction::default();
    tx.slot = rng.random_range(100000..200000);
    tx.index = rng.random_range(0..1000);
    tx.signature = (0..64).map(|_| rng.random::<u8>()).collect();
    
    let instr = solana::Instruction {
        r#type: "PumpFunCreate".to_string(),
        parsed: Some(solana::instruction::Parsed::PumpfunCreate(
            proto_lib::transaction::pumpfun::instructions::Create {
                name: "Test Token".to_string(),
                symbol: "TEST".to_string(),
                uri: "https://test.com/metadata.json".to_string(),
                creator: random_bytes_32(rng),
                accounts: Some(proto_lib::transaction::pumpfun::instructions::CreateAccounts {
                    mint: random_bytes_32(rng),
                    mint_authority: random_bytes_32(rng),
                    bonding_curve: random_bytes_32(rng),
                    associated_bonding_curve: random_bytes_32(rng),
                    global_account: random_bytes_32(rng),
                    mpl_token_metadata: random_bytes_32(rng),
                    metadata: random_bytes_32(rng),
                    user: random_bytes_32(rng),
                    system_program: random_bytes_32(rng),
                    token_program: random_bytes_32(rng),
                    associated_token_program: random_bytes_32(rng),
                    rent: random_bytes_32(rng),
                    event_authority: random_bytes_32(rng),
                    program: random_bytes_32(rng),
                }),
            }
        )),
    };
    
    let event = solana::Instruction {
        r#type: "PumpFunCreateEvent".to_string(),
        parsed: Some(solana::instruction::Parsed::PumpfunCreateEvent(
            proto_lib::transaction::pumpfun::events::CreateEvent {
                name: "Test Token".to_string(),
                symbol: "TEST".to_string(),
                uri: "https://test.com/metadata.json".to_string(),
                mint: random_bytes_32(rng),
                bonding_curve: random_bytes_32(rng),
                user: random_bytes_32(rng),
                creator: random_bytes_32(rng),
                timestamp: rng.random(),
                virtual_token_reserves: rng.random(),
                virtual_sol_reserves: rng.random(),
                real_token_reserves: rng.random(),
                token_total_supply: rng.random(),
            }
        )),
    };
    
    tx.instructions = vec![instr, event];
    tx
}

// 3. PumpFun Migrate
fn create_pumpfun_migrate_tx(rng: &mut StdRng) -> Transaction {
    let mut tx = Transaction::default();
    tx.slot = rng.random_range(100000..200000);
    tx.index = rng.random_range(0..1000);
    tx.signature = (0..64).map(|_| rng.random::<u8>()).collect();
    
    let instr = solana::Instruction {
        r#type: "PumpFunMigrate".to_string(),
        parsed: Some(solana::instruction::Parsed::PumpfunMigrate(
            proto_lib::transaction::pumpfun::instructions::Migrate {
                accounts: Some(proto_lib::transaction::pumpfun::instructions::MigrateAccounts {
                    global_account: random_bytes_32(rng),
                    withdraw_authority: random_bytes_32(rng),
                    mint: random_bytes_32(rng),
                    bonding_curve: random_bytes_32(rng),
                    associated_bonding_curve: random_bytes_32(rng),
                    user: random_bytes_32(rng),
                    system_program: random_bytes_32(rng),
                    token_program: random_bytes_32(rng),
                    pump_amm: random_bytes_32(rng),
                    pool: random_bytes_32(rng),
                    pool_authority: random_bytes_32(rng),
                    pool_authority_mint_account: random_bytes_32(rng),
                    pool_authority_wsol_account: random_bytes_32(rng),
                    amm_global_config: random_bytes_32(rng),
                    wsol_mint: random_bytes_32(rng),
                    lp_mint: random_bytes_32(rng),
                    user_pool_token_account: random_bytes_32(rng),
                    pool_base_token_account: random_bytes_32(rng),
                    pool_quote_token_account: random_bytes_32(rng),
                    token_2022_program: random_bytes_32(rng),
                    associated_token_program: random_bytes_32(rng),
                    pump_amm_event_authority: random_bytes_32(rng),
                    event_authority: random_bytes_32(rng),
                    program: random_bytes_32(rng),
                }),
            }
        )),
    };
    
    let event = solana::Instruction {
        r#type: "PumpFunMigrateEvent".to_string(),
        parsed: Some(solana::instruction::Parsed::PumpfunMigrationEvent(
            proto_lib::transaction::pumpfun::events::MigrationEvent {
                user: random_bytes_32(rng),
                mint: random_bytes_32(rng),
                mint_amount: rng.random(),
                sol_amount: rng.random(),
                pool_migration_fee: rng.random(),
                bonding_curve: random_bytes_32(rng),
                timestamp: rng.random(),
                pool: random_bytes_32(rng),
            }
        )),
    };
    
    tx.instructions = vec![instr, event];
    tx
}

// 4. PumpFun AMM Buy
fn create_pumpfun_amm_buy_tx(rng: &mut StdRng) -> Transaction {
    let mut tx = Transaction::default();
    tx.slot = rng.random_range(100000..200000);
    tx.index = rng.random_range(0..1000);
    tx.signature = (0..64).map(|_| rng.random::<u8>()).collect();
    
    let instr = solana::Instruction {
        r#type: "PumpFunAmmBuy".to_string(),
        parsed: Some(solana::instruction::Parsed::PumpfunAmmBuy(
            proto_lib::transaction::pumpfun_amm::instructions::Buy {
                base_amount_out: rng.random(),
                max_quote_amount_in: rng.random(),
                track_volume: Some(true),
                is_main_pool: true,
                accounts: Some(proto_lib::transaction::pumpfun_amm::instructions::BuyAccounts {
                    pool: random_bytes_32(rng),
                    user: random_bytes_32(rng),
                    global_config: random_bytes_32(rng),
                    base_mint: random_bytes_32(rng),
                    quote_mint: random_bytes_32(rng),
                    user_base_token_account: random_bytes_32(rng),
                    user_quote_token_account: random_bytes_32(rng),
                    pool_base_token_account: random_bytes_32(rng),
                    pool_quote_token_account: random_bytes_32(rng),
                    protocol_fee_recipient: random_bytes_32(rng),
                    protocol_fee_recipient_token_account: random_bytes_32(rng),
                    base_token_program: random_bytes_32(rng),
                    quote_token_program: random_bytes_32(rng),
                    system_program: random_bytes_32(rng),
                    associated_token_program: random_bytes_32(rng),
                    event_authority: random_bytes_32(rng),
                    program: random_bytes_32(rng),
                    coin_creator_vault_ata: random_bytes_32(rng),
                    coin_creator_vault_authority: random_bytes_32(rng),
                    global_volume_accumulator: random_bytes_32(rng),
                    user_volume_accumulator: random_bytes_32(rng),
                    fee_config: random_bytes_32(rng),
                    fee_program: random_bytes_32(rng),
                }),
            }
        )),
    };
    
    let event = solana::Instruction {
        r#type: "PumpFunAmmBuyEvent".to_string(),
        parsed: Some(solana::instruction::Parsed::PumpfunAmmBuyEvent(
            proto_lib::transaction::pumpfun_amm::events::BuyEvent {
                timestamp: rng.random(),
                base_amount_out: rng.random(),
                max_quote_amount_in: rng.random(),
                user_base_token_reserves: rng.random(),
                user_quote_token_reserves: rng.random(),
                pool_base_token_reserves: rng.random(),
                pool_quote_token_reserves: rng.random(),
                quote_amount_in: rng.random(),
                lp_fee_basis_points: rng.random(),
                lp_fee: rng.random(),
                protocol_fee_basis_points: rng.random(),
                protocol_fee: rng.random(),
                quote_amount_in_with_lp_fee: rng.random(),
                user_quote_amount_in: rng.random(),
                pool: random_bytes_32(rng),
                user: random_bytes_32(rng),
                user_base_token_account: random_bytes_32(rng),
                user_quote_token_account: random_bytes_32(rng),
                protocol_fee_recipient: random_bytes_32(rng),
                protocol_fee_recipient_token_account: random_bytes_32(rng),
                coin_creator: random_bytes_32(rng),
                coin_creator_fee_basis_points: rng.random(),
                coin_creator_fee: rng.random(),
                track_volume: true,
                total_unclaimed_tokens: rng.random(),
                total_claimed_tokens: rng.random(),
                current_sol_volume: rng.random(),
                last_update_timestamp: rng.random(),
            }
        )),
    };
    
    tx.instructions = vec![instr, event];
    tx
}

// 5. PumpFun AMM Sell
fn create_pumpfun_amm_sell_tx(rng: &mut StdRng) -> Transaction {
    let mut tx = Transaction::default();
    tx.slot = rng.random_range(100000..200000);
    tx.index = rng.random_range(0..1000);
    tx.signature = (0..64).map(|_| rng.random::<u8>()).collect();
    
    let instr = solana::Instruction {
        r#type: "PumpFunAmmSell".to_string(),
        parsed: Some(solana::instruction::Parsed::PumpfunAmmSell(
            proto_lib::transaction::pumpfun_amm::instructions::Sell {
                base_amount_in: rng.random(),
                min_quote_amount_out: rng.random(),
                is_main_pool: true,
                accounts: Some(proto_lib::transaction::pumpfun_amm::instructions::SellAccounts {
                    pool: random_bytes_32(rng),
                    user: random_bytes_32(rng),
                    global_config: random_bytes_32(rng),
                    base_mint: random_bytes_32(rng),
                    quote_mint: random_bytes_32(rng),
                    user_base_token_account: random_bytes_32(rng),
                    user_quote_token_account: random_bytes_32(rng),
                    pool_base_token_account: random_bytes_32(rng),
                    pool_quote_token_account: random_bytes_32(rng),
                    protocol_fee_recipient: random_bytes_32(rng),
                    protocol_fee_recipient_token_account: random_bytes_32(rng),
                    base_token_program: random_bytes_32(rng),
                    quote_token_program: random_bytes_32(rng),
                    system_program: random_bytes_32(rng),
                    associated_token_program: random_bytes_32(rng),
                    event_authority: random_bytes_32(rng),
                    program: random_bytes_32(rng),
                    coin_creator_vault_ata: random_bytes_32(rng),
                    coin_creator_vault_authority: random_bytes_32(rng),
                    fee_config: random_bytes_32(rng),
                    fee_program: random_bytes_32(rng),
                }),
            }
        )),
    };
    
    let event = solana::Instruction {
        r#type: "PumpFunAmmSellEvent".to_string(),
        parsed: Some(solana::instruction::Parsed::PumpfunAmmSellEvent(
            proto_lib::transaction::pumpfun_amm::events::SellEvent {
                timestamp: rng.random(),
                base_amount_in: rng.random(),
                min_quote_amount_out: rng.random(),
                user_base_token_reserves: rng.random(),
                user_quote_token_reserves: rng.random(),
                pool_base_token_reserves: rng.random(),
                pool_quote_token_reserves: rng.random(),
                quote_amount_out: rng.random(),
                lp_fee_basis_points: rng.random(),
                lp_fee: rng.random(),
                protocol_fee_basis_points: rng.random(),
                protocol_fee: rng.random(),
                quote_amount_out_without_lp_fee: rng.random(),
                user_quote_amount_out: rng.random(),
                pool: random_bytes_32(rng),
                user: random_bytes_32(rng),
                user_base_token_account: random_bytes_32(rng),
                user_quote_token_account: random_bytes_32(rng),
                protocol_fee_recipient: random_bytes_32(rng),
                protocol_fee_recipient_token_account: random_bytes_32(rng),
                coin_creator: random_bytes_32(rng),
                coin_creator_fee_basis_points: rng.random(),
                coin_creator_fee: rng.random(),
            }
        )),
    };
    
    tx.instructions = vec![instr, event];
    tx
}

// 6. PumpFun AMM CreatePool
fn create_pumpfun_amm_create_pool_tx(rng: &mut StdRng) -> Transaction {
    let mut tx = Transaction::default();
    tx.slot = rng.random_range(100000..200000);
    tx.index = rng.random_range(0..1000);
    tx.signature = (0..64).map(|_| rng.random::<u8>()).collect();
    
    let instr = solana::Instruction {
        r#type: "PumpFunAmmCreatePool".to_string(),
        parsed: Some(solana::instruction::Parsed::PumpfunAmmCreatePool(
            proto_lib::transaction::pumpfun_amm::instructions::CreatePool {
                index: rng.random(),
                base_amount_in: rng.random(),
                quote_amount_in: rng.random(),
                coin_creator: random_bytes_32(rng),
                is_main_pool: true,
                accounts: Some(proto_lib::transaction::pumpfun_amm::instructions::CreatePoolAccounts {
                    pool: random_bytes_32(rng),
                    global_config: random_bytes_32(rng),
                    creator: random_bytes_32(rng),
                    base_mint: random_bytes_32(rng),
                    quote_mint: random_bytes_32(rng),
                    lp_mint: random_bytes_32(rng),
                    user_base_token_account: random_bytes_32(rng),
                    user_quote_token_account: random_bytes_32(rng),
                    user_pool_token_account: random_bytes_32(rng),
                    pool_base_token_account: random_bytes_32(rng),
                    pool_quote_token_account: random_bytes_32(rng),
                    system_program: random_bytes_32(rng),
                    token_2022_program: random_bytes_32(rng),
                    base_token_program: random_bytes_32(rng),
                    quote_token_program: random_bytes_32(rng),
                    associated_token_program: random_bytes_32(rng),
                    event_authority: random_bytes_32(rng),
                    program: random_bytes_32(rng),
                }),
            }
        )),
    };
    
    let event = solana::Instruction {
        r#type: "PumpFunAmmCreatePoolEvent".to_string(),
        parsed: Some(solana::instruction::Parsed::PumpfunAmmCreatePoolEvent(
            proto_lib::transaction::pumpfun_amm::events::CreatePoolEvent {
                timestamp: rng.random(),
                index: rng.random(),
                creator: random_bytes_32(rng),
                base_mint: random_bytes_32(rng),
                quote_mint: random_bytes_32(rng),
                base_mint_decimals: rng.random(),
                quote_mint_decimals: rng.random(),
                base_amount_in: rng.random(),
                quote_amount_in: rng.random(),
                pool_base_amount: rng.random(),
                pool_quote_amount: rng.random(),
                minimum_liquidity: rng.random(),
                initial_liquidity: rng.random(),
                lp_token_amount_out: rng.random(),
                pool_bump: rng.random(),
                pool: random_bytes_32(rng),
                lp_mint: random_bytes_32(rng),
                user_base_token_account: random_bytes_32(rng),
                user_quote_token_account: random_bytes_32(rng),
                coin_creator: random_bytes_32(rng),
            }
        )),
    };
    
    tx.instructions = vec![instr, event];
    tx
}

// 7. PumpFun AMM Deposit
fn create_pumpfun_amm_deposit_tx(rng: &mut StdRng) -> Transaction {
    let mut tx = Transaction::default();
    tx.slot = rng.random_range(100000..200000);
    tx.index = rng.random_range(0..1000);
    tx.signature = (0..64).map(|_| rng.random::<u8>()).collect();
    
    let instr = solana::Instruction {
        r#type: "PumpFunAmmDeposit".to_string(),
        parsed: Some(solana::instruction::Parsed::PumpfunAmmDeposit(
            proto_lib::transaction::pumpfun_amm::instructions::Deposit {
                lp_token_amount_out: rng.random(),
                max_base_amount_in: rng.random(),
                max_quote_amount_in: rng.random(),
                is_main_pool: true,
                accounts: Some(proto_lib::transaction::pumpfun_amm::instructions::DepositAccounts {
                    pool: random_bytes_32(rng),
                    global_config: random_bytes_32(rng),
                    user: random_bytes_32(rng),
                    base_mint: random_bytes_32(rng),
                    quote_mint: random_bytes_32(rng),
                    lp_mint: random_bytes_32(rng),
                    user_base_token_account: random_bytes_32(rng),
                    user_quote_token_account: random_bytes_32(rng),
                    user_pool_token_account: random_bytes_32(rng),
                    pool_base_token_account: random_bytes_32(rng),
                    pool_quote_token_account: random_bytes_32(rng),
                    token_program: random_bytes_32(rng),
                    token_2022_program: random_bytes_32(rng),
                    event_authority: random_bytes_32(rng),
                    program: random_bytes_32(rng),
                }),
            }
        )),
    };
    
    let event = solana::Instruction {
        r#type: "PumpFunAmmDepositEvent".to_string(),
        parsed: Some(solana::instruction::Parsed::PumpfunAmmDepositEvent(
            proto_lib::transaction::pumpfun_amm::events::DepositEvent {
                timestamp: rng.random(),
                lp_token_amount_out: rng.random(),
                max_base_amount_in: rng.random(),
                max_quote_amount_in: rng.random(),
                user_base_token_reserves: rng.random(),
                user_quote_token_reserves: rng.random(),
                pool_base_token_reserves: rng.random(),
                pool_quote_token_reserves: rng.random(),
                base_amount_in: rng.random(),
                quote_amount_in: rng.random(),
                lp_mint_supply: rng.random(),
                pool: random_bytes_32(rng),
                user: random_bytes_32(rng),
                user_base_token_account: random_bytes_32(rng),
                user_quote_token_account: random_bytes_32(rng),
                user_pool_token_account: random_bytes_32(rng),
            }
        )),
    };
    
    tx.instructions = vec![instr, event];
    tx
}

// 8. PumpFun AMM Withdraw
fn create_pumpfun_amm_withdraw_tx(rng: &mut StdRng) -> Transaction {
    let mut tx = Transaction::default();
    tx.slot = rng.random_range(100000..200000);
    tx.index = rng.random_range(0..1000);
    tx.signature = (0..64).map(|_| rng.random::<u8>()).collect();
    
    let instr = solana::Instruction {
        r#type: "PumpFunAmmWithdraw".to_string(),
        parsed: Some(solana::instruction::Parsed::PumpfunAmmWithdraw(
            proto_lib::transaction::pumpfun_amm::instructions::Withdraw {
                lp_token_amount_in: rng.random(),
                min_base_amount_out: rng.random(),
                min_quote_amount_out: rng.random(),
                is_main_pool: true,
                accounts: Some(proto_lib::transaction::pumpfun_amm::instructions::WithdrawAccounts {
                    pool: random_bytes_32(rng),
                    global_config: random_bytes_32(rng),
                    user: random_bytes_32(rng),
                    base_mint: random_bytes_32(rng),
                    quote_mint: random_bytes_32(rng),
                    lp_mint: random_bytes_32(rng),
                    user_base_token_account: random_bytes_32(rng),
                    user_quote_token_account: random_bytes_32(rng),
                    user_pool_token_account: random_bytes_32(rng),
                    pool_base_token_account: random_bytes_32(rng),
                    pool_quote_token_account: random_bytes_32(rng),
                    token_program: random_bytes_32(rng),
                    token_2022_program: random_bytes_32(rng),
                    event_authority: random_bytes_32(rng),
                    program: random_bytes_32(rng),
                }),
            }
        )),
    };
    
    let event = solana::Instruction {
        r#type: "PumpFunAmmWithdrawEvent".to_string(),
        parsed: Some(solana::instruction::Parsed::PumpfunAmmWithdrawEvent(
            proto_lib::transaction::pumpfun_amm::events::WithdrawEvent {
                timestamp: rng.random(),
                lp_token_amount_in: rng.random(),
                min_base_amount_out: rng.random(),
                min_quote_amount_out: rng.random(),
                user_base_token_reserves: rng.random(),
                user_quote_token_reserves: rng.random(),
                pool_base_token_reserves: rng.random(),
                pool_quote_token_reserves: rng.random(),
                base_amount_out: rng.random(),
                quote_amount_out: rng.random(),
                lp_mint_supply: rng.random(),
                pool: random_bytes_32(rng),
                user: random_bytes_32(rng),
                user_base_token_account: random_bytes_32(rng),
                user_quote_token_account: random_bytes_32(rng),
                user_pool_token_account: random_bytes_32(rng),
            }
        )),
    };
    
    tx.instructions = vec![instr, event];
    tx
}

fn benchmark_individual_events(c: &mut Criterion) {
    let mut group = c.benchmark_group("individual_events");
    let mut rng = StdRng::seed_from_u64(SEED);
    
    // 预生成测试数据
    let pumpfun_trade_tx = create_pumpfun_trade_tx(&mut rng);
    let pumpfun_create_tx = create_pumpfun_create_tx(&mut rng);
    let pumpfun_migrate_tx = create_pumpfun_migrate_tx(&mut rng);
    let pumpfun_amm_buy_tx = create_pumpfun_amm_buy_tx(&mut rng);
    let pumpfun_amm_sell_tx = create_pumpfun_amm_sell_tx(&mut rng);
    let pumpfun_amm_create_pool_tx = create_pumpfun_amm_create_pool_tx(&mut rng);
    let pumpfun_amm_deposit_tx = create_pumpfun_amm_deposit_tx(&mut rng);
    let pumpfun_amm_withdraw_tx = create_pumpfun_amm_withdraw_tx(&mut rng);
    
    macro_rules! bench_event {
        ($name:expr, $tx:expr) => {
            group.bench_function($name, |b| {
                b.iter(|| {
                    let mut pumpfun_trade_event_rows: Vec<clickhouse_events::PumpfunTradeEventV2> = vec![];
                    let mut pumpfun_create_event_rows: Vec<clickhouse_events::PumpfunCreateEventV2> = vec![];
                    let mut pumpfun_migrate_event_rows: Vec<clickhouse_events::PumpfunMigrateEventV2> = vec![];
                    let mut pumpfun_amm_buy_event_rows: Vec<clickhouse_events::PumpfunAmmBuyEventV2> = vec![];
                    let mut pumpfun_amm_sell_event_rows: Vec<clickhouse_events::PumpfunAmmSellEventV2> = vec![];
                    let mut pumpfun_amm_create_pool_event_rows: Vec<clickhouse_events::PumpfunAmmCreatePoolEventV2> = vec![];
                    let mut pumpfun_amm_deposit_event_rows: Vec<clickhouse_events::PumpfunAmmDepositEventV2> = vec![];
                    let mut pumpfun_amm_withdraw_event_rows: Vec<clickhouse_events::PumpfunAmmWithdrawEventV2> = vec![];
                    
                    TransactionConverter::convert(
                        std::hint::black_box(&$tx),
                        &mut pumpfun_trade_event_rows,
                        &mut pumpfun_create_event_rows,
                        &mut pumpfun_migrate_event_rows,
                        &mut pumpfun_amm_buy_event_rows,
                        &mut pumpfun_amm_sell_event_rows,
                        &mut pumpfun_amm_create_pool_event_rows,
                        &mut pumpfun_amm_deposit_event_rows,
                        &mut pumpfun_amm_withdraw_event_rows,
                    );
                });
            });
        };
    }
    
    bench_event!("pumpfun_trade", pumpfun_trade_tx);
    bench_event!("pumpfun_create", pumpfun_create_tx);
    bench_event!("pumpfun_migrate", pumpfun_migrate_tx);
    bench_event!("pumpfun_amm_buy", pumpfun_amm_buy_tx);
    bench_event!("pumpfun_amm_sell", pumpfun_amm_sell_tx);
    bench_event!("pumpfun_amm_create_pool", pumpfun_amm_create_pool_tx);
    bench_event!("pumpfun_amm_deposit", pumpfun_amm_deposit_tx);
    bench_event!("pumpfun_amm_withdraw", pumpfun_amm_withdraw_tx);
    
    group.finish();
}

fn benchmark_batch_conversion(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_conversion");
    let mut rng = StdRng::seed_from_u64(SEED);
    
    // 混合交易：模拟真实场景
    let mixed_txs: Vec<Transaction> = (0..100).map(|i| {
        match i % 8 {
            0 => create_pumpfun_trade_tx(&mut rng),
            1 => create_pumpfun_create_tx(&mut rng),
            2 => create_pumpfun_migrate_tx(&mut rng),
            3 => create_pumpfun_amm_buy_tx(&mut rng),
            4 => create_pumpfun_amm_sell_tx(&mut rng),
            5 => create_pumpfun_amm_create_pool_tx(&mut rng),
            6 => create_pumpfun_amm_deposit_tx(&mut rng),
            _ => create_pumpfun_amm_withdraw_tx(&mut rng),
        }
    }).collect();
    
    group.bench_function("mixed_100_txs", |b| {
        b.iter(|| {
            let mut pumpfun_trade_event_rows: Vec<clickhouse_events::PumpfunTradeEventV2> = vec![];
            let mut pumpfun_create_event_rows: Vec<clickhouse_events::PumpfunCreateEventV2> = vec![];
            let mut pumpfun_migrate_event_rows: Vec<clickhouse_events::PumpfunMigrateEventV2> = vec![];
            let mut pumpfun_amm_buy_event_rows: Vec<clickhouse_events::PumpfunAmmBuyEventV2> = vec![];
            let mut pumpfun_amm_sell_event_rows: Vec<clickhouse_events::PumpfunAmmSellEventV2> = vec![];
            let mut pumpfun_amm_create_pool_event_rows: Vec<clickhouse_events::PumpfunAmmCreatePoolEventV2> = vec![];
            let mut pumpfun_amm_deposit_event_rows: Vec<clickhouse_events::PumpfunAmmDepositEventV2> = vec![];
            let mut pumpfun_amm_withdraw_event_rows: Vec<clickhouse_events::PumpfunAmmWithdrawEventV2> = vec![];
            
            for tx in std::hint::black_box(&mixed_txs) {
                TransactionConverter::convert(
                    tx,
                    &mut pumpfun_trade_event_rows,
                    &mut pumpfun_create_event_rows,
                    &mut pumpfun_migrate_event_rows,
                    &mut pumpfun_amm_buy_event_rows,
                    &mut pumpfun_amm_sell_event_rows,
                    &mut pumpfun_amm_create_pool_event_rows,
                    &mut pumpfun_amm_deposit_event_rows,
                    &mut pumpfun_amm_withdraw_event_rows,
                );
            }
        });
    });
    
    group.finish();
}

criterion_group!(benches, benchmark_individual_events, benchmark_batch_conversion);
criterion_main!(benches);
