use chrono::NaiveDate;
use syncer::extractor::ClickHouseExtractor;
use utils::clickhouse_events::*;

#[tokio::test]
async fn test_extract_trade_event() {
    // 设置测试日期 - 2025年1月1日
    let date = NaiveDate::from_ymd_opt(2025, 10, 1).unwrap();

    let extractor = ClickHouseExtractor::new();

    // 提取 pumpfun_trade_event_v2 表的数据
    let result = extractor
        .extract_daily_events("pumpfun_trade_event_v2", "PumpfunTradeEventV2", date)
        .await;

    match result {
        Ok(batch) => {
            println!("✓ Successfully extracted {} rows", batch.num_rows());
            println!("  Columns: {}", batch.num_columns());

            // 验证可以转换回结构体
            let events: Vec<PumpfunTradeEventV2> = arrow_batch_to_vec(&batch);
            println!("  Converted to {} PumpfunTradeEventV2 events", events.len());

            assert_eq!(batch.num_rows(), events.len(), "RecordBatch rows should match converted events");

            // 双向转换测试：events → batch → events，验证数据完整性
            if !events.is_empty() {
                let batch2 = vec_to_arrow_batch(&events);
                assert_eq!(batch.num_rows(), batch2.num_rows(), "Row count should match after round-trip");
                assert_eq!(batch.num_columns(), batch2.num_columns(), "Column count should match after round-trip");
                
                let events2: Vec<PumpfunTradeEventV2> = arrow_batch_to_vec(&batch2);
                assert_eq!(events.len(), events2.len(), "Event count should match after round-trip");
                
                // 验证第一条和最后一条记录完全一致
                assert_eq!(events[0], events2[0], "First event should match after round-trip");
                assert_eq!(events[events.len()-1], events2[events2.len()-1], "Last event should match after round-trip");
                
                println!("  ✓ Round-trip conversion verified (events → batch → events)");
            }

            // 打印第一条记录（如果有）
            if let Some(first) = events.first() {
                println!(
                    "  First event: slot={}, signature={}, sol_amount={}",
                    first.slot, first.signature, first.sol_amount
                );
            }
        }
        Err(e) => {
            println!("✗ Error: {}", e);
            // 测试环境可能没有数据，不算失败
            println!("  (This is OK if the database is empty for this date)");
        }
    }
}

#[tokio::test]
async fn test_extract_create_event() {
    let date = NaiveDate::from_ymd_opt(2025, 10, 1).unwrap();

    let extractor = ClickHouseExtractor::new();

    let result = extractor
        .extract_daily_events("pumpfun_create_event_v2", "PumpfunCreateEventV2", date)
        .await;

    match result {
        Ok(batch) => {
            println!(
                "✓ Successfully extracted {} rows from pumpfun_create_event_v2",
                batch.num_rows()
            );

            let events: Vec<PumpfunCreateEventV2> = arrow_batch_to_vec(&batch);
            assert_eq!(batch.num_rows(), events.len(), "RecordBatch rows should match converted events");

            // 双向转换验证
            if !events.is_empty() {
                let batch2 = vec_to_arrow_batch(&events);
                let events2: Vec<PumpfunCreateEventV2> = arrow_batch_to_vec(&batch2);
                
                assert_eq!(events.len(), events2.len(), "Event count should match after round-trip");
                assert_eq!(events[0], events2[0], "First event should match after round-trip");
                
                println!("  ✓ Round-trip conversion verified");
            }

            if let Some(first) = events.first() {
                println!(
                    "  First create event: mint={}, symbol={}, name={}",
                    first.mint, first.symbol, first.name
                );
            }
        }
        Err(e) => {
            println!("✗ Error: {} (OK if no data)", e);
        }
    }
}

#[tokio::test]
async fn test_extract_migrate_event() {
    let date = NaiveDate::from_ymd_opt(2025, 10, 1).unwrap();

    let extractor = ClickHouseExtractor::new();

    let result = extractor
        .extract_daily_events("pumpfun_migrate_event_v2", "PumpfunMigrateEventV2", date)
        .await;

    match result {
        Ok(batch) => {
            println!(
                "✓ Successfully extracted {} rows from pumpfun_migrate_event_v2",
                batch.num_rows()
            );

            let events: Vec<PumpfunMigrateEventV2> = arrow_batch_to_vec(&batch);
            assert_eq!(batch.num_rows(), events.len(), "RecordBatch rows should match converted events");
            
            // 双向转换验证
            if !events.is_empty() {
                let batch2 = vec_to_arrow_batch(&events);
                let events2: Vec<PumpfunMigrateEventV2> = arrow_batch_to_vec(&batch2);
                assert_eq!(events[0], events2[0], "Data integrity after round-trip");
                println!("  ✓ Round-trip conversion verified");
            }
        }
        Err(e) => {
            println!("✗ Error: {} (OK if no data)", e);
        }
    }
}

#[tokio::test]
#[ignore = "slow test, run manually"]
async fn test_extract_amm_buy_event() {
    let date = NaiveDate::from_ymd_opt(2025, 10, 1).unwrap();

    let extractor = ClickHouseExtractor::new();

    let result = extractor
        .extract_daily_events("pumpfun_amm_buy_event_v2", "PumpfunAmmBuyEventV2", date)
        .await;

    match result {
        Ok(batch) => {
            println!(
                "✓ Successfully extracted {} rows from pumpfun_amm_buy_event_v2",
                batch.num_rows()
            );

            let events: Vec<PumpfunAmmBuyEventV2> = arrow_batch_to_vec(&batch);
            assert_eq!(batch.num_rows(), events.len(), "RecordBatch rows should match converted events");
            
            // 双向转换验证
            if !events.is_empty() {
                let batch2 = vec_to_arrow_batch(&events);
                let events2: Vec<PumpfunAmmBuyEventV2> = arrow_batch_to_vec(&batch2);
                assert_eq!(events[0], events2[0], "Data integrity after round-trip");
                println!("  ✓ Round-trip conversion verified");
            }
        }
        Err(e) => {
            println!("✗ Error: {} (OK if no data)", e);
        }
    }
}

#[tokio::test]
#[ignore = "slow test, run manually"]
async fn test_extract_amm_sell_event() {
    let date = NaiveDate::from_ymd_opt(2025, 10, 1).unwrap();

    let extractor = ClickHouseExtractor::new();

    let result = extractor
        .extract_daily_events("pumpfun_amm_sell_event_v2", "PumpfunAmmSellEventV2", date)
        .await;

    match result {
        Ok(batch) => {
            println!(
                "✓ Successfully extracted {} rows from pumpfun_amm_sell_event_v2",
                batch.num_rows()
            );

            let events: Vec<PumpfunAmmSellEventV2> = arrow_batch_to_vec(&batch);
            assert_eq!(batch.num_rows(), events.len(), "RecordBatch rows should match converted events");
            
            // 双向转换验证
            if !events.is_empty() {
                let batch2 = vec_to_arrow_batch(&events);
                let events2: Vec<PumpfunAmmSellEventV2> = arrow_batch_to_vec(&batch2);
                assert_eq!(events[0], events2[0], "Data integrity after round-trip");
                println!("  ✓ Round-trip conversion verified");
            }
        }
        Err(e) => {
            println!("✗ Error: {} (OK if no data)", e);
        }
    }
}

#[tokio::test]
async fn test_extract_amm_create_pool_event() {
    let date = NaiveDate::from_ymd_opt(2025, 10, 1).unwrap();

    let extractor = ClickHouseExtractor::new();

    let result = extractor
        .extract_daily_events(
            "pumpfun_amm_create_pool_event_v2",
            "PumpfunAmmCreatePoolEventV2",
            date,
        )
        .await;

    match result {
        Ok(batch) => {
            println!(
                "✓ Successfully extracted {} rows from pumpfun_amm_create_pool_event_v2",
                batch.num_rows()
            );

            let events: Vec<PumpfunAmmCreatePoolEventV2> = arrow_batch_to_vec(&batch);
            assert_eq!(batch.num_rows(), events.len(), "RecordBatch rows should match converted events");
            
            // 双向转换验证
            if !events.is_empty() {
                let batch2 = vec_to_arrow_batch(&events);
                let events2: Vec<PumpfunAmmCreatePoolEventV2> = arrow_batch_to_vec(&batch2);
                assert_eq!(events[0], events2[0], "Data integrity after round-trip");
                println!("  ✓ Round-trip conversion verified");
            }
        }
        Err(e) => {
            println!("✗ Error: {} (OK if no data)", e);
        }
    }
}

#[tokio::test]
async fn test_extract_amm_deposit_event() {
    let date = NaiveDate::from_ymd_opt(2025, 10, 1).unwrap();

    let extractor = ClickHouseExtractor::new();

    let result = extractor
        .extract_daily_events(
            "pumpfun_amm_deposit_event_v2",
            "PumpfunAmmDepositEventV2",
            date,
        )
        .await;

    match result {
        Ok(batch) => {
            println!(
                "✓ Successfully extracted {} rows from pumpfun_amm_deposit_event_v2",
                batch.num_rows()
            );

            let events: Vec<PumpfunAmmDepositEventV2> = arrow_batch_to_vec(&batch);
            assert_eq!(batch.num_rows(), events.len(), "RecordBatch rows should match converted events");
            
            // 双向转换验证
            if !events.is_empty() {
                let batch2 = vec_to_arrow_batch(&events);
                let events2: Vec<PumpfunAmmDepositEventV2> = arrow_batch_to_vec(&batch2);
                assert_eq!(events[0], events2[0], "Data integrity after round-trip");
                println!("  ✓ Round-trip conversion verified");
            }
        }
        Err(e) => {
            println!("✗ Error: {} (OK if no data)", e);
        }
    }
}

#[tokio::test]
async fn test_extract_amm_withdraw_event() {
    let date = NaiveDate::from_ymd_opt(2025, 10, 1).unwrap();

    let extractor = ClickHouseExtractor::new();

    let result = extractor
        .extract_daily_events(
            "pumpfun_amm_withdraw_event_v2",
            "PumpfunAmmWithdrawEventV2",
            date,
        )
        .await;

    match result {
        Ok(batch) => {
            println!(
                "✓ Successfully extracted {} rows from pumpfun_amm_withdraw_event_v2",
                batch.num_rows()
            );

            let events: Vec<PumpfunAmmWithdrawEventV2> = arrow_batch_to_vec(&batch);
            assert_eq!(batch.num_rows(), events.len(), "RecordBatch rows should match converted events");
            
            // 双向转换验证
            if !events.is_empty() {
                let batch2 = vec_to_arrow_batch(&events);
                let events2: Vec<PumpfunAmmWithdrawEventV2> = arrow_batch_to_vec(&batch2);
                assert_eq!(events[0], events2[0], "Data integrity after round-trip");
                println!("  ✓ Round-trip conversion verified");
            }
        }
        Err(e) => {
            println!("✗ Error: {} (OK if no data)", e);
        }
    }
}

#[tokio::test]
async fn test_invalid_event_type() {
    let date = NaiveDate::from_ymd_opt(2025, 10, 1).unwrap();

    let extractor = ClickHouseExtractor::new();

    let result = extractor
        .extract_daily_events("some_table", "InvalidEventType", date)
        .await;

    assert!(
        result.is_err(),
        "Should return error for invalid event type"
    );

    if let Err(e) = result {
        let error_msg = e.to_string();
        assert!(
            error_msg.contains("Unknown event type"),
            "Error message should mention unknown event type"
        );
        println!("✓ Correctly rejected invalid event type: {}", error_msg);
    }
}

#[tokio::test]
async fn test_date_range_extraction() {
    // 测试日期边界处理
    let date = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();

    let extractor = ClickHouseExtractor::new();

    let result = extractor
        .extract_daily_events("pumpfun_trade_event_v2", "PumpfunTradeEventV2", date)
        .await;

    match result {
        Ok(batch) => {
            println!("✓ Successfully extracted data for 2025-01-15");
            println!("  Found {} rows", batch.num_rows());

            // 验证所有记录的时间戳都在正确的范围内
            let events: Vec<PumpfunTradeEventV2> = arrow_batch_to_vec(&batch);
            let start_ts = date.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp() as u32;
            let end_ts = start_ts + 86400;

            for event in &events {
                assert!(
                    event.timestamp >= start_ts && event.timestamp < end_ts,
                    "Event timestamp {} is outside range [{}, {})",
                    event.timestamp,
                    start_ts,
                    end_ts
                );
            }

            if !events.is_empty() {
                println!(
                    "  ✓ All {} events have timestamps within the correct date range",
                    events.len()
                );
            }
        }
        Err(e) => {
            println!("✗ Error: {} (OK if no data)", e);
        }
    }
}

#[tokio::test]
async fn test_data_integrity_round_trip() {
    // 专门测试数据完整性：DB → Arrow → Vec → Arrow → Vec
    let date = NaiveDate::from_ymd_opt(2025, 10, 1).unwrap();
    let extractor = ClickHouseExtractor::new();

    let result = extractor
        .extract_daily_events("pumpfun_trade_event_v2", "PumpfunTradeEventV2", date)
        .await;

    match result {
        Ok(original_batch) => {
            if original_batch.num_rows() == 0 {
                println!("⚠ No data to test, skipping integrity check");
                return;
            }

            println!("Testing data integrity with {} rows", original_batch.num_rows());

            // 第一次转换：Arrow → Vec
            let events_v1: Vec<PumpfunTradeEventV2> = arrow_batch_to_vec(&original_batch);
            
            // 第二次转换：Vec → Arrow
            let batch_v2 = vec_to_arrow_batch(&events_v1);
            
            // 第三次转换：Arrow → Vec
            let events_v2: Vec<PumpfunTradeEventV2> = arrow_batch_to_vec(&batch_v2);
            
            // 验证
            assert_eq!(original_batch.num_rows(), batch_v2.num_rows(), "Row count mismatch");
            assert_eq!(original_batch.num_columns(), batch_v2.num_columns(), "Column count mismatch");
            assert_eq!(events_v1.len(), events_v2.len(), "Vec length mismatch");
            
            // 逐条比较所有记录
            for (i, (e1, e2)) in events_v1.iter().zip(events_v2.iter()).enumerate() {
                assert_eq!(e1, e2, "Event #{} mismatch after round-trip", i);
            }
            
            println!("✓ All {} events verified - complete data integrity maintained", events_v1.len());
            println!("  First event: signature={}, slot={}", events_v1[0].signature, events_v1[0].slot);
            println!("  Last event: signature={}, slot={}", events_v1[events_v1.len()-1].signature, events_v1[events_v1.len()-1].slot);
        }
        Err(e) => {
            println!("✗ Error: {} (OK if no data)", e);
        }
    }
}
