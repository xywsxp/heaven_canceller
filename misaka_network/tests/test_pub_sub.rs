use misaka_network::{MisakaNetwork, TelepathConfig, MisakaSignal, AckPolicy};
use misaka_network::misaka_signal::AuthorityLevel;
use std::time::Duration;
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use tokio::time::sleep;
use uuid::Uuid;
use prost_types::Timestamp;

#[tokio::test]
async fn test_create_and_pub_sub() -> anyhow::Result<()> {
    // Connect to NATS
    let client = MisakaNetwork::new("nats://localhost:4222").await?;
    
    // Create a telepath
    let telepath_name = "test_demo";
    let config = TelepathConfig::default();
    client.create_telepath(telepath_name, config).await?;
    println!("✓ Created telepath: {}", telepath_name);
    
    // Spawn subscriber task
    let client_sub = MisakaNetwork::new("nats://localhost:4222").await?;
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();
    
    let sub_task = tokio::spawn(async move {
        println!("Starting subscriber...");
        let result = client_sub
            .subscribe_telepath("test_demo", AckPolicy::Explicit, move |signal| {
                let count = counter_clone.fetch_add(1, Ordering::SeqCst) + 1;
                println!("📨 Received signal #{}: content_type={}, payload={}", 
                    count, 
                    signal.content_type,
                    String::from_utf8_lossy(&signal.payload)
                );
                
                if count >= 3 {
                    // Return error to stop the subscription
                    return Err(anyhow::anyhow!("Received enough messages"));
                }
                Ok(())
            })
            .await;
        
        // This is expected - we error out to stop the subscription
        if let Err(e) = result {
            println!("Subscription stopped: {}", e);
        }
    });
    
    // Give subscriber time to start
    sleep(Duration::from_secs(1)).await;
    
    // Publish some test signals
    for i in 1..=3 {
        let content = format!("Test message {}", i);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap();
        
        let signal = MisakaSignal {
            timestamp: Some(Timestamp {
                seconds: now.as_secs() as i64,
                nanos: now.subsec_nanos() as i32,
            }),
            uuid: Uuid::new_v4().to_string(),
            parent_uuid: String::new(),
            sender_agent: "test_agent".to_string(),
            authority: AuthorityLevel::Lv0 as i32,
            content_type: "test.message".to_string(),
            payload: content.as_bytes().to_vec(),
        };
        
        client.emit_signal(telepath_name, signal).await?;
        println!("📤 Published signal #{}", i);
        sleep(Duration::from_millis(500)).await;
    }
    
    // Wait a bit for all messages to be processed
    sleep(Duration::from_secs(2)).await;
    
    let received_count = counter.load(Ordering::SeqCst);
    println!("✓ Subscriber received {} signals", received_count);
    
    // Wait for the subscriber task to finish (it should exit after receiving 3 messages)
    match tokio::time::timeout(Duration::from_secs(5), sub_task).await {
        Ok(_) => println!("✓ Subscriber task completed"),
        Err(_) => {
            println!("⚠ Subscriber task timeout - this is OK for this test");
        }
    }
    
    assert_eq!(received_count, 3, "Should have received 3 messages");
    
    println!("✅ Test completed successfully!");
    Ok(())
}
