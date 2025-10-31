"""
Python client test for Misaka Network

Usage:
    # Terminal 1 - Run subscriber
    python tests/test_python_client.py sub
    
    # Terminal 2 - Run publisher
    python tests/test_python_client.py pub
"""
import asyncio
import sys
import time
import uuid

from google.protobuf.timestamp_pb2 import Timestamp

from misaka_network import MisakaNetwork, TelepathConfig, AckPolicy
from generated import misaka_signal_v2_pb2


async def test_publisher():
    """测试发布消息"""
    print("🚀 Starting publisher test...")
    
    network = MisakaNetwork("nats://localhost:4222")
    await network.connect()
    print("✅ Connected to NATS")
    
    try:
        # 创建 telepath
        config = TelepathConfig(
            ttl=300,
            max_msgs=10_000,
            max_bytes=-1
        )
        await network.create_telepath("test_python", config)
        print("✅ Created telepath: test_python")
        
        # 发布多条消息
        for i in range(1, 4):
            # 创建时间戳
            ts = Timestamp()
            ts.GetCurrentTime()
            
            # 创建 MisakaSignal
            signal = misaka_signal_v2_pb2.MisakaSignal(
                timestamp=ts,
                uuid=str(uuid.uuid4()),
                sender_agent=f"python_publisher_{i}",
                authority=misaka_signal_v2_pb2.MisakaSignal.AuthorityLevel.LV0,
                content_type="test.message",
                payload=f"Test message {i} from Python".encode('utf-8')
            )
            
            seq = await network.emit_signal("test_python", signal)
            print(f"📤 Published message #{i}, seq: {seq}, uuid: {signal.uuid}")
            
            await asyncio.sleep(0.5)
        
        print("✅ Publisher test completed!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise
    finally:
        await network.close()


async def test_subscriber():
    """测试订阅消息"""
    print("🚀 Starting subscriber test...")
    
    network = MisakaNetwork("nats://localhost:4222")
    await network.connect()
    print("✅ Connected to NATS")
    
    message_count = 0
    max_messages = 3
    
    async def handle_message(signal: misaka_signal_v2_pb2.MisakaSignal):
        nonlocal message_count
        message_count += 1
        
        # 解码消息
        message = signal.payload.decode('utf-8')
        print(f"📨 Received message #{message_count}:")
        print(f"   UUID: {signal.uuid}")
        print(f"   Sender: {signal.sender_agent}")
        print(f"   Content Type: {signal.content_type}")
        print(f"   Payload: {message}")
        
        # 收到 3 条消息后退出
        if message_count >= max_messages:
            print(f"✅ Received {message_count} messages, exiting...")
            raise KeyboardInterrupt("Test complete")
    
    try:
        print("📡 Subscribing to telepath: test_python")
        await network.subscribe_telepath(
            "test_python",
            handle_message,
            ack_policy=AckPolicy.EXPLICIT
        )
        
        # 保持运行直到收到足够消息
        await asyncio.Event().wait()
        
    except KeyboardInterrupt:
        print("✅ Subscriber test completed!")
    except Exception as e:
        print(f"❌ Error: {e}")
        raise
    finally:
        await network.close()


async def test_full():
    """完整测试：创建、发布、订阅"""
    print("🚀 Starting full integration test...")
    
    network = MisakaNetwork("nats://localhost:4222")
    await network.connect()
    print("✅ Connected to NATS")
    
    try:
        # 1. 创建 telepath
        config = TelepathConfig()
        telepath_name = "test_full_python"
        await network.create_telepath(telepath_name, config)
        print(f"✅ Created telepath: {telepath_name}")
        
        # 2. 启动订阅者任务
        messages_received = []
        
        async def handle_message(signal: misaka_signal_v2_pb2.MisakaSignal):
            message = signal.payload.decode('utf-8')
            messages_received.append(message)
            print(f"📨 Received: {message} (uuid: {signal.uuid})")
            
            # 收到 3 条后退出
            if len(messages_received) >= 3:
                raise KeyboardInterrupt("Test complete")
        
        # 创建订阅者网络连接
        subscriber_network = MisakaNetwork("nats://localhost:4222")
        await subscriber_network.connect()
        
        # 启动订阅任务
        async def subscriber_task():
            try:
                await subscriber_network.subscribe_telepath(
                    telepath_name,
                    handle_message,
                    ack_policy=AckPolicy.EXPLICIT
                )
                await asyncio.Event().wait()
            except KeyboardInterrupt:
                pass
            finally:
                await subscriber_network.close()
        
        sub_task = asyncio.create_task(subscriber_task())
        
        # 等待订阅者启动
        await asyncio.sleep(1)
        
        # 3. 发布消息
        print("\n📤 Publishing messages...")
        for i in range(1, 4):
            # 创建时间戳
            ts = Timestamp()
            ts.GetCurrentTime()
            
            signal = misaka_signal_v2_pb2.MisakaSignal(
                timestamp=ts,
                uuid=str(uuid.uuid4()),
                sender_agent=f"python_test_full_{i}",
                authority=misaka_signal_v2_pb2.MisakaSignal.AuthorityLevel.LV0,
                content_type="test.integration",
                payload=f"Integration test message {i}".encode('utf-8')
            )
            
            seq = await network.emit_signal(telepath_name, signal)
            print(f"📤 Published message #{i}, seq: {seq}")
            
            await asyncio.sleep(0.5)
        
        # 等待订阅者处理完消息
        await asyncio.sleep(2)
        
        # 取消订阅任务
        sub_task.cancel()
        try:
            await sub_task
        except asyncio.CancelledError:
            pass
        
        # 4. 验证结果
        print(f"\n✅ Test completed! Received {len(messages_received)} messages")
        for i, msg in enumerate(messages_received, 1):
            print(f"  {i}. {msg}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise
    finally:
        await network.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python test_python_client.py pub   # Run publisher")
        print("  python test_python_client.py sub   # Run subscriber")
        print("  python test_python_client.py full  # Run full integration test")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == "pub":
        asyncio.run(test_publisher())
    elif command == "sub":
        asyncio.run(test_subscriber())
    elif command == "full":
        asyncio.run(test_full())
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
