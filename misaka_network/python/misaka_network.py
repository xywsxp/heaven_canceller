"""
Misaka Network Python Client

简洁的 NATS JetStream 客户端，用于发布和订阅 MisakaSignal
"""
from typing import Callable, Optional
from datetime import timedelta
from enum import Enum
from dataclasses import dataclass

import nats
from nats.js.api import StreamConfig, RetentionPolicy, StorageType, DiscardPolicy
from nats.js import JetStreamContext

from generated import misaka_signal_v2_pb2


class AckPolicy(Enum):
    """消息确认策略"""
    EXPLICIT = "explicit"
    NONE = "none"
    ALL = "all"


@dataclass
class TelepathConfig:
    """Configuration for creating a telepath (stream)"""
    ttl: int = 300  # seconds
    max_msgs: int = 10_000
    max_bytes: int = -1  # No limit


class MisakaNetwork:
    """NATS-based Misaka Network Client"""
    
    def __init__(self, nats_url: str):
        self.nats_url = nats_url
        self.nc: Optional[nats.NATS] = None
        self.js: Optional[JetStreamContext] = None
    
    async def connect(self):
        """连接到 NATS"""
        self.nc = await nats.connect(self.nats_url)
        self.js = self.nc.jetstream()
    
    async def close(self):
        """关闭连接"""
        if self.nc:
            await self.nc.close()
    
    async def create_telepath(
        self,
        telepath_name: str,
        config: Optional[TelepathConfig] = None
    ):
        """创建一个新的 Telepath (JetStream Stream)"""
        if config is None:
            config = TelepathConfig()
        
        stream_name = f"telepath_{telepath_name}"
        
        stream_config = StreamConfig(
            name=stream_name,
            subjects=[f"{stream_name}.>"],
            max_age=config.ttl,  # nats-py expects seconds as int or float
            max_msgs=config.max_msgs,
            max_bytes=config.max_bytes,
            storage=StorageType.MEMORY,
            retention=RetentionPolicy.LIMITS,
            discard=DiscardPolicy.OLD,
            num_replicas=1,
        )
        
        await self.js.add_stream(config=stream_config)
    
    async def emit_signal(
        self,
        telepath_name: str,
        signal: misaka_signal_v2_pb2.MisakaSignal
    ) -> str:
        """
        发布 Signal 到指定 Telepath
        
        Args:
            telepath_name: Telepath 名称
            signal: MisakaSignal protobuf 消息
        
        Returns:
            消息序列号
        """
        stream_name = f"telepath_{telepath_name}"
        # 根据 signal 的 authority 字段发到对应的 subject
        authority_level = signal.authority
        subject = f"{stream_name}.lv{authority_level}"
        
        # 序列化 protobuf
        signal_bytes = signal.SerializeToString()
        
        ack = await self.js.publish(subject, signal_bytes)
        return str(ack.seq)
    
    async def subscribe_telepath(
        self,
        telepath_name: str,
        handler: Callable[[misaka_signal_v2_pb2.MisakaSignal], None],
        ack_policy: AckPolicy = AckPolicy.EXPLICIT
    ):
        """
        订阅 Telepath (Push 模式 - 实时推送)
        
        Args:
            telepath_name: Telepath 名称
            handler: 消息处理回调，接收 MisakaSignal protobuf 消息
            ack_policy: 消息确认策略
        """
        stream_name = f"telepath_{telepath_name}"
        consumer_name = f"{telepath_name}_consumer"
        
        async def message_handler(msg):
            try:
                # 反序列化 protobuf
                signal = misaka_signal_v2_pb2.MisakaSignal()
                signal.ParseFromString(msg.data)
                
                # 调用用户的处理函数
                await handler(signal)
                
                # 根据策略确认消息
                if ack_policy == AckPolicy.EXPLICIT:
                    await msg.ack()
            except Exception as e:
                print(f"Handler error: {e}")
        
        # 订阅
        await self.js.subscribe(
            subject=f"{stream_name}.>",
            cb=message_handler,
            durable=consumer_name,
            stream=stream_name,
        )
