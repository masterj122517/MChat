# MChat Project Roadmap

    Goal: Build a Netty-based 10k+ concurrent connection instant messaging platform, mastering high-performance network programming.

# Phase 1: Core Skeleton & Custom Protocol (COMPLETE)

Core task: Get data running correctly and efficiently over the wire.

    [x] Environment setup: Spring Boot 3.x + Netty 4.1.x + Protobuf.

    [x] Custom protocol design:

        Magic Number (0x4D434854) to prevent illegal packet attacks.

        LengthFieldBasedFrameDecoder to fully solve TCP sticky/half-packet.

    [x] Codec implementation: Hand-written MessageEncoder and MessageDecoder.

## Phase 2: Long Connection Management & Stability (COMPLETE)

Core task: Handle massive connections, ensure server doesn't crash or drop connections.

    [x] Heartbeat detection: IdleStateHandler implementation, proactively clean "dead connections".

    [x] Connection pool management: ConcurrentHashMap maintaining UserId -> Channel mapping.

    [x] Graceful shutdown: Handle Netty resource release on JVM shutdown.

## Phase 3: High-Performance Message Dispatch (COMPLETE)

Core task: Use multi-threading model so message processing doesn't block I/O.

    [x] Business thread pool isolation: Offload blocking business logic from Netty's EventLoop.

    [x] P2P/Group chat logic: Protobuf-based command set with full routing.

        - P2P chat with target lookup and offline fallback
        - Group chat with create/join/leave/broadcast operations
        - Group membership tracking across reconnections

    [x] Message reliability: ACK acknowledgement mechanism (prevents message loss).

        - Server sends ACK to sender on delivery/storage
        - Client sends ACK to server on receipt
        - Unique messageId tracking per message

    [x] Login response: Server confirms login success/failure to client.

    [x] Offline message delivery: Redis-backed offline queue, flushed on login.

## Phase 4: Distributed Extension & Monitoring (COMPLETE)

Core task: Single server isn't enough, scale horizontally.

    [x] Redis routing layer: Cross-server communication (User A on Server1, User B on Server2).

        - Redis Pub/Sub for real-time cross-server message routing
        - Redis key-based online status tracking per user
        - Automatic fallback: local -> remote -> offline storage

    [x] Performance monitoring: ByteBuf leak detection (Netty ResourceLeakDetector ADVANCED level).

        - Scheduled metrics reporting every 30 seconds
        - Heap/non-heap/direct memory tracking
        - Message throughput (in/out) rate calculation
        - Active connection count tracking

    [ ] Load testing: Use JMeter or custom scripts to simulate 10k+ concurrent connections.

## Development Principles

    KISS (Keep It Simple, Stupid): If NIO can solve it, don't write complex business logic.

    Zero-Copy awareness: Use Netty's CompositeByteBuf to reduce memory copies where possible.

    No CRUD: All storage prioritizes in-memory or Redis, no database tables for now.
