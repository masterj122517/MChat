package com.mcorp.MChat.server;

import io.netty.util.ResourceLeakDetector;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Monitors key metrics for the MChat server:
 * - Netty ByteBuf leak detection
 * - Memory usage (heap / direct)
 * - Connection counts
 * - Message throughput
 */
@Slf4j
@Component
@EnableScheduling
public class MetricsMonitor {

    // Counters for throughput tracking
    private static final AtomicLong TOTAL_MESSAGES_IN = new AtomicLong(0);
    private static final AtomicLong TOTAL_MESSAGES_OUT = new AtomicLong(0);
    private static final AtomicLong TOTAL_CONNECTIONS = new AtomicLong(0);

    // Snapshot for rate calculation
    private long lastMessagesIn = 0;
    private long lastMessagesOut = 0;
    private long lastCheckTime = System.currentTimeMillis();

    @PostConstruct
    public void init() {
        // Enable Netty ByteBuf leak detection
        // PARANOID: logs every allocation/deallocation (heavy, for dev/test)
        // ADVANCED: samples allocation (good for staging)
        // SIMPLE: samples at a lower rate (good for production)
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.ADVANCED);
        log.info("Netty ResourceLeakDetector set to ADVANCED level");
        log.info("MetricsMonitor initialized. Metrics will be logged every 30 seconds.");
    }

    /**
     * Log metrics every 30 seconds
     */
    @Scheduled(fixedRate = 30000)
    public void reportMetrics() {
        long now = System.currentTimeMillis();
        long elapsedSeconds = Math.max(1, (now - lastCheckTime) / 1000);

        long currentIn = TOTAL_MESSAGES_IN.get();
        long currentOut = TOTAL_MESSAGES_OUT.get();

        long inRate = (currentIn - lastMessagesIn) / elapsedSeconds;
        long outRate = (currentOut - lastMessagesOut) / elapsedSeconds;

        lastMessagesIn = currentIn;
        lastMessagesOut = currentOut;
        lastCheckTime = now;

        // Memory stats
        MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
        MemoryUsage nonHeapUsage = memoryBean.getNonHeapMemoryUsage();

        // Direct memory (Netty ByteBuf uses direct memory heavily)
        long directMemory = io.netty.util.internal.PlatformDependent.usedDirectMemory();

        log.info("=== MChat Metrics ===");
        log.info("  Active connections: {}", TOTAL_CONNECTIONS.get());
        log.info("  Messages IN:  total={}, rate={}/s", currentIn, inRate);
        log.info("  Messages OUT: total={}, rate={}/s", currentOut, outRate);
        log.info("  Heap memory:   used={}MB / max={}MB",
                heapUsage.getUsed() / (1024 * 1024),
                heapUsage.getMax() / (1024 * 1024));
        log.info("  Non-heap memory: used={}MB",
                nonHeapUsage.getUsed() / (1024 * 1024));
        log.info("  Direct memory (Netty): {}MB", directMemory / (1024 * 1024));
        log.info("=====================");
    }

    // --- Static increment methods for use throughout the application ---

    public static void incrementMessagesIn() {
        TOTAL_MESSAGES_IN.incrementAndGet();
    }

    public static void incrementMessagesOut() {
        TOTAL_MESSAGES_OUT.incrementAndGet();
    }

    public static void incrementConnections() {
        TOTAL_CONNECTIONS.incrementAndGet();
    }

    public static void decrementConnections() {
        TOTAL_CONNECTIONS.decrementAndGet();
    }

    public static long getActiveConnections() {
        return TOTAL_CONNECTIONS.get();
    }

    public static long getTotalMessagesIn() {
        return TOTAL_MESSAGES_IN.get();
    }

    public static long getTotalMessagesOut() {
        return TOTAL_MESSAGES_OUT.get();
    }
}
