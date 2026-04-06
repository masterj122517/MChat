package com.mcorp.MChat.server;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class BusinessThreadPool {
    private static final int CPU_COUNT = Runtime.getRuntime().availableProcessors();

  // 业务线程池，核心线程数 = CPU 核心数，最大线程数 = CPU 核心数 * 2，空闲线程存活时间 60 秒，使用有界队列和自定义线程工厂
  private static final ExecutorService EXECUTOR =
      new ThreadPoolExecutor(
          CPU_COUNT,
          CPU_COUNT * 2,
          60L,
          TimeUnit.SECONDS,
          new LinkedBlockingQueue<>(1000), // 队列容量 1000，避免过多请求时 OOM
          new ThreadFactory() {
              private final AtomicInteger count = new AtomicInteger(0);
              @Override
              public Thread newThread(Runnable r) {
                  return new Thread(r, "MChat-business-thread-" + count.getAndIncrement());
              }
          });

  public static void submit(Runnable task) {
      EXECUTOR.submit(task);
  }
}
