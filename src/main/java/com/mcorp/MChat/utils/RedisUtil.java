package com.mcorp.MChat.utils;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;

@Component
public class RedisUtil {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    // 定义一个静态变量，供全局（包括非 Spring 管理的类）调用
    private static RedisTemplate<String, Object> staticRedisTemplate;

    @PostConstruct
    public void init() {
        // 在 Spring 初始化完成后，将 Bean 赋值给静态变量
        staticRedisTemplate = redisTemplate;
    }

    /**
     * 核心方法：提供给 ServerHandler 使用
     */
    public static RedisTemplate<String, Object> getRedis() {
        return staticRedisTemplate;
    }
}