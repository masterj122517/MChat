package com.mcorp.MChat;

import com.mcorp.MChat.server.NettyServer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class, HibernateJpaAutoConfiguration.class})
public class MChatApplication implements CommandLineRunner {

    @Autowired
    private NettyServer nettyServer;

    public static void main(String[] args) {
        // 启动 Spring Boot
        SpringApplication.run(MChatApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        // Spring 初始化完成后，启动 NettyServer
        // 注意：要在新线程启动，否则会阻塞 Spring 的主线程导致启动不完整
        new Thread(() -> nettyServer.start()).start();
    }
}