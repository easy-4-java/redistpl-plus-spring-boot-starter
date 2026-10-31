package redistpl.plus.spring.boot;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.Future;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

class RedisCachingConfigurationIntegrationTest {

    @Test
    void configuredExecutorRunsTasksOnNamedDaemonThreadsAndShutsDown() throws Exception {
        RedisThreadPoolProperties.Pool pool = new RedisThreadPoolProperties.Pool();
        pool.setCoreSize(1);
        pool.setMaxSize(2);
        pool.setQueueCapacity(3);
        pool.setKeepAlive(Duration.ofSeconds(5));
        pool.setThreadNamePrefix("isolated-redis-test-");
        pool.setDaemon(true);

        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        try {
            ThreadPoolTaskExecutor executor = new RedisCachingConfiguration()
                    .redisThreadPoolTaskExecutor(pool, registry, "test.redis.executor");
            try {
                Future<String> name = executor.submit(() -> Thread.currentThread().getName());
                Future<Boolean> daemon = executor.submit(() -> Thread.currentThread().isDaemon());
                assertThat(name.get()).startsWith("isolated-redis-test-");
                assertThat(daemon.get()).isTrue();
                assertThat(executor.getCorePoolSize()).isEqualTo(1);
                assertThat(executor.getMaxPoolSize()).isEqualTo(2);
            } finally {
                executor.shutdown();
            }
        } finally {
            registry.close();
        }
    }
}
