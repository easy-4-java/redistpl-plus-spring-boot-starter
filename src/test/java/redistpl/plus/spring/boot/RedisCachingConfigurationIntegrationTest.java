package redistpl.plus.spring.boot;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.Future;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.MessageListenerAdapter;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.StreamMessageErrorHandler;
import org.springframework.data.redis.stream.StreamListenerAdapter;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RedisCachingConfigurationIntegrationTest {

    private static class TrackedConfiguration extends RedisCachingConfiguration {
        private final List<ThreadPoolTaskExecutor> executors = new ArrayList<>();

        @Override
        protected ThreadPoolTaskExecutor redisThreadPoolTaskExecutor(
                RedisThreadPoolProperties.Pool pool, io.micrometer.core.instrument.MeterRegistry registry,
                String prefix) {
            ThreadPoolTaskExecutor executor = super.redisThreadPoolTaskExecutor(pool, registry, prefix);
            executors.add(executor);
            return executor;
        }

        void shutdownExecutors() {
            executors.forEach(ThreadPoolTaskExecutor::shutdown);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    void containerFactoriesConfigureEmptyListenersWithoutStartingSubscriptions() throws Exception {
        TrackedConfiguration configuration = new TrackedConfiguration();
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ObjectProvider<RedisConnectionFactory> factories = mock(ObjectProvider.class);
        ObjectProvider<MessageListenerAdapter> messages = mock(ObjectProvider.class);
        ObjectProvider<StreamListenerAdapter> streams = mock(ObjectProvider.class);
        ObjectProvider<StreamMessageErrorHandler> errors = mock(ObjectProvider.class);
        ObjectProvider<StringRedisTemplate> stringTemplates = mock(ObjectProvider.class);
        ObjectProvider<io.micrometer.core.instrument.MeterRegistry> registries = mock(ObjectProvider.class);
        when(factories.getIfAvailable()).thenReturn(mock(RedisConnectionFactory.class));
        when(registries.getIfAvailable()).thenReturn(registry);
        when(messages.orderedStream()).thenReturn(Stream.empty());
        when(streams.orderedStream()).thenReturn(Stream.empty());
        when(errors.orderedStream()).thenReturn(Stream.empty());
        RedisThreadPoolProperties pools = new RedisThreadPoolProperties();
        try {
            RedisMessageListenerContainer messageContainer = configuration.redisMessageListenerContainer(
                    factories, messages, registries, pools);
            assertThat(messageContainer).isNotNull();
            StreamMessageListenerContainer<?, ?> streamContainer = configuration.streamMessageListenerContainer(
                    stringTemplates, factories, streams, errors, registries, pools);
            assertThat(streamContainer).isNotNull();
            assertThat(configuration.executors).hasSize(3);
        } finally {
            configuration.shutdownExecutors();
            registry.close();
        }
    }

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
