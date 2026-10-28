package redistpl.plus.spring.boot;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.GeoTemplate;
import org.springframework.data.redis.core.RedisOperationTemplate;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class RedisCachingConfigurationContextTest {

    @Test
    void boot34CreatesRedisHelpersWithSuppliedConnectionFactory() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(RedisCachingConfiguration.class))
                .withBean(RedisConnectionFactory.class, () -> mock(RedisConnectionFactory.class))
                .withBean(RedisMessageListenerContainer.class,
                        () -> mock(RedisMessageListenerContainer.class))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(GeoTemplate.class);
                    assertThat(context).hasSingleBean(RedisOperationTemplate.class);
                });
    }
}
