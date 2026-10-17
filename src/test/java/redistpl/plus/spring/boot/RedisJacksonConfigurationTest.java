package redistpl.plus.spring.boot;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;

class RedisJacksonConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(RedisJacksonConfiguration.class);

    @Test
    void serializerRoundTripsRedisValuesWithoutAConnection() {
        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(Jackson2JsonRedisSerializer.class);
            @SuppressWarnings("unchecked")
            Jackson2JsonRedisSerializer<Object> serializer = context.getBean(Jackson2JsonRedisSerializer.class);
            byte[] encoded = serializer.serialize(Map.of("name", "ddd4j"));

            assertThat(encoded).isNotEmpty();
            assertThat(serializer.deserialize(encoded)).isEqualTo(Map.of("name", "ddd4j"));
        });
    }
}
