package org.springframework.data.redis.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/** Exercises public operations against a Redis process owned by this test class. */
class RedisLiveIntegrationTest {
    private static LocalRedisFixture redis;
    private static LettuceConnectionFactory connection;
    private static RedisOperationTemplate sync;
    private static ReactiveRedisOperationTemplate reactive;

    @BeforeAll
    static void start() throws Exception {
        redis = new LocalRedisFixture();
        try {
            connection = new LettuceConnectionFactory("127.0.0.1", redis.port);
            connection.afterPropertiesSet();
            StringRedisSerializer strings = new StringRedisSerializer();
            RedisTemplate<String, Object> template = new RedisTemplate<>();
            template.setConnectionFactory(connection);
            template.setKeySerializer(strings);
            template.setValueSerializer(strings);
            template.setHashKeySerializer(strings);
            template.setHashValueSerializer(strings);
            template.afterPropertiesSet();
            sync = new RedisOperationTemplate(template, new ObjectMapper());

            @SuppressWarnings("unchecked")
            RedisSerializer<Object> values = (RedisSerializer<Object>) (RedisSerializer<?>) strings;
            RedisSerializationContext<String, Object> context = RedisSerializationContext
                    .<String, Object>newSerializationContext(strings)
                    .value(values).hashKey(values).hashValue(values).build();
            reactive = new ReactiveRedisOperationTemplate(
                    new ReactiveRedisTemplate<>(connection, context), new ObjectMapper());
        } catch (Exception failure) {
            stop();
            throw failure;
        }
    }

    @AfterAll
    static void stop() throws Exception {
        if (connection != null) connection.destroy();
        if (redis != null) redis.close();
    }

    private String key(String suffix) {
        return "redistpl:integration:" + UUID.randomUUID() + ":" + suffix;
    }

    @Test
    void synchronousKeyValueAndExpiry() {
        String key = key("value");
        assertThat(sync.hasKey(key)).isFalse();
        assertThat(sync.set(key, "first")).isTrue();
        assertThat(sync.getString(key)).isEqualTo("first");
        assertThat(sync.setNx(key, "second")).isFalse();
        assertThat(sync.getString(key)).isEqualTo("first");
        assertThat(sync.expire(key, Duration.ofSeconds(30))).isTrue();
        assertThat(sync.getExpire(key)).isBetween(1L, 30L);
        assertThat(sync.persist(key)).isTrue();
        assertThat(sync.getExpire(key)).isEqualTo(-1L);
        String renamed = key("renamed");
        sync.rename(key, renamed);
        assertThat(sync.hasKey(key)).isFalse();
        assertThat(sync.getString(renamed)).isEqualTo("first");
        assertThat(sync.del(renamed)).isEqualTo(1L);
        assertThat(sync.hasKey(renamed)).isFalse();
    }

    @Test
    void reactivePublishersChangeStateOnlyWhenSubscribed() {
        String key = key("reactive");
        var pending = reactive.set(key, "first");
        assertThat(reactive.hasKey(key).block()).isFalse();
        assertThat(pending.block()).isTrue();
        assertThat(reactive.getString(key).block()).isEqualTo("first");
        assertThat(reactive.setNX(key, "second").block()).isFalse();
        assertThat(reactive.expire(key, Duration.ofSeconds(30)).block()).isTrue();
        assertThat(reactive.getExpire(key).block()).isBetween(Duration.ofSeconds(1), Duration.ofSeconds(30));
        assertThat(reactive.del(key).block()).isEqualTo(1L);
        assertThat(reactive.hasKey(key).block()).isFalse();
    }

    @Test
    void synchronousListOrderingAndRemoval() {
        String key = key("list");
        assertThat(sync.lRightPush(key, "one")).isEqualTo(1L);
        assertThat(sync.lRightPush(key, "two")).isEqualTo(2L);
        assertThat(sync.lLeftPush(key, "zero")).isEqualTo(3L);
        assertThat(sync.lRangeString(key, 0, -1)).containsExactly("zero", "one", "two");
        assertThat(sync.lIndexString(key, 1)).isEqualTo("one");
        assertThat(sync.lLeftPop(key)).isEqualTo("zero");
        assertThat(sync.lRightPop(key)).isEqualTo("two");
        assertThat(sync.lSize(key)).isEqualTo(1L);
    }

    @Test
    void reactiveListOrderingAndRemoval() {
        String key = key("list-reactive");
        assertThat(reactive.lRightPush(key, "one").block()).isEqualTo(1L);
        assertThat(reactive.lRightPush(key, "two").block()).isEqualTo(2L);
        assertThat(reactive.lLeftPush(key, "zero").block()).isEqualTo(3L);
        assertThat(reactive.lRangeString(key, 0, -1).collectList().block())
                .containsExactly("zero", "one", "two");
        assertThat(reactive.lLeftPop(key).block()).isEqualTo("zero");
        assertThat(reactive.lRightPop(key).block()).isEqualTo("two");
        assertThat(reactive.lSize(key).block()).isEqualTo(1L);
    }

    @Test
    void synchronousHashAndSetChangesAreVisible() {
        String hash = key("hash");
        assertThat(sync.hSet(hash, "status", "new")).isTrue();
        assertThat(sync.hGet(hash, "status")).isEqualTo("new");
        assertThat(sync.hHasKey(hash, "status")).isTrue();
        assertThat(sync.hSet(hash, "status", "ready")).isTrue();
        assertThat(sync.hGet(hash, "status")).isEqualTo("ready");
        assertThat(sync.hSize(hash)).isEqualTo(1L);
        String set = key("set");
        assertThat(sync.sAdd(set, "a", "b")).isEqualTo(2L);
        assertThat(sync.sAdd(set, "a")).isZero();
        assertThat(sync.sGet(set)).containsExactlyInAnyOrder("a", "b");
        assertThat(sync.sHasKey(set, "b")).isTrue();
        assertThat(sync.sRemove(set, "b")).isEqualTo(1L);
        assertThat(sync.sGet(set)).containsExactly("a");
    }

    @Test
    void reactiveHashAndSetChangesAreVisible() {
        String hash = key("hash-reactive");
        assertThat(reactive.hSet(hash, "status", "new").block()).isTrue();
        assertThat(reactive.hGet(hash, "status").block()).isEqualTo("new");
        assertThat(reactive.hHasKey(hash, "status").block()).isTrue();
        assertThat(reactive.hSet(hash, "status", "ready").block()).isFalse();
        assertThat(reactive.hGet(hash, "status").block()).isEqualTo("ready");
        assertThat(reactive.hSize(hash).block()).isEqualTo(1L);
        String set = key("set-reactive");
        assertThat(reactive.sAdd(set, "a", "b").block()).isEqualTo(2L);
        assertThat(reactive.sAdd(set, "a").block()).isZero();
        assertThat(reactive.sGet(set).collectList().block()).containsExactlyInAnyOrder("a", "b");
        assertThat(reactive.sHasKey(set, "b").block()).isTrue();
        assertThat(reactive.sRemove(set, "b").block()).isEqualTo(1L);
        assertThat(reactive.sGet(set).collectList().block()).containsExactly("a");
    }

    @Test
    void synchronousCountersAndSortedSets() {
        String counter = key("counter");
        assertThat(sync.incr(counter, 5L)).isEqualTo(5L);
        assertThat(sync.decr(counter, 2L)).isEqualTo(3L);
        assertThat(sync.getLong(counter)).isEqualTo(3L);
        String ranked = key("ranked");
        assertThat(sync.zAdd(ranked, "low", 1.0)).isTrue();
        assertThat(sync.zAdd(ranked, "high", 3.0)).isTrue();
        assertThat(sync.zCard(ranked)).isEqualTo(2L);
        assertThat(sync.zRange(ranked, 0, -1)).containsExactly("low", "high");
        assertThat(sync.zIncr(ranked, "low", 4.0)).isEqualTo(5.0);
        assertThat(sync.zScore(ranked, "low")).isEqualTo(5.0);
        assertThat(sync.zRange(ranked, 0, -1)).containsExactly("high", "low");
    }

    @Test
    void reactiveCountersAndSortedSets() {
        String counter = key("counter-reactive");
        assertThat(reactive.incr(counter, 5L).block()).isEqualTo(5L);
        assertThat(reactive.decr(counter, 2L).block()).isEqualTo(3L);
        assertThat(reactive.getLong(counter).block()).isEqualTo(3L);
        String ranked = key("ranked-reactive");
        assertThat(reactive.zAdd(ranked, "low", 1.0).block()).isTrue();
        assertThat(reactive.zAdd(ranked, "high", 3.0).block()).isTrue();
        assertThat(reactive.zCard(ranked).block()).isEqualTo(2L);
        assertThat(reactive.zScore(ranked, "low").block()).isEqualTo(1.0);
        assertThat(reactive.zIncr(ranked, "low", 4.0).block()).isEqualTo(5.0);
        assertThat(reactive.zScore(ranked, "low").block()).isEqualTo(5.0);
    }
}
