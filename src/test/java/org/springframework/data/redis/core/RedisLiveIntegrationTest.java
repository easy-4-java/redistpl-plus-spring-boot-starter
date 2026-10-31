package org.springframework.data.redis.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Range;
import org.springframework.data.geo.Point;
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

    @Test
    void synchronousValueVariantsPreserveExpiryAndBatchResults() {
        String prefix = key("batch");
        String first = prefix + ":1";
        String second = prefix + ":2";
        assertThat(sync.set(first, "11", 30L)).isTrue();
        assertThat(sync.set(second, "22", Duration.ofSeconds(30))).isTrue();
        assertThat(sync.getExpire(first)).isBetween(1L, 30L);
        assertThat(sync.getExpire(second)).isBetween(1L, 30L);
        assertThat(sync.getLong(first)).isEqualTo(11L);
        assertThat(sync.getInteger(second)).isEqualTo(22);
        assertThat(sync.getString(prefix + ":absent", "fallback")).isEqualTo("fallback");
        assertThat(sync.mGetString(List.of(first, second))).containsExactly("11", "22");
        assertThat(sync.keys(prefix + ":*")).containsExactlyInAnyOrder(first, second);
        assertThat(sync.getAndSet(first, "33")).isEqualTo("11");
        assertThat(sync.getString(first)).isEqualTo("33");
        assertThat(sync.delPattern(prefix + ":*")).isEqualTo(2L);
        assertThat(sync.hasKey(first)).isFalse();
        assertThat(sync.hasKey(second)).isFalse();
    }

    @Test
    void reactiveValueVariantsPreserveExpiryAndBatchResults() {
        String prefix = key("batch-reactive");
        String first = prefix + ":1";
        String second = prefix + ":2";
        assertThat(reactive.set(first, "11", 30L).block()).isTrue();
        assertThat(reactive.set(second, "22", Duration.ofSeconds(30)).block()).isTrue();
        assertThat(reactive.getExpire(first).block()).isBetween(Duration.ofSeconds(1), Duration.ofSeconds(30));
        assertThat(reactive.getLong(first).block()).isEqualTo(11L);
        assertThat(reactive.getInteger(second).block()).isEqualTo(22);
        assertThat(reactive.mGetString(List.of(first, second)).block()).containsExactly("11", "22");
        assertThat(reactive.setNx(first, "duplicate", Duration.ofSeconds(30)).block()).isFalse();
        assertThat(reactive.getString(first).block()).isEqualTo("11");
        assertThat(reactive.del(first, second).block()).isEqualTo(2L);
        assertThat(reactive.hasKey(first).block()).isFalse();
        assertThat(reactive.hasKey(second).block()).isFalse();
    }

    @Test
    void synchronousHashCountersAndSetAlgebra() {
        String hash = key("hash-count");
        assertThat(sync.hIncr(hash, "hits", 5L)).isEqualTo(5L);
        assertThat(sync.hDecr(hash, "hits", 2L)).isEqualTo(3L);
        assertThat(sync.hGetLong(hash, "hits")).isEqualTo(3L);
        assertThat(sync.hIncr(hash, "ratio", 1.5)).isEqualTo(1.5);
        assertThat(sync.hGetDouble(hash, "ratio")).isEqualTo(1.5);
        String left = key("left");
        String right = key("right");
        assertThat(sync.sAdd(left, "a", "b")).isEqualTo(2L);
        assertThat(sync.sAdd(right, "b", "c")).isEqualTo(2L);
        assertThat(sync.sDiff(left, right)).containsExactly("a");
        assertThat(sync.sIntersect(left, right)).containsExactly("b");
        assertThat(sync.sUnion(left, right)).containsExactlyInAnyOrder("a", "b", "c");
        assertThat(sync.sSize(left)).isEqualTo(2L);
    }

    @Test
    void reactiveHashCountersAndSetAlgebra() {
        String hash = key("hash-count-reactive");
        assertThat(reactive.hIncr(hash, "hits", 5L).block()).isEqualTo(5L);
        assertThat(reactive.hDecr(hash, "hits", 2L).block()).isEqualTo(3L);
        assertThat(reactive.hGetLong(hash, "hits").block()).isEqualTo(3L);
        assertThat(reactive.hIncr(hash, "ratio", 1.5).block()).isEqualTo(1.5);
        assertThat(reactive.hGetDouble(hash, "ratio").block()).isEqualTo(1.5);
        String left = key("left-reactive");
        String right = key("right-reactive");
        assertThat(reactive.sAdd(left, "a", "b").block()).isEqualTo(2L);
        assertThat(reactive.sAdd(right, "b", "c").block()).isEqualTo(2L);
        assertThat(reactive.sDiff(left, right).collectList().block()).containsExactly("a");
        assertThat(reactive.sIntersect(left, right).collectList().block()).containsExactly("b");
        assertThat(reactive.sUnion(left, right).collectList().block()).containsExactlyInAnyOrder("a", "b", "c");
        assertThat(reactive.sSize(left).block()).isEqualTo(2L);
    }

    @Test
    void reactiveSortedSetChangesRanking() {
        String key = key("zset-reactive");
        assertThat(reactive.zAdd(key, "one", 1.0).block()).isTrue();
        assertThat(reactive.zAdd(key, "two", 2.0).block()).isTrue();
        assertThat(reactive.zRange(key, Range.closed(0L, 1L)).collectList().block())
                .containsExactly("one", "two");
        assertThat(reactive.zRem(key, "one").block()).isEqualTo(1L);
        assertThat(reactive.zRange(key, Range.closed(0L, 1L)).collectList().block())
                .containsExactly("two");
    }

    @Test
    void geoQueriesReturnNearbyMembersAndMetricDistance() {
        GeoTemplate geo = new GeoTemplate(sync.getRedisTemplate());
        String a = key("geo-a");
        String b = key("geo-b");
        assertThat(geo.geoAdd(a, 0.0, 0.0)).isEqualTo(1L);
        assertThat(geo.geoAdd(new Point(0.01, 0.0), b)).isEqualTo(1L);
        assertThat(geo.distanceValue(a, b)).isBetween(1000.0, 1200.0);
        assertThat(geo.getCircleUsersByDistance(a, 2000.0,
                result -> (String) result.getContent().getName())).contains(a, b);
        assertThat(geo.getCircleUsersByRadius(a, 2000.0,
                result -> (String) result.getContent().getName())).contains(a, b);
        assertThat(geo.getWGS84Distance(0.0, 0.0, 0.0, 0.01)).isBetween(1000.0, 1200.0);
    }

    @Test
    void luaScriptReadsActualStoredValue() {
        String key = key("lua");
        sync.set(key, "value");
        String script = "return redis.call('GET', KEYS[1])";
        assertThat(sync.executeLuaScript(script, String.class, List.of(key))).isEqualTo("value");
        assertThat(reactive.executeLuaScript(script, String.class, List.of(key)).block())
                .isEqualTo("value");
    }
}
