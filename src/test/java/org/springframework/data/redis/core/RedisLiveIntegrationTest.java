package org.springframework.data.redis.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.springframework.data.domain.Range;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import reactor.core.publisher.Mono;

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
        try {
            if (connection != null) connection.destroy();
        } finally {
            if (redis != null) redis.close();
        }
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

    @Test
    void reactiveTimedListWriteActuallySetsExpiry() {
        String key = key("timed-list");
        assertThat(reactive.lLeftPush(key, "one", 30L).block()).isEqualTo(1L);
        assertThat(reactive.lRangeString(key, 0, -1).collectList().block()).containsExactly("one");
        assertThat(reactive.getExpire(key).block())
                .isBetween(Duration.ofSeconds(1), Duration.ofSeconds(30));
    }

    @Test
    void reactiveTimedWritesAcrossCommandFamiliesSetExpiry() {
        String counter = key("timed-counter");
        assertThat(reactive.incr(counter, 2L, 30L).block()).isEqualTo(2L);
        assertThat(reactive.getLong(counter).block()).isEqualTo(2L);
        assertTimed(counter);

        String hash = key("timed-hash");
        assertThat(reactive.hmSet(hash, Map.<Object, Object>of("status", "ready"), Duration.ofSeconds(30)).block()).isTrue();
        assertThat(reactive.hGetString(hash, "status").block()).isEqualTo("ready");
        assertTimed(hash);

        String set = key("timed-set");
        assertThat(reactive.sAddAndExpire(set, 30L, "member").block()).isEqualTo(1L);
        assertThat(reactive.sHasKey(set, "member").block()).isTrue();
        assertTimed(set);

        String ranked = key("timed-zset");
        assertThat(reactive.zIncr(ranked, "member", 2.0, Duration.ofSeconds(30)).block()).isEqualTo(2.0);
        assertThat(reactive.zScore(ranked, "member").block()).isEqualTo(2.0);
        assertTimed(ranked);
    }

    private void assertTimed(String key) {
        assertThat(reactive.getExpire(key).block())
                .isBetween(Duration.ofSeconds(1), Duration.ofSeconds(30));
    }

    private record TimedWrite(String name, Consumer<String> verify) {}

    @TestFactory
    Stream<DynamicTest> reactiveTimedOverloadsKeepTheirValueAndExpiry() {
        return Stream.of(
                new TimedWrite("increment long duration", key -> {
                    assertThat(reactive.incr(key, 4L, Duration.ofSeconds(30)).block()).isEqualTo(4L);
                    assertThat(reactive.getLong(key).block()).isEqualTo(4L);
                }),
                new TimedWrite("increment double seconds", key -> {
                    assertThat(reactive.incr(key, 1.5, 30L).block()).isEqualTo(1.5);
                    assertThat(reactive.getDouble(key).block()).isEqualTo(1.5);
                }),
                new TimedWrite("decrement long seconds", key -> {
                    assertThat(reactive.decr(key, 2L, 30L).block()).isEqualTo(-2L);
                    assertThat(reactive.getLong(key).block()).isEqualTo(-2L);
                }),
                new TimedWrite("decrement double duration", key -> {
                    assertThat(reactive.decr(key, 1.25, Duration.ofSeconds(30)).block()).isEqualTo(-1.25);
                    assertThat(reactive.getDouble(key).block()).isEqualTo(-1.25);
                }),
                new TimedWrite("left push duration", key -> {
                    assertThat(reactive.lLeftPush(key, "one", Duration.ofSeconds(30)).block()).isEqualTo(1L);
                    assertThat(reactive.lRangeString(key, 0, -1).collectList().block()).containsExactly("one");
                }),
                new TimedWrite("left push all seconds", key -> {
                    assertThat(reactive.lLeftPushAll(key, List.of("one", "two"), 30L).block()).isEqualTo(2L);
                    assertThat(reactive.lRangeString(key, 0, -1).collectList().block()).containsExactly("two", "one");
                }),
                new TimedWrite("left push if present duration", key -> {
                    assertThat(reactive.lLeftPush(key, "one").block()).isEqualTo(1L);
                    assertThat(reactive.lLeftPushx(key, "zero", Duration.ofSeconds(30)).block()).isEqualTo(2L);
                    assertThat(reactive.lRangeString(key, 0, -1).collectList().block()).containsExactly("zero", "one");
                }),
                new TimedWrite("right push seconds", key -> {
                    assertThat(reactive.lRightPush(key, "one", 30L).block()).isEqualTo(1L);
                    assertThat(reactive.lRangeString(key, 0, -1).collectList().block()).containsExactly("one");
                }),
                new TimedWrite("right push all duration", key -> {
                    assertThat(reactive.lRightPushAll(key, List.of("one", "two"), Duration.ofSeconds(30)).block()).isEqualTo(2L);
                    assertThat(reactive.lRangeString(key, 0, -1).collectList().block()).containsExactly("one", "two");
                }),
                new TimedWrite("right push if present seconds", key -> {
                    assertThat(reactive.lRightPush(key, "one").block()).isEqualTo(1L);
                    assertThat(reactive.lRightPushx(key, "two", 30L).block()).isEqualTo(2L);
                    assertThat(reactive.lRangeString(key, 0, -1).collectList().block()).containsExactly("one", "two");
                }),
                new TimedWrite("hash set seconds", key -> {
                    assertThat(reactive.hSet(key, "status", "ready", 30L).block()).isTrue();
                    assertThat(reactive.hGetString(key, "status").block()).isEqualTo("ready");
                }),
                new TimedWrite("hash increment long duration", key -> {
                    assertThat(reactive.hIncr(key, "hits", 4L, Duration.ofSeconds(30)).block()).isEqualTo(4L);
                    assertThat(reactive.hGetLong(key, "hits").block()).isEqualTo(4L);
                }),
                new TimedWrite("hash increment double seconds", key -> {
                    assertThat(reactive.hIncr(key, "ratio", 1.5, 30L).block()).isEqualTo(1.5);
                    assertThat(reactive.hGetDouble(key, "ratio").block()).isEqualTo(1.5);
                }),
                new TimedWrite("set add duration", key -> {
                    assertThat(reactive.sAddAndExpire(key, Duration.ofSeconds(30), "member").block()).isEqualTo(1L);
                    assertThat(reactive.sHasKey(key, "member").block()).isTrue();
                }),
                new TimedWrite("sorted set increment seconds", key -> {
                    assertThat(reactive.zIncr(key, "member", 1.5, 30L).block()).isEqualTo(1.5);
                    assertThat(reactive.zScore(key, "member").block()).isEqualTo(1.5);
                })
        ).map(write -> DynamicTest.dynamicTest(write.name(), () -> {
            String key = key("timed-" + write.name().replace(' ', '-'));
            write.verify().accept(key);
            assertTimed(key);
        }));
    }

    @Test
    void streamRecordsCanBeReadAndDeletedById() {
        String stream = key("stream");
        var first = sync.xAdd(stream, Map.of("event", "created"));
        var second = sync.xAdd(stream, Map.of("event", "updated"));
        assertThat(first).isNotNull();
        assertThat(second).isNotEqualTo(first);
        assertThat(sync.xLen(stream)).isEqualTo(2L);
        assertThat(sync.xRange(stream, Range.unbounded()))
                .extracting(record -> record.getValue().get("event"))
                .containsExactly("created", "updated");
        assertThat(sync.xDel(stream, first)).isEqualTo(1L);
        assertThat(sync.xLen(stream)).isEqualTo(1L);
        assertThat(sync.xRange(stream, Range.unbounded()))
                .extracting(record -> record.getValue().get("event"))
                .containsExactly("updated");
    }

    @Test
    void bitmapAndHyperLogLogCommandsUseRealServerState() {
        String bitmap = key("bitmap");
        assertThat(sync.getBit(bitmap, 7)).isFalse();
        assertThat(sync.setBit(bitmap, 7, true)).isFalse();
        assertThat(sync.getBit(bitmap, 7)).isTrue();
        assertThat(reactive.getBit(bitmap, 7).block()).isTrue();
        assertThat(reactive.setBit(bitmap, 7, false).block()).isTrue();
        assertThat(sync.getBit(bitmap, 7)).isFalse();

        String first = key("hll-1");
        String second = key("hll-2");
        assertThat(sync.pfAdd(first, "a", "b")).isEqualTo(1L);
        assertThat(sync.pfCount(first)).isEqualTo(2L);
        assertThat(reactive.pfAdd(second, "b", "c").block()).isEqualTo(1L);
        assertThat(reactive.pfCount(second).block()).isEqualTo(2L);
        String merged = key("hll-merged");
        assertThat(sync.pfMerge(merged, first, second)).isEqualTo(3L);
        assertThat(sync.pfCount(merged)).isEqualTo(3L);
    }

    @Test
    void synchronousSetStoreOperationsPersistComputedMembers() {
        String left = key("set-left-store");
        String right = key("set-right-store");
        sync.sAdd(left, "a", "b");
        sync.sAdd(right, "b", "c");
        String diff = key("set-diff-store");
        String intersection = key("set-intersection-store");
        String union = key("set-union-store");
        assertThat(sync.sDiffAndStore(left, right, diff)).isEqualTo(1L);
        assertThat(sync.sGet(diff)).containsExactly("a");
        assertThat(sync.sIntersectAndStore(left, right, intersection)).isEqualTo(1L);
        assertThat(sync.sGet(intersection)).containsExactly("b");
        assertThat(sync.sUnionAndStore(left, right, union)).isEqualTo(3L);
        assertThat(sync.sGet(union)).containsExactlyInAnyOrder("a", "b", "c");
    }

    @Test
    void reactiveSetStoreOperationsPersistComputedMembers() {
        String left = key("reactive-set-left-store");
        String right = key("reactive-set-right-store");
        reactive.sAdd(left, "a", "b").block();
        reactive.sAdd(right, "b", "c").block();
        String diff = key("reactive-set-diff-store");
        String intersection = key("reactive-set-intersection-store");
        String union = key("reactive-set-union-store");
        assertThat(reactive.sDiffAndStore(left, right, diff).block()).isEqualTo(1L);
        assertThat(reactive.sGet(diff).collectList().block()).containsExactly("a");
        assertThat(reactive.sIntersectAndStore(left, right, intersection).block()).isEqualTo(1L);
        assertThat(reactive.sGet(intersection).collectList().block()).containsExactly("b");
        assertThat(reactive.sUnionAndStore(left, right, union).block()).isEqualTo(3L);
        assertThat(reactive.sGet(union).collectList().block()).containsExactlyInAnyOrder("a", "b", "c");
    }

    @Test
    void sortedSetScoreRangesAndStoresPreserveOrderAndScores() {
        String left = key("z-left");
        String right = key("z-right");
        assertThat(sync.zAdd(left, "a", 1.0)).isTrue();
        assertThat(sync.zAdd(left, "b", 3.0)).isTrue();
        assertThat(sync.zAdd(right, "b", 2.0)).isTrue();
        assertThat(sync.zAdd(right, "c", 4.0)).isTrue();
        assertThat(sync.zCount(left, 1.0, 2.0)).isEqualTo(1L);
        assertThat(sync.zRangeByScore(left, 2.0, 4.0)).containsExactly("b");
        assertThat(reactive.zRangeByScore(left, Range.closed(2.0, 4.0)).collectList().block())
                .containsExactly("b");
        assertThat(reactive.zRevrange(left, Range.closed(0L, 1L)).collectList().block())
                .containsExactly("b", "a");
        assertThat(sync.zRevRank(left, "b")).isZero();
        String intersection = key("z-intersection");
        assertThat(sync.zIntersectAndStore(left, right, intersection)).isEqualTo(1L);
        assertThat(sync.zScore(intersection, "b")).isEqualTo(5.0);
        String union = key("z-union");
        assertThat(reactive.zUnionAndStore(left, right, union).block()).isEqualTo(3L);
        assertThat(reactive.zRange(union, Range.closed(0L, 2L)).collectList().block())
                .containsExactly("a", "c", "b");
        assertThat(reactive.zScore(union, "b").block()).isEqualTo(5.0);
        assertThat(reactive.zRemByScore(union, Range.closed(4.0, 5.0)).block()).isEqualTo(2L);
        assertThat(reactive.zRange(union, Range.closed(0L, 2L)).collectList().block())
                .containsExactly("a");
    }

    @Test
    void keyDiscoveryRenameAndAbsoluteExpiryObserveRealState() {
        String prefix = key("keys");
        String original = prefix + ":original";
        String renamed = prefix + ":renamed";
        sync.set(original, "value");
        assertThat(sync.type(original)).isEqualTo(DataType.STRING);
        assertThat(sync.keys(prefix + ":*")).contains(original);
        assertThat(sync.scan(prefix + ":*", 10)).contains(original);
        assertThat(reactive.getKey(prefix + ":*").collectList().block()).contains(original);
        assertThat(sync.renameIfAbsent(original, renamed)).isTrue();
        assertThat(sync.hasKey(original)).isFalse();
        assertThat(sync.getString(renamed)).isEqualTo("value");
        assertThat(sync.expireAt(renamed, new Date(System.currentTimeMillis() + 30_000))).isTrue();
        assertThat(sync.getExpire(renamed)).isBetween(1L, 30L);
        assertThat(reactive.getVagueKey("renamed").collectList().block()).contains(renamed);
    }

    @Test
    void wrongRedisTypeFailsWithoutChangingTheOriginalValue() {
        String key = key("wrong-type");
        sync.set(key, "plain");
        assertThatThrownBy(() -> sync.hSet(key, "field", "value"))
                .isInstanceOf(RedisOperationException.class);
        assertThat(sync.getString(key)).isEqualTo("plain");
        assertThatThrownBy(() -> reactive.hSet(key, "field", "value").block())
                .satisfies(error -> assertThat(error.getCause()).hasMessageContaining("WRONGTYPE"));
        assertThat(reactive.getString(key).block()).isEqualTo("plain");
    }

    @Test
    void synchronousHashBatchReadScanAndDelete() {
        String hash = key("hash-batch");
        assertThat(sync.hmSet(hash, Map.of("a", "11", "b", "22"))).isTrue();
        assertThat(sync.hmGet(hash)).containsEntry("a", "11").containsEntry("b", "22");
        assertThat(sync.hmGet(hash, List.of("b", "a"))).containsExactly("22", "11");
        assertThat(sync.hmMultiGet(hash, List.of("a", "b")))
                .containsEntry("a", "11").containsEntry("b", "22");
        assertThat(sync.hKeys(hash)).containsExactlyInAnyOrder("a", "b");
        assertThat(sync.hValues(hash)).containsExactlyInAnyOrder("11", "22");
        List<Map.Entry<Object, Object>> visited = new ArrayList<>();
        sync.hScan(hash, visited::add);
        assertThat(visited).extracting(Map.Entry::getKey).containsExactlyInAnyOrder("a", "b");
        sync.hDel(hash, "a");
        assertThat(sync.hHasKey(hash, "a")).isFalse();
        assertThat(sync.hGet(hash, "b")).isEqualTo("22");
    }

    @Test
    void reactiveHashBatchReadAndDelete() {
        String hash = key("hash-batch-reactive");
        assertThat(reactive.hmSet(hash, Map.<Object, Object>of("a", "11", "b", "22")).block()).isTrue();
        assertThat(reactive.hmMultiGet(hash, List.<Object>of("a", "b")).block())
                .containsEntry("a", "11").containsEntry("b", "22");
        assertThat(reactive.hMultiGet(hash, List.<Object>of("b", "a")).block())
                .containsExactly("22", "11");
        assertThat(reactive.hKeys(hash).collectList().block()).containsExactlyInAnyOrder("a", "b");
        assertThat(reactive.hDel(hash, "a").block()).isEqualTo(1L);
        assertThat(reactive.hHasKey(hash, "a").block()).isFalse();
        assertThat(reactive.hGet(hash, "b").block()).isEqualTo("22");
    }

    @Test
    void synchronousListMutationKeepsExpectedPositions() {
        String list = key("list-mutate");
        assertThat(sync.lRightPushAll(list, List.of("a", "b", "c"))).isEqualTo(3L);
        assertThat(sync.lSet(list, 1, "middle")).isTrue();
        assertThat(sync.lRangeString(list, 0, -1)).containsExactly("a", "middle", "c");
        assertThat(sync.lRem(list, 1, "middle")).isEqualTo(1L);
        assertThat(sync.lRangeString(list, 0, -1)).containsExactly("a", "c");
        assertThat(sync.lTrim(list, 0, 0)).isTrue();
        assertThat(sync.lRangeString(list, 0, -1)).containsExactly("a");
    }

    @Test
    void reactiveListMutationKeepsExpectedPositions() {
        String list = key("list-mutate-reactive");
        assertThat(reactive.lRightPushAll(list, List.of("a", "b", "c")).block()).isEqualTo(3L);
        assertThat(reactive.lSet(list, 1, "middle").block()).isTrue();
        assertThat(reactive.lRangeString(list, 0, -1).collectList().block())
                .containsExactly("a", "middle", "c");
        assertThat(reactive.lRem(list, 1, "middle").block()).isEqualTo(1L);
        assertThat(reactive.lRangeString(list, 0, -1).collectList().block()).containsExactly("a", "c");
        assertThat(reactive.lTrim(list, 0, 0).block()).isTrue();
        assertThat(reactive.lRangeString(list, 0, -1).collectList().block()).containsExactly("a");
    }

    @Test
    void synchronousTypedReadsConvertStoredNumericText() {
        String value = key("typed-value");
        sync.set(value, "12");
        assertThat(sync.getLong(value)).isEqualTo(12L);
        assertThat(sync.getInteger(value)).isEqualTo(12);
        assertThat(sync.getDouble(value)).isEqualTo(12.0);
        assertThat(sync.getLong(key("missing-long"), 7L)).isEqualTo(7L);
        String list = key("typed-list");
        sync.lRightPushAll(list, List.of("1", "2"));
        assertThat(sync.lRangeLong(list, 0, -1)).containsExactly(1L, 2L);
        assertThat(sync.lRangeDouble(list, 0, -1)).containsExactly(1.0, 2.0);
        String hash = key("typed-hash");
        sync.hmSet(hash, Map.of("a", "1", "b", "2"));
        assertThat(sync.hGetLong(hash, "a")).isEqualTo(1L);
        assertThat(sync.hGetDouble(hash, "b")).isEqualTo(2.0);
        String set = key("typed-set");
        sync.sAdd(set, "1", "2");
        assertThat(sync.sGetLong(set)).containsExactlyInAnyOrder(1L, 2L);
        String ranked = key("typed-zset");
        sync.zAdd(ranked, "1", 1.0);
        sync.zAdd(ranked, "2", 2.0);
        assertThat(sync.zRangeLong(ranked, 0, -1)).containsExactly(1L, 2L);
        assertThat(sync.zRangeDoubleByScore(ranked, 1.0, 2.0)).containsExactly(1.0, 2.0);
    }

    @Test
    void reactiveTypedReadsConvertStoredNumericText() {
        String value = key("typed-value-reactive");
        reactive.set(value, "12").block();
        assertThat(reactive.getLong(value).block()).isEqualTo(12L);
        assertThat(reactive.getInteger(value).block()).isEqualTo(12);
        assertThat(reactive.getDouble(value).block()).isEqualTo(12.0);
        String list = key("typed-list-reactive");
        reactive.lRightPushAll(list, List.of("1", "2")).block();
        assertThat(reactive.lRangeLong(list, 0, -1).collectList().block()).containsExactly(1L, 2L);
        assertThat(reactive.lRangeDouble(list, 0, -1).collectList().block()).containsExactly(1.0, 2.0);
        String hash = key("typed-hash-reactive");
        reactive.hmSet(hash, Map.<Object, Object>of("a", "1", "b", "2")).block();
        assertThat(reactive.hGetLong(hash, "a").block()).isEqualTo(1L);
        assertThat(reactive.hGetDouble(hash, "b").block()).isEqualTo(2.0);
        String set = key("typed-set-reactive");
        reactive.sAdd(set, "1", "2").block();
        assertThat(reactive.sGetLong(set).collectList().block()).containsExactlyInAnyOrder(1L, 2L);
        String ranked = key("typed-zset-reactive");
        reactive.zAdd(ranked, "1", 1.0).block();
        reactive.zAdd(ranked, "2", 2.0).block();
        assertThat(reactive.zRangeLong(ranked, Range.closed(0L, 1L)).collectList().block())
                .containsExactly(1L, 2L);
        assertThat(reactive.zRangeDoubleByScore(ranked, Range.closed(1.0, 2.0)).collectList().block())
                .containsExactly(1.0, 2.0);
    }

    @Test
    void stringRangesAndBatchNumericReadsUseActualRedisValues() {
        String string = key("range");
        sync.set(string, "hello");
        assertThat(sync.getRange(string, 0, 1)).isEqualTo("he");
        sync.setRange(string, "y", 1);
        assertThat(sync.getString(string)).isEqualTo("hyllo");
        assertThat(sync.append(string, "!")).isEqualTo(6);
        assertThat(sync.getStringAndSet(string, "done")).isEqualTo("hyllo!");
        assertThat(sync.getString(string)).isEqualTo("done");

        String first = key("numeric-one");
        String second = key("numeric-two");
        sync.set(first, "1");
        sync.set(second, "2");
        assertThat(sync.mGetLong(List.of(first, second))).containsExactly(1L, 2L);
        assertThat(sync.mGetDouble(List.of(first, second))).containsExactly(1.0, 2.0);
        assertThat(reactive.mGetLong(List.of(first, second)).block()).containsExactly(1L, 2L);
        assertThat(reactive.mGetDouble(List.of(first, second)).block()).containsExactly(1.0, 2.0);
    }

    @Test
    void distinctListPushMovesExistingMembersWithoutDuplication() {
        String syncList = key("distinct-sync");
        sync.lRightPushAll(syncList, List.of("a", "b", "a"));
        assertThat(sync.lLeftPushDistinct(syncList, "a")).isEqualTo(2L);
        assertThat(sync.lRangeString(syncList, 0, -1)).containsExactly("a", "b");
        assertThat(sync.lRightPushDistinct(syncList, "a")).isEqualTo(2L);
        assertThat(sync.lRangeString(syncList, 0, -1)).containsExactly("b", "a");

        String reactiveList = key("distinct-reactive");
        reactive.lRightPushAll(reactiveList, List.of("a", "b", "a")).block();
        assertThat(reactive.lLeftPushDistinct(reactiveList, "a").block()).isEqualTo(2L);
        assertThat(reactive.lRangeString(reactiveList, 0, -1).collectList().block())
                .containsExactly("a", "b");
        assertThat(reactive.lRightPushDistinct(reactiveList, "a").block()).isEqualTo(2L);
        assertThat(reactive.lRangeString(reactiveList, 0, -1).collectList().block())
                .containsExactly("b", "a");
    }

    @Test
    void reactiveRightPushIfPresentCollectionCompletesEveryWriteBeforeExpiry() {
        String secondsKey = key("pushx-seconds");
        List<String> seed = List.of("s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9");
        assertThat(reactive.lRightPushAll(secondsKey, seed).block()).isEqualTo(10L);
        assertThat(reactive.lRightPushxAll(secondsKey, List.of("a", "b", "c"), 30L).block())
                .isEqualTo(3L);
        assertThat(reactive.lRangeString(secondsKey, 0, -1).collectList().block())
                .containsExactly("s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "a", "b", "c");
        assertTimed(secondsKey);

        String durationKey = key("pushx-duration");
        assertThat(reactive.lRightPushAll(durationKey, seed).block()).isEqualTo(10L);
        assertThat(reactive.lRightPushxAll(durationKey, List.of("a", "b", "c"), Duration.ZERO).block())
                .isEqualTo(3L);
        assertThat(reactive.lRangeString(durationKey, 0, -1).collectList().block())
                .containsExactly("s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "a", "b", "c");
        assertThat(reactive.getExpire(durationKey).block()).isEqualTo(Duration.ZERO);
    }

    @Test
    void timedReactiveBatchIsColdAndNonPositiveTimeoutKeepsExistingTtlPolicy() {
        String key = key("pushx-cold");
        reactive.lRightPush(key, "seed").block();
        Mono<Long> pending = reactive.lRightPushxAll(key, List.of("a", "b"), 30L);
        assertThat(reactive.lRangeString(key, 0, -1).collectList().block()).containsExactly("seed");
        assertThat(reactive.getExpire(key).block()).isEqualTo(Duration.ZERO);
        assertThat(pending.block()).isEqualTo(2L);
        assertThat(reactive.lRangeString(key, 0, -1).collectList().block()).containsExactly("seed", "a", "b");
        assertTimed(key);

        String zero = key("pushx-zero");
        reactive.lRightPush(zero, "seed").block();
        assertThat(reactive.lRightPushxAll(zero, List.of("a", "b"), 0L).block()).isEqualTo(2L);
        assertThat(reactive.lRangeString(zero, 0, -1).collectList().block())
                .containsExactly("seed", "a", "b");
        assertThat(reactive.getExpire(zero).block()).isEqualTo(Duration.ZERO);

        String negative = key("pushx-negative");
        reactive.lRightPush(negative, "seed").block();
        assertThat(reactive.lRightPushxAll(negative, List.of("a", "b"), Duration.ofSeconds(-1)).block())
                .isEqualTo(2L);
        assertThat(reactive.lRangeString(negative, 0, -1).collectList().block())
                .containsExactly("seed", "a", "b");
        assertThat(reactive.getExpire(negative).block()).isEqualTo(Duration.ZERO);
        assertThat(reactive.lRightPushxAll(negative, List.<String>of(), Duration.ZERO).block()).isZero();
        assertThat(reactive.lRangeString(negative, 0, -1).collectList().block())
                .containsExactly("seed", "a", "b");
    }

    @Test
    void timedReactiveBatchDoesNotExpireAfterCommandOrLaterValueFailure() {
        String wrongType = key("pushx-wrong-type");
        sync.set(wrongType, "plain");
        assertThatThrownBy(() -> reactive.lRightPushxAll(wrongType, List.of("a", "b"), 30L).block())
                .hasCauseInstanceOf(io.lettuce.core.RedisCommandExecutionException.class);
        assertThat(sync.getString(wrongType)).isEqualTo("plain");
        assertThat(reactive.getExpire(wrongType).block()).isEqualTo(Duration.ZERO);

        String laterBadValue = key("pushx-later-bad-value");
        reactive.lRightPush(laterBadValue, "seed").block();
        assertThatThrownBy(() -> reactive.lRightPushxAll(laterBadValue,
                List.<Object>of("a", 123), 30L).block()).isInstanceOf(RuntimeException.class);
        assertThat(reactive.lRangeString(laterBadValue, 0, -1).collectList().block())
                .containsExactly("seed", "a");
        assertThat(reactive.getExpire(laterBadValue).block()).isEqualTo(Duration.ZERO);
    }

    @Test
    void expiryErrorPropagatesAfterSuccessfulWriteWithoutClaimingRollback() {
        String key = key("expiry-error");
        ReactiveRedisOperationTemplate failingExpiry = new ReactiveRedisOperationTemplate(
                reactive.getReactiveRedisTemplate(), new ObjectMapper()) {
            @Override
            public Mono<Boolean> expire(String key, long seconds) {
                return Mono.error(new RedisOperationException("expiry rejected"));
            }
        };
        assertThatThrownBy(() -> failingExpiry.lRightPush(key, "value", 30L).block())
                .isInstanceOf(RedisOperationException.class)
                .hasMessageContaining("expiry rejected");
        assertThat(reactive.lRangeString(key, 0, -1).collectList().block())
                .containsExactly("value");
        assertThat(reactive.getExpire(key).block()).isEqualTo(Duration.ZERO);
    }

    @Test
    void updatingExistingHashFieldReturnsFalseButStillSetsExpiry() {
        String key = key("hash-update-ttl");
        assertThat(reactive.hSet(key, "field", "old").block()).isTrue();
        assertThat(reactive.hSet(key, "field", "new", Duration.ofSeconds(30)).block()).isFalse();
        assertThat(reactive.hGetString(key, "field").block()).isEqualTo("new");
        assertTimed(key);
    }

    @Test
    void reactiveGeoDistanceHelpersUseMeters() {
        ReactiveGeoTemplate geo = new ReactiveGeoTemplate();
        assertThat(geo.getDistance(0.0, 0.0, 0.0, 0.01)).isBetween(1000.0, 1200.0);
        assertThat(geo.getSphereDistance(0.0, 0.0, 0.0, 0.01)).isBetween(1000.0, 1200.0);
        assertThat(geo.getWGS84Distance(0.0, 0.0, 0.0, 0.01)).isBetween(1000.0, 1200.0);
    }
}
