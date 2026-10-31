package org.springframework.data.redis.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.json.JsonMapperBuilderCustomizer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.springframework.data.domain.Range;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.Limit;
import org.springframework.data.redis.connection.zset.Aggregate;
import org.springframework.data.redis.connection.zset.DefaultTuple;
import org.springframework.data.redis.connection.zset.Weights;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import redistpl.plus.spring.boot.RedisCachingConfiguration;
import redistpl.plus.spring.boot.RedisReactiveCachingConfiguration;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import org.reactivestreams.Publisher;

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
                .isEqualTo(11L);
        assertThat(reactive.lRangeString(secondsKey, 0, -1).collectList().block())
                .containsExactly("s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "a", "b", "c");
        assertTimed(secondsKey);

        String durationKey = key("pushx-duration");
        assertThat(reactive.lRightPushAll(durationKey, seed).block()).isEqualTo(10L);
        assertThat(reactive.lRightPushxAll(durationKey, List.of("a", "b", "c"), Duration.ZERO).block())
                .isEqualTo(11L);
        assertThat(reactive.lRangeString(durationKey, 0, -1).collectList().block())
                .containsExactly("s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "a", "b", "c");
        assertThat(reactive.getExpire(durationKey).block()).isEqualTo(Duration.ZERO);

        String missing = key("pushx-missing");
        assertThat(reactive.lRightPushxAll(missing, List.of("a", "b", "c"), 30L).block())
                .isZero();
        assertThat(reactive.hasKey(missing).block()).isFalse();
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

    @Test
    void synchronousHashCounterOverloadsPreserveTypeValueAndExpiry() {
        String integer = key("hash-int-overloads");
        assertThat(sync.hIncr(integer, "count", 2)).isEqualTo(2L);
        assertThat(sync.hIncr(integer, "count", 3, 30L)).isEqualTo(5L);
        assertThat(sync.hIncr(integer, "count", 4, Duration.ofSeconds(30))).isEqualTo(9L);
        assertThat(sync.hDecr(integer, "count", 1)).isEqualTo(8L);
        assertThat(sync.hGetLong(integer, "count")).isEqualTo(8L);
        assertThat(sync.getExpire(integer)).isBetween(1L, 30L);

        String longCounter = key("hash-long-overloads");
        assertThat(sync.hIncr(longCounter, "count", 2L)).isEqualTo(2L);
        assertThat(sync.hIncr(longCounter, "count", 3L, 30L)).isEqualTo(5L);
        assertThat(sync.hIncr(longCounter, "count", 4L, Duration.ofSeconds(30))).isEqualTo(9L);
        assertThat(sync.hDecr(longCounter, "count", 2L)).isEqualTo(7L);
        assertThat(sync.hGetLong(longCounter, "count")).isEqualTo(7L);
        assertThat(sync.getExpire(longCounter)).isBetween(1L, 30L);

        String decimal = key("hash-decimal-overloads");
        assertThat(sync.hIncr(decimal, "count", 1.25)).isEqualTo(1.25);
        assertThat(sync.hIncr(decimal, "count", 1.5, 30L)).isEqualTo(2.75);
        assertThat(sync.hIncr(decimal, "count", 2.0, Duration.ofSeconds(30))).isEqualTo(4.75);
        assertThat(sync.hDecr(decimal, "count", 0.5)).isEqualTo(4.25);
        assertThat(sync.hGetDouble(decimal, "count")).isEqualTo(4.25);
        assertThat(sync.getExpire(decimal)).isBetween(1L, 30L);
        assertThatThrownBy(() -> sync.hIncr(decimal, "count", -1L))
                .isInstanceOf(RedisOperationException.class);
        assertThat(sync.hGetDouble(decimal, "count")).isEqualTo(4.25);
    }

    @Test
    void reactiveHashCounterOverloadsPreserveTypeValueAndExpiry() {
        String integer = key("reactive-hash-int-overloads");
        assertThat(reactive.hIncr(integer, "count", 2).block()).isEqualTo(2L);
        assertThat(reactive.hIncr(integer, "count", 3, 30L).block()).isEqualTo(5L);
        assertThat(reactive.hIncr(integer, "count", 4, Duration.ofSeconds(30)).block()).isEqualTo(9L);
        assertThat(reactive.hDecr(integer, "count", 1).block()).isEqualTo(8L);
        assertThat(reactive.hGetLong(integer, "count").block()).isEqualTo(8L);
        assertTimed(integer);

        String longCounter = key("reactive-hash-long-overloads");
        assertThat(reactive.hIncr(longCounter, "count", 2L).block()).isEqualTo(2L);
        assertThat(reactive.hIncr(longCounter, "count", 3L, 30L).block()).isEqualTo(5L);
        assertThat(reactive.hIncr(longCounter, "count", 4L, Duration.ofSeconds(30)).block()).isEqualTo(9L);
        assertThat(reactive.hDecr(longCounter, "count", 2L).block()).isEqualTo(7L);
        assertThat(reactive.hGetLong(longCounter, "count").block()).isEqualTo(7L);
        assertTimed(longCounter);

        String decimal = key("reactive-hash-decimal-overloads");
        assertThat(reactive.hIncr(decimal, "count", 1.25).block()).isEqualTo(1.25);
        assertThat(reactive.hIncr(decimal, "count", 1.5, 30L).block()).isEqualTo(2.75);
        assertThat(reactive.hIncr(decimal, "count", 2.0, Duration.ofSeconds(30)).block()).isEqualTo(4.75);
        assertThat(reactive.hDecr(decimal, "count", 0.5).block()).isEqualTo(4.25);
        assertThat(reactive.hGetDouble(decimal, "count").block()).isEqualTo(4.25);
        assertTimed(decimal);
        assertThatThrownBy(() -> reactive.hIncr(decimal, "count", -1L).block())
                .isInstanceOf(RedisOperationException.class);
        assertThat(reactive.hGetDouble(decimal, "count").block()).isEqualTo(4.25);
    }

    @Test
    void synchronousValueCounterOverloadsPreserveNumericValuesAndExpiry() {
        String whole = key("value-whole-counter");
        assertThat(sync.incr(whole, 2L)).isEqualTo(2L);
        assertThat(sync.incr(whole, 3L, 30L)).isEqualTo(5L);
        assertThat(sync.incr(whole, 4L, Duration.ofSeconds(30))).isEqualTo(9L);
        assertThat(sync.decr(whole, 1L)).isEqualTo(8L);
        assertThat(sync.decr(whole, 2L, 30L)).isEqualTo(6L);
        assertThat(sync.decr(whole, 3L, Duration.ofSeconds(30))).isEqualTo(3L);
        assertThat(sync.getLong(whole)).isEqualTo(3L);
        assertThat(sync.getExpire(whole)).isBetween(1L, 30L);

        String decimal = key("value-decimal-counter");
        assertThat(sync.incr(decimal, 1.25)).isEqualTo(1.25);
        assertThat(sync.incr(decimal, 1.5, 30L)).isEqualTo(2.75);
        assertThat(sync.incr(decimal, 2.0, Duration.ofSeconds(30))).isEqualTo(4.75);
        assertThat(sync.decr(decimal, 0.5)).isEqualTo(4.25);
        assertThat(sync.decr(decimal, 0.25, 30L)).isEqualTo(4.0);
        assertThat(sync.decr(decimal, 0.5, Duration.ofSeconds(30))).isEqualTo(3.5);
        assertThat(sync.getDouble(decimal)).isEqualTo(3.5);
        assertThat(sync.getExpire(decimal)).isBetween(1L, 30L);
        assertThatThrownBy(() -> sync.incr(whole, -1L)).isInstanceOf(RedisOperationException.class);
        assertThat(sync.getLong(whole)).isEqualTo(3L);
    }

    @Test
    void reactiveValueCounterOverloadsPreserveNumericValuesAndExpiry() {
        String whole = key("reactive-value-whole-counter");
        assertThat(reactive.incr(whole, 2L).block()).isEqualTo(2L);
        assertThat(reactive.incr(whole, 3L, 30L).block()).isEqualTo(5L);
        assertThat(reactive.incr(whole, 4L, Duration.ofSeconds(30)).block()).isEqualTo(9L);
        assertThat(reactive.decr(whole, 1L).block()).isEqualTo(8L);
        assertThat(reactive.decr(whole, 2L, 30L).block()).isEqualTo(6L);
        assertThat(reactive.decr(whole, 3L, Duration.ofSeconds(30)).block()).isEqualTo(3L);
        assertThat(reactive.getLong(whole).block()).isEqualTo(3L);
        assertTimed(whole);

        String decimal = key("reactive-value-decimal-counter");
        assertThat(reactive.incr(decimal, 1.25).block()).isEqualTo(1.25);
        assertThat(reactive.incr(decimal, 1.5, 30L).block()).isEqualTo(2.75);
        assertThat(reactive.incr(decimal, 2.0, Duration.ofSeconds(30)).block()).isEqualTo(4.75);
        assertThat(reactive.decr(decimal, 0.5).block()).isEqualTo(4.25);
        assertThat(reactive.decr(decimal, 0.25, 30L).block()).isEqualTo(4.0);
        assertThat(reactive.decr(decimal, 0.5, Duration.ofSeconds(30)).block()).isEqualTo(3.5);
        assertThat(reactive.getDouble(decimal).block()).isEqualTo(3.5);
        assertTimed(decimal);
        assertThatThrownBy(() -> reactive.incr(whole, -1L).block())
                .isInstanceOf(RedisOperationException.class);
        assertThat(reactive.getLong(whole).block()).isEqualTo(3L);
    }

    @Test
    void synchronousPushIfPresentBatchActuallyWritesEveryMember() {
        String right = key("sync-pushx-right");
        sync.lRightPush(right, "seed");
        assertThat(sync.lRightPushxAll(right, List.of("a", "b", "c"), 30L)).isEqualTo(3L);
        assertThat(sync.lRangeString(right, 0, -1)).containsExactly("seed", "a", "b", "c");
        assertThat(sync.getExpire(right)).isBetween(1L, 30L);

        String left = key("sync-pushx-left");
        sync.lLeftPush(left, "seed");
        assertThat(sync.lLeftPushxAll(left, List.of("a", "b", "c"), Duration.ofSeconds(30)))
                .isEqualTo(3L);
        assertThat(sync.lRangeString(left, 0, -1)).containsExactly("c", "b", "a", "seed");
        assertThat(sync.getExpire(left)).isBetween(1L, 30L);

        String rightDuration = key("sync-pushx-right-duration");
        sync.lRightPush(rightDuration, "seed");
        assertThat(sync.lRightPushxAll(rightDuration, List.of("a", "b"), Duration.ofSeconds(30)))
                .isEqualTo(2L);
        assertThat(sync.lRangeString(rightDuration, 0, -1)).containsExactly("seed", "a", "b");
        assertThat(sync.getExpire(rightDuration)).isBetween(1L, 30L);

        String leftSeconds = key("sync-pushx-left-seconds");
        sync.lLeftPush(leftSeconds, "seed");
        assertThat(sync.lLeftPushxAll(leftSeconds, List.of("a", "b"), 30L)).isEqualTo(2L);
        assertThat(sync.lRangeString(leftSeconds, 0, -1)).containsExactly("b", "a", "seed");
        assertThat(sync.getExpire(leftSeconds)).isBetween(1L, 30L);

        String missing = key("sync-pushx-missing");
        assertThat(sync.lRightPushxAll(missing, List.of("a", "b"), 30L)).isEqualTo(2L);
        assertThat(sync.hasKey(missing)).isFalse();
        assertThat(sync.lLeftPushxAll(leftSeconds, List.<String>of(), Duration.ofSeconds(30)))
                .isZero();
        assertThat(sync.lRangeString(leftSeconds, 0, -1)).containsExactly("b", "a", "seed");
    }

    @Test
    void batchListPopReturnsRemovedMembersAndLeavesTheRest() {
        String syncLeft = key("sync-pop-left");
        sync.lRightPushAll(syncLeft, List.of("a", "b", "c", "d"));
        assertThat(sync.lLeftPop(syncLeft, 2)).containsExactly("a", "b");
        assertThat(sync.lRangeString(syncLeft, 0, -1)).containsExactly("c", "d");

        String syncRight = key("sync-pop-right");
        sync.lRightPushAll(syncRight, List.of("a", "b", "c", "d"));
        assertThat(sync.lRightPop(syncRight, 2)).containsExactly("c", "d");
        assertThat(sync.lRangeString(syncRight, 0, -1)).containsExactly("a", "b");

        String reactiveLeft = key("reactive-pop-left");
        reactive.lRightPushAll(reactiveLeft, List.of("a", "b", "c", "d")).block();
        assertThat(reactive.lLeftPop(reactiveLeft, 2).collectList().block()).containsExactly("a", "b");
        assertThat(reactive.lRangeString(reactiveLeft, 0, -1).collectList().block())
                .containsExactly("c", "d");

        String reactiveRight = key("reactive-pop-right");
        reactive.lRightPushAll(reactiveRight, List.of("a", "b", "c", "d")).block();
        assertThat(reactive.lRightPop(reactiveRight, 2).collectList().block()).containsExactly("c", "d");
        assertThat(reactive.lRangeString(reactiveRight, 0, -1).collectList().block())
                .containsExactly("a", "b");

        assertThat(sync.lLeftPop(syncLeft, 0)).isEmpty();
        assertThat(sync.lRightPop(syncRight, 0)).isEmpty();
        assertThat(reactive.lLeftPop(reactiveLeft, 0).collectList().block()).isEmpty();
        assertThat(reactive.lRightPop(reactiveRight, 0).collectList().block()).isEmpty();
        assertThat(sync.lRangeString(syncLeft, 0, -1)).containsExactly("c", "d");
        assertThat(reactive.lRangeString(reactiveRight, 0, -1).collectList().block())
                .containsExactly("a", "b");

        assertThat(sync.lRightPop(syncRight, 10)).containsExactly("a", "b");
        assertThat(sync.hasKey(syncRight)).isFalse();
        assertThat(reactive.lLeftPop(reactiveLeft, 10).collectList().block())
                .containsExactly("c", "d");
        assertThat(reactive.hasKey(reactiveLeft).block()).isFalse();
    }

    @Test
    void hashWritesAcrossKeysRespectNxAndExpireWithoutLosingValues() {
        String first = key("hash-pipeline-a");
        String second = key("hash-pipeline-b");
        assertThat(sync.hSet(List.of(first, second), "shared", "one")).isTrue();
        assertThat(sync.hGet(first, "shared")).isEqualTo("one");
        assertThat(sync.hGet(second, "shared")).isEqualTo("one");
        assertThat(sync.hSetNX(first, "shared", "replacement")).isFalse();
        assertThat(sync.hSetNX(first, "new", "two")).isTrue();
        assertThat(sync.hSet(first, "timed", "three", 30L)).isTrue();
        assertThat(sync.hSet(second, "timed", "four", Duration.ofSeconds(30))).isTrue();
        assertThat(sync.hSize(first)).isEqualTo(3L);
        assertThat(sync.hGet(first, "shared")).isEqualTo("one");
        assertThat(sync.hGet(first, "new")).isEqualTo("two");
        assertThat(sync.getExpire(first)).isBetween(1L, 30L);
        assertThat(sync.getExpire(second)).isBetween(1L, 30L);

        String reactiveKey = key("hash-reactive-nx");
        assertThat(reactive.hSet(reactiveKey, "shared", "one", 30L).block()).isTrue();
        assertThat(reactive.hSetNX(reactiveKey, "shared", "replacement").block()).isFalse();
        assertThat(reactive.hSetNX(reactiveKey, "new", "two").block()).isTrue();
        assertThat(reactive.hSet(reactiveKey, "timed", "three", Duration.ofSeconds(30)).block()).isTrue();
        assertThat(reactive.hSize(reactiveKey).block()).isEqualTo(3L);
        assertThat(reactive.hGet(reactiveKey, "shared").block()).isEqualTo("one");
        assertThat(reactive.hGet(reactiveKey, "new").block()).isEqualTo("two");
        assertTimed(reactiveKey);
    }

    @Test
    void sortedSetTimedWritesKeepScoresAndRankOrder() {
        String key = key("zset-timed-sync");
        assertThat(sync.zAdd(key, "low", 1.0, 30L)).isTrue();
        assertThat(sync.zAdd(key, "high", 3.0, Duration.ofSeconds(30))).isTrue();
        assertThat(sync.zIncr(key, "low", 1.0, 30L)).isEqualTo(2.0);
        assertThat(sync.zIncr(key, "low", 2.0, Duration.ofSeconds(30))).isEqualTo(4.0);
        assertThat(sync.zRange(key, 0, -1)).containsExactly("high", "low");
        assertThat(sync.zScore(key, "low")).isEqualTo(4.0);
        assertThat(sync.getExpire(key)).isBetween(1L, 30L);

        String reactiveKey = key("zset-timed-reactive");
        assertThat(reactive.zAdd(reactiveKey, "low", 1.0).block()).isTrue();
        assertThat(reactive.zAdd(reactiveKey, "high", 3.0).block()).isTrue();
        assertThat(reactive.zIncr(reactiveKey, "low", 1.0, 30L).block()).isEqualTo(2.0);
        assertThat(reactive.zIncr(reactiveKey, "low", 2.0, Duration.ofSeconds(30)).block()).isEqualTo(4.0);
        assertThat(reactive.zRange(reactiveKey, Range.closed(0L, 1L)).collectList().block())
                .containsExactly("high", "low");
        assertThat(reactive.zScore(reactiveKey, "low").block()).isEqualTo(4.0);
        assertTimed(reactiveKey);
    }

    @Test
    void listPushOverloadsAndMovesPreserveOrderAndExpiry() {
        String syncKey = key("list-overloads-sync");
        assertThat(sync.lLeftPush(syncKey, "middle", 30L)).isEqualTo(1L);
        assertThat(sync.lRightPush(syncKey, "right", Duration.ofSeconds(30))).isEqualTo(2L);
        assertThat(sync.lLeftPush(syncKey, "left", Duration.ofSeconds(30))).isEqualTo(3L);
        assertThat(sync.lRightPush(syncKey, "tail", 30L)).isEqualTo(4L);
        assertThat(sync.lRangeString(syncKey, 0, -1))
                .containsExactly("left", "middle", "right", "tail");
        String syncDestination = key("list-overloads-sync-destination");
        assertThat(sync.lRightPopAndLeftPush(syncKey, syncDestination)).isEqualTo("tail");
        assertThat(sync.lRangeString(syncKey, 0, -1)).containsExactly("left", "middle", "right");
        assertThat(sync.lRangeString(syncDestination, 0, -1)).containsExactly("tail");
        assertThat(sync.getExpire(syncKey)).isBetween(1L, 30L);

        String reactiveKey = key("list-overloads-reactive");
        assertThat(reactive.lLeftPush(reactiveKey, "middle", 30L).block()).isEqualTo(1L);
        assertThat(reactive.lRightPush(reactiveKey, "right", Duration.ofSeconds(30)).block()).isEqualTo(2L);
        assertThat(reactive.lLeftPush(reactiveKey, "left", Duration.ofSeconds(30)).block()).isEqualTo(3L);
        assertThat(reactive.lRightPush(reactiveKey, "tail", 30L).block()).isEqualTo(4L);
        assertThat(reactive.lRangeString(reactiveKey, 0, -1).collectList().block())
                .containsExactly("left", "middle", "right", "tail");
        String reactiveDestination = key("list-overloads-reactive-destination");
        assertThat(reactive.lRightPopAndLeftPush(reactiveKey, reactiveDestination).block())
                .isEqualTo("tail");
        assertThat(reactive.lRangeString(reactiveKey, 0, -1).collectList().block())
                .containsExactly("left", "middle", "right");
        assertThat(reactive.lRangeString(reactiveDestination, 0, -1).collectList().block())
                .containsExactly("tail");
        assertTimed(reactiveKey);
    }

    @Test
    void listTypedIndicesAndRangesReadStoredNumbersWithoutChangingTheList() {
        String syncKey = key("list-typed-index-sync");
        assertThat(sync.lRightPushAll(syncKey, List.of("2", "4", "6"))).isEqualTo(3L);
        assertThat(sync.lIndexLong(syncKey, 0)).isEqualTo(2L);
        assertThat(sync.lIndexInteger(syncKey, 1)).isEqualTo(4);
        assertThat(sync.lIndexDouble(syncKey, -1)).isEqualTo(6.0);
        assertThat((Integer) sync.lIndexFor(syncKey, 1, value -> Integer.parseInt((String) value) * 2))
                .isEqualTo(8);
        assertThat(sync.lRangeInteger(syncKey, 0, -1)).containsExactly(2, 4, 6);
        assertThat(sync.lRangeFor(syncKey, 0, -1, value -> Integer.parseInt((String) value) + 1))
                .containsExactly(3, 5, 7);
        assertThat(sync.glIndexString(syncKey, 20, "missing")).isEqualTo("missing");
        assertThat(sync.lIndexLong(syncKey, 20, 42L)).isEqualTo(42L);
        assertThat(sync.lIndexInteger(syncKey, 20, 42)).isEqualTo(42);
        assertThat(sync.lIndexDouble(syncKey, 20, 42.0)).isEqualTo(42.0);
        assertThat(sync.lRangeString(syncKey, 0, -1)).containsExactly("2", "4", "6");

        String reactiveKey = key("list-typed-index-reactive");
        assertThat(reactive.lRightPushAll(reactiveKey, List.of("2", "4", "6")).block()).isEqualTo(3L);
        assertThat(reactive.lRangeInteger(reactiveKey, 0, -1).collectList().block())
                .containsExactly(2, 4, 6);
        assertThat(reactive.lRangeFor(reactiveKey, 0, -1,
                value -> Integer.parseInt((String) value) + 1).collectList().block())
                .containsExactly(3, 5, 7);
        assertThat(reactive.lIndex(reactiveKey, -1).block()).isEqualTo("6");
        assertThat(reactive.lRangeString(reactiveKey, 0, -1).collectList().block())
                .containsExactly("2", "4", "6");
    }

    @Test
    void hashTypedReadsAndDefaultsReflectPersistedValuesAcrossKeys() {
        String first = key("hash-typed-a");
        String second = key("hash-typed-b");
        assertThat(sync.hmSet(first, Map.of("count", "2", "ratio", "1.5"))).isTrue();
        assertThat(sync.hmSet(second, Map.of("count", "4", "ratio", "2.5"))).isTrue();
        assertThat(sync.hGetLong(first, "count")).isEqualTo(2L);
        assertThat(sync.hGetInteger(second, "count")).isEqualTo(4);
        assertThat(sync.hGetDouble(first, "ratio")).isEqualTo(1.5);
        assertThat(sync.hGetString(first, "count")).isEqualTo("2");
        assertThat(sync.hGetLong(first, "absent", 9L)).isEqualTo(9L);
        assertThat(sync.hGetInteger(first, "absent", 9)).isEqualTo(9);
        assertThat(sync.hGetDouble(first, "absent", 9.5)).isEqualTo(9.5);
        assertThat(sync.hGetString(first, "absent", "none")).isEqualTo("none");
        assertThat(sync.hGetLong(List.of(first, second), "count")).containsExactly(2L, 4L);
        assertThat(sync.hGetInteger(List.of(first, second), "count")).containsExactly(2, 4);
        assertThat(sync.hGetDouble(List.of(first, second), "ratio")).containsExactly(1.5, 2.5);
        assertThat(sync.hGetString(List.of(first, second), "count")).containsExactly("2", "4");
        assertThat(sync.hMultiGetLong(List.of(first, second), "count")).containsExactly(2L, 4L);
        assertThat(sync.hMultiGetString(List.of(first, second), "count")).containsExactly("2", "4");
        assertThat(sync.hGet(first, "count")).isEqualTo("2");

        String reactiveFirst = key("hash-typed-reactive-a");
        String reactiveSecond = key("hash-typed-reactive-b");
        assertThat(reactive.hmSet(reactiveFirst, Map.<Object, Object>of("count", "2", "ratio", "1.5")).block()).isTrue();
        assertThat(reactive.hmSet(reactiveSecond, Map.<Object, Object>of("count", "4", "ratio", "2.5")).block()).isTrue();
        assertThat(reactive.hGetLong(reactiveFirst, "count").block()).isEqualTo(2L);
        assertThat(reactive.hGetInteger(reactiveSecond, "count").block()).isEqualTo(4);
        assertThat(reactive.hGetDouble(reactiveFirst, "ratio").block()).isEqualTo(1.5);
        assertThat(reactive.hGetLong(reactiveFirst, "absent", 9L).block()).isEqualTo(9L);
        assertThat(reactive.hGetInteger(reactiveFirst, "absent", 9).block()).isEqualTo(9);
        assertThat(reactive.hGetDouble(reactiveFirst, "absent", 9.5).block()).isEqualTo(9.5);
        assertThat(reactive.hGetString(reactiveFirst, "absent", "none").block()).isEqualTo("none");
        assertThat(reactive.hGetLong(List.of(reactiveFirst, reactiveSecond), "count").collectList().block())
                .containsExactly(2L, 4L);
        assertThat(reactive.hGetInteger(List.of(reactiveFirst, reactiveSecond), "count").collectList().block())
                .containsExactly(2, 4);
        assertThat(reactive.hGetDouble(List.of(reactiveFirst, reactiveSecond), "ratio").collectList().block())
                .containsExactly(1.5, 2.5);
        assertThat(reactive.hGetString(List.of(reactiveFirst, reactiveSecond), "count").collectList().block())
                .containsExactly("2", "4");
        assertThat(reactive.hGet(reactiveFirst, "count").block()).isEqualTo("2");
    }

    @Test
    void hashBulkWritesPersistEachRequestedKeyAndField() {
        String first = key("hash-bulk-sync-a");
        String second = key("hash-bulk-sync-b");
        assertThat(sync.hMultiSet(List.of(first, second), "status", "ready")).isTrue();
        assertThat(sync.hGet(first, "status")).isEqualTo("ready");
        assertThat(sync.hGet(second, "status")).isEqualTo("ready");
        assertThat(sync.hMultiSet(List.<String>of(), "status", "ignored")).isFalse();
        assertThat(sync.hmSet(first, Map.of("count", "2"), 30L)).isTrue();
        assertThat(sync.hmSet(second, Map.of("count", "4"), Duration.ofSeconds(30))).isTrue();
        assertThat(sync.hGetLong(first, "count")).isEqualTo(2L);
        assertThat(sync.hGetLong(second, "count")).isEqualTo(4L);
        assertThat(sync.getExpire(first)).isBetween(1L, 30L);
        assertThat(sync.getExpire(second)).isBetween(1L, 30L);

        String reactiveKey = key("hash-bulk-reactive");
        assertThat(reactive.hmMultiSet(reactiveKey, List.<Object>of("a", "b"), "shared").block())
                .isTrue();
        assertThat(reactive.hGet(reactiveKey, "a").block()).isEqualTo("shared");
        assertThat(reactive.hGet(reactiveKey, "b").block()).isEqualTo("shared");
        assertThat(reactive.hmMultiSet(reactiveKey, List.<Object>of("a", "b"), "updated").block())
                .isTrue();
        assertThat(reactive.hGet(reactiveKey, "a").block()).isEqualTo("updated");
        assertThat(reactive.hGet(reactiveKey, "b").block()).isEqualTo("updated");
        assertThat(reactive.hmMultiSet(reactiveKey, List.of(), "ignored").block()).isFalse();
        assertThat(reactive.hmSet(reactiveKey, Map.<Object, Object>of("count", "2"), 30L).block())
                .isTrue();
        assertThat(reactive.hGetLong(reactiveKey, "count").block()).isEqualTo(2L);
        assertTimed(reactiveKey);
    }

    @Test
    void setAlgebraAndRemovalMatchStoredMembership() {
        String left = key("set-algebra-a");
        String right = key("set-algebra-b");
        assertThat(sync.sAdd(left, "a", "b", "c")).isEqualTo(3L);
        assertThat(sync.sAdd(right, "b", "d")).isEqualTo(2L);
        assertThat(sync.sSize(left)).isEqualTo(3L);
        assertThat(sync.sDiff(left, right)).containsExactlyInAnyOrder("a", "c");
        assertThat(sync.sIntersect(left, right)).containsExactly("b");
        assertThat(sync.sUnion(left, right)).containsExactlyInAnyOrder("a", "b", "c", "d");
        assertThat(sync.sRandomDistinct(left, 2)).hasSize(2).isSubsetOf("a", "b", "c");
        assertThat(sync.sRemove(left, "a", "c")).isEqualTo(2L);
        assertThat(sync.sGet(left)).containsExactly("b");
        assertThat(sync.sHasKey(left, "a")).isFalse();

        String reactiveLeft = key("set-algebra-reactive-a");
        String reactiveRight = key("set-algebra-reactive-b");
        assertThat(reactive.sAdd(reactiveLeft, "a", "b", "c").block()).isEqualTo(3L);
        assertThat(reactive.sAdd(reactiveRight, "b", "d").block()).isEqualTo(2L);
        assertThat(reactive.sSize(reactiveLeft).block()).isEqualTo(3L);
        assertThat(reactive.sDiff(reactiveLeft, reactiveRight).collectList().block())
                .containsExactlyInAnyOrder("a", "c");
        assertThat(reactive.sIntersect(reactiveLeft, reactiveRight).collectList().block())
                .containsExactly("b");
        assertThat(reactive.sUnion(reactiveLeft, reactiveRight).collectList().block())
                .containsExactlyInAnyOrder("a", "b", "c", "d");
        assertThat(reactive.sRandomSetDistinct(reactiveLeft, 2).collectList().block())
                .hasSize(2).isSubsetOf("a", "b", "c");
        assertThat(reactive.sRemove(reactiveLeft, "a", "c").block()).isEqualTo(2L);
        assertThat(reactive.sGet(reactiveLeft).collectList().block()).containsExactly("b");
        assertThat(reactive.sHasKey(reactiveLeft, "a").block()).isFalse();
    }

    @Test
    void timedBatchListPushesPreserveEveryMemberAndExpiry() {
        String syncLeftSeconds = key("sync-list-left-batch-seconds");
        assertThat(sync.lLeftPushAll(syncLeftSeconds, List.of("a", "b"), 30L)).isEqualTo(2L);
        assertThat(sync.lRangeString(syncLeftSeconds, 0, -1)).containsExactly("b", "a");
        assertThat(sync.getExpire(syncLeftSeconds)).isBetween(1L, 30L);
        String syncLeftDuration = key("sync-list-left-batch-duration");
        assertThat(sync.lLeftPushAll(syncLeftDuration, List.of("a", "b"), Duration.ofSeconds(30)))
                .isEqualTo(2L);
        assertThat(sync.lRangeString(syncLeftDuration, 0, -1)).containsExactly("b", "a");
        assertThat(sync.getExpire(syncLeftDuration)).isBetween(1L, 30L);
        String syncRightSeconds = key("sync-list-right-batch-seconds");
        assertThat(sync.lRightPushAll(syncRightSeconds, List.of("a", "b"), 30L)).isEqualTo(2L);
        assertThat(sync.lRangeString(syncRightSeconds, 0, -1)).containsExactly("a", "b");
        assertThat(sync.getExpire(syncRightSeconds)).isBetween(1L, 30L);
        String syncRightDuration = key("sync-list-right-batch-duration");
        assertThat(sync.lRightPushAll(syncRightDuration, List.of("a", "b"), Duration.ofSeconds(30)))
                .isEqualTo(2L);
        assertThat(sync.lRangeString(syncRightDuration, 0, -1)).containsExactly("a", "b");
        assertThat(sync.getExpire(syncRightDuration)).isBetween(1L, 30L);

        String reactiveLeftDuration = key("reactive-list-left-batch-duration");
        assertThat(reactive.lLeftPushAll(reactiveLeftDuration, List.of("a", "b"), Duration.ofSeconds(30)).block())
                .isEqualTo(2L);
        assertThat(reactive.lRangeString(reactiveLeftDuration, 0, -1).collectList().block())
                .containsExactly("b", "a");
        assertTimed(reactiveLeftDuration);
        String reactiveRightSeconds = key("reactive-list-right-batch-seconds");
        assertThat(reactive.lRightPushAll(reactiveRightSeconds, List.of("a", "b"), 30L).block())
                .isEqualTo(2L);
        assertThat(reactive.lRangeString(reactiveRightSeconds, 0, -1).collectList().block())
                .containsExactly("a", "b");
        assertTimed(reactiveRightSeconds);
    }

    @Test
    void pushIfPresentOverloadsOnlyAppendToExistingLists() {
        String syncKey = key("sync-list-pushx-single");
        assertThat(sync.lRightPush(syncKey, "seed")).isEqualTo(1L);
        assertThat(sync.lLeftPushx(syncKey, "a", 30L)).isEqualTo(2L);
        assertThat(sync.lRightPushx(syncKey, "b", 30L)).isEqualTo(3L);
        assertThat(sync.lLeftPushx(syncKey, "c", Duration.ofSeconds(30))).isEqualTo(4L);
        assertThat(sync.lRightPushx(syncKey, "d", Duration.ofSeconds(30))).isEqualTo(5L);
        assertThat(sync.lRangeString(syncKey, 0, -1)).containsExactly("c", "a", "seed", "b", "d");
        assertThat(sync.getExpire(syncKey)).isBetween(1L, 30L);

        String reactiveKey = key("reactive-list-pushx-single");
        assertThat(reactive.lRightPush(reactiveKey, "seed").block()).isEqualTo(1L);
        assertThat(reactive.lLeftPushx(reactiveKey, "a", 30L).block()).isEqualTo(2L);
        assertThat(reactive.lRightPushx(reactiveKey, "b", Duration.ofSeconds(30)).block()).isEqualTo(3L);
        assertThat(reactive.lRangeString(reactiveKey, 0, -1).collectList().block())
                .containsExactly("a", "seed", "b");
        assertTimed(reactiveKey);
    }

    @Test
    void configuredReactiveTemplateSerializesRealValuesAndHashFields() {
        RedisReactiveCachingConfiguration configuration = new RedisReactiveCachingConfiguration();
        Jackson2JsonRedisSerializer<Object> json = new Jackson2JsonRedisSerializer<>(
                new ObjectMapper(), Object.class);
        ReactiveRedisTemplate<String, Object> configured = configuration.reactiveRedisTemplate(connection, json);
        String valueKey = key("configured-reactive-value");
        assertThat(configured.opsForValue().set(valueKey, "ready").block()).isTrue();
        assertThat(configured.opsForValue().get(valueKey).block()).isEqualTo("ready");
        String hashKey = key("configured-reactive-hash");
        assertThat(configured.opsForHash().put(hashKey, "status", "ready").block()).isTrue();
        assertThat(configured.opsForHash().get(hashKey, "status").block()).isEqualTo("ready");
        try (GenericApplicationContext context = new GenericApplicationContext()) {
            context.refresh();
            ReactiveRedisOperationTemplate wrapped = configuration.reactiveRedisOperationTemplate(
                    configured, context.getBeanProvider(JsonMapperBuilderCustomizer.class));
            assertThat(wrapped.getString(valueKey).block()).isEqualTo("ready");
            assertThat(wrapped.hGetString(hashKey, "status").block()).isEqualTo("ready");
        }
    }

    @Test
    void luaInventoryCountersEnforceStockAndUpdateActualRedisValues() {
        String syncKey = key("lua-stock-sync");
        assertThat(sync.luaIncr(syncKey, 2L)).isEqualTo(-3L);
        sync.set(syncKey, "10");
        assertThat(sync.luaIncr(syncKey, 2L)).isEqualTo(12L);
        assertThat(sync.luaDecr(syncKey, 3L)).isEqualTo(9L);
        assertThat(sync.luaDecr(syncKey, 20L)).isEqualTo(-2L);
        assertThat(sync.luaDiv(syncKey, 3L)).isEqualTo(3L);
        assertThat(sync.getLong(syncKey)).isEqualTo(9L);

        String reactiveKey = key("lua-stock-reactive");
        assertThat(reactive.luaIncr(reactiveKey, 2L).block()).isEqualTo(-3L);
        reactive.set(reactiveKey, "10").block();
        assertThat(reactive.luaIncr(reactiveKey, 2L).block()).isEqualTo(12L);
        assertThat(reactive.luaDecr(reactiveKey, 3L).block()).isEqualTo(9L);
        assertThat(reactive.luaDecr(reactiveKey, 20L).block()).isEqualTo(-2L);
        assertThat(reactive.getLong(reactiveKey).block()).isEqualTo(9L);
    }

    @Test
    void luaHashInventoryCountersUpdateOnlyTheRequestedField() {
        String syncKey = key("lua-hash-sync");
        assertThat(sync.luaHincr(syncKey, "stock", 2L)).isEqualTo(-3L);
        sync.hSet(syncKey, "stock", "10");
        sync.hSet(syncKey, "other", "99");
        assertThat(sync.luaHincr(syncKey, "stock", 2L)).isEqualTo(12L);
        assertThat(sync.luaHdecr(syncKey, "stock", 3L)).isEqualTo(9L);
        assertThat(sync.luaHdiv(syncKey, "stock", 3L)).isEqualTo(3L);
        assertThat(sync.hGetLong(syncKey, "stock")).isEqualTo(9L);
        assertThat(sync.hGetLong(syncKey, "other")).isEqualTo(99L);

        String reactiveKey = key("lua-hash-reactive");
        assertThat(reactive.luaHincr(reactiveKey, "stock", 2L).block()).isEqualTo(-3L);
        reactive.hSet(reactiveKey, "stock", "10").block();
        reactive.hSet(reactiveKey, "other", "99").block();
        assertThat(reactive.luaHincr(reactiveKey, "stock", 2L).block()).isEqualTo(12L);
        assertThat(reactive.luaHdecr(reactiveKey, "stock", 3L).block()).isEqualTo(9L);
        assertThat(reactive.hGetLong(reactiveKey, "stock").block()).isEqualTo(9L);
        assertThat(reactive.hGetLong(reactiveKey, "other").block()).isEqualTo(99L);
    }

    @Test
    void luaDecimalCountersPreserveFractionalStoredValues() {
        String syncValue = key("lua-decimal-sync");
        sync.set(syncValue, "10.5");
        assertThat(sync.luaIncr(syncValue, 1.25)).isEqualTo(11.75);
        assertThat(sync.luaDecr(syncValue, 0.5)).isEqualTo(11.25);
        assertThat(sync.getDouble(syncValue)).isEqualTo(11.25);
        String syncHash = key("lua-decimal-hash-sync");
        sync.hSet(syncHash, "stock", "10.5");
        assertThat(sync.luaHincr(syncHash, "stock", 1.25)).isEqualTo(11.75);
        assertThat(sync.luaHdecr(syncHash, "stock", 0.5)).isEqualTo(11.25);
        assertThat(sync.hGetDouble(syncHash, "stock")).isEqualTo(11.25);

        String reactiveValue = key("lua-decimal-reactive");
        reactive.set(reactiveValue, "10.5").block();
        assertThat(reactive.luaIncr(reactiveValue, 1.25).block()).isEqualTo(11.75);
        assertThat(reactive.luaDecr(reactiveValue, 0.5).block()).isEqualTo(11.25);
        assertThat(reactive.getDouble(reactiveValue).block()).isEqualTo(11.25);
        String reactiveHash = key("lua-decimal-hash-reactive");
        reactive.hSet(reactiveHash, "stock", "10.5").block();
        assertThat(reactive.luaHincr(reactiveHash, "stock", 1.25).block()).isEqualTo(11.75);
        assertThat(reactive.luaHdecr(reactiveHash, "stock", 0.5).block()).isEqualTo(11.25);
        assertThat(reactive.hGetDouble(reactiveHash, "stock").block()).isEqualTo(11.25);
    }

    @Test
    void sortedSetTupleBatchesAndTimedWritesPreserveScores() {
        Set<ZSetOperations.TypedTuple<Object>> first = Set.of(
                new DefaultTypedTuple<>("low", 1.0), new DefaultTypedTuple<>("high", 3.0));
        String syncSeconds = key("zset-tuples-seconds");
        assertThat(sync.zAdd(syncSeconds, first, 30L)).isEqualTo(2L);
        assertThat(sync.zRange(syncSeconds, 0, -1)).containsExactly("low", "high");
        assertThat(sync.zScore(syncSeconds, "high")).isEqualTo(3.0);
        assertThat(sync.getExpire(syncSeconds)).isBetween(1L, 30L);

        String syncDuration = key("zset-tuples-duration");
        assertThat(sync.zAdd(syncDuration, first, Duration.ofSeconds(30))).isEqualTo(2L);
        assertThat(sync.zRange(syncDuration, 0, -1)).containsExactly("low", "high");
        assertThat(sync.zScore(syncDuration, "low")).isEqualTo(1.0);
        assertThat(sync.getExpire(syncDuration)).isBetween(1L, 30L);

        String reactiveKey = key("zset-tuples-reactive");
        assertThat(reactive.zAdd(reactiveKey, first).block()).isEqualTo(2L);
        assertThat(reactive.zRange(reactiveKey, Range.closed(0L, 1L)).collectList().block())
                .containsExactly("low", "high");
        assertThat(reactive.zScore(reactiveKey, "high").block()).isEqualTo(3.0);
        assertThat(reactive.zRevRank(reactiveKey, "high").block()).isZero();
        assertThat(reactive.zRem(reactiveKey, "low").block()).isEqualTo(1L);
        assertThat(reactive.zRange(reactiveKey, Range.closed(0L, 1L)).collectList().block())
                .containsExactly("high");
    }

    @SuppressWarnings("unchecked")
    @Test
    void configuredSynchronousTemplatesRoundTripActualRedisValues() {
        RedisCachingConfiguration configuration = new RedisCachingConfiguration();
        ObjectProvider<RedisConnectionFactory> factories = mock(ObjectProvider.class);
        when(factories.getIfAvailable()).thenReturn(connection);
        Jackson2JsonRedisSerializer<Object> json = new Jackson2JsonRedisSerializer<>(
                new ObjectMapper(), Object.class);
        ObjectProvider<Jackson2JsonRedisSerializer<Object>> serializers = mock(ObjectProvider.class);
        when(serializers.getIfAvailable()).thenReturn(json);
        ObjectProvider<JsonMapperBuilderCustomizer> customizers = mock(ObjectProvider.class);
        when(customizers.orderedStream()).thenReturn(Stream.empty());

        RedisTemplate<String, Object> configured = configuration.redisTemplate(factories, serializers);
        String valueKey = key("configured-sync-value");
        configured.opsForValue().set(valueKey, "ready");
        assertThat(configured.opsForValue().get(valueKey)).isEqualTo("ready");
        RedisOperationTemplate wrapped = configuration.redisOperationTemplate(configured, customizers);
        assertThat(wrapped.getString(valueKey)).isEqualTo("ready");
        assertThat(configuration.geoTemplate(configured)).isNotNull();

        StringRedisTemplate strings = configuration.stringRedisTemplate(factories);
        strings.afterPropertiesSet();
        String plainKey = key("configured-string-value");
        strings.opsForValue().set(plainKey, "plain");
        assertThat(strings.opsForValue().get(plainKey)).isEqualTo("plain");
    }

    @Test
    void sortedSetTypedScoreAndReverseRangesPreserveOrder() {
        String key = key("zset-range-typed");
        assertThat(sync.zAdd(key, "1", 10.0)).isTrue();
        assertThat(sync.zAdd(key, "2", 20.0)).isTrue();
        assertThat(sync.zAdd(key, "3", 30.0)).isTrue();
        assertThat(sync.zRangeInteger(key, 0, -1)).containsExactly(1, 2, 3);
        assertThat(sync.zRevrangeInteger(key, 0, -1)).containsExactly(3, 2, 1);
        assertThat(sync.zRangeStringByScore(key, 15.0, 30.0)).containsExactly("2", "3");
        assertThat(sync.zRevrangeStringByScore(key, 15.0, 30.0)).containsExactly("3", "2");
        assertThat(sync.zRangeWithScores(key, 0, -1))
                .extracting(ZSetOperations.TypedTuple::getScore).containsExactly(10.0, 20.0, 30.0);
        assertThat(sync.zRevrangeWithScores(key, 0, -1))
                .extracting(ZSetOperations.TypedTuple::getScore).containsExactly(30.0, 20.0, 10.0);
        assertThat(sync.zRevrangeByScoreWithScores(key, 15.0, 30.0))
                .extracting(ZSetOperations.TypedTuple::getScore).containsExactly(30.0, 20.0);

        Range<Long> positions = Range.closed(0L, 2L);
        Range<Double> scores = Range.closed(15.0, 30.0);
        assertThat(reactive.zRangeInteger(key, positions).collectList().block()).containsExactly(1, 2, 3);
        assertThat(reactive.zRevrangeInteger(key, positions).collectList().block()).containsExactly(3, 2, 1);
        assertThat(reactive.zRangeStringByScore(key, scores).collectList().block())
                .containsExactly("2", "3");
        assertThat(reactive.zRevrangeStringByScore(key, scores).collectList().block())
                .containsExactly("3", "2");
        assertThat(reactive.zRangeWithScores(key, positions).collectList().block())
                .extracting(ZSetOperations.TypedTuple::getScore).containsExactly(10.0, 20.0, 30.0);
        assertThat(reactive.zRevrangeWithScores(key, positions).collectList().block())
                .extracting(ZSetOperations.TypedTuple::getScore).containsExactly(30.0, 20.0, 10.0);
        assertThat(reactive.zRevrangeByScoreWithScores(key, scores).collectList().block())
                .extracting(ZSetOperations.TypedTuple::getScore).containsExactly(30.0, 20.0);
    }

    @Test
    void sortedSetUnionAndIntersectionApplyWeightsAndAggregation() {
        String left = key("zset-weight-left");
        String right = key("zset-weight-right");
        sync.zAdd(left, "a", 1.0);
        sync.zAdd(left, "b", 2.0);
        sync.zAdd(right, "b", 3.0);
        sync.zAdd(right, "c", 4.0);

        String syncMin = key("zset-union-min");
        assertThat(sync.zUnionAndStore(left, List.of(right), syncMin, Aggregate.MIN)).isEqualTo(3L);
        assertThat(sync.zScore(syncMin, "b")).isEqualTo(2.0);
        String syncWeighted = key("zset-union-weighted");
        assertThat(sync.zUnionAndStore(left, List.of(right), syncWeighted,
                Aggregate.SUM, Weights.of(2, 3))).isEqualTo(3L);
        assertThat(sync.zScore(syncWeighted, "a")).isEqualTo(2.0);
        assertThat(sync.zScore(syncWeighted, "b")).isEqualTo(13.0);
        assertThat(sync.zScore(syncWeighted, "c")).isEqualTo(12.0);
        String syncIntersection = key("zset-intersection-max");
        assertThat(sync.zIntersectAndStore(left, List.of(right), syncIntersection, Aggregate.MAX))
                .isEqualTo(1L);
        assertThat(sync.zScore(syncIntersection, "b")).isEqualTo(3.0);

        String reactiveMax = key("zset-union-reactive-max");
        assertThat(reactive.zUnionAndStore(left, List.of(right), reactiveMax, Aggregate.MAX).block())
                .isEqualTo(3L);
        assertThat(reactive.zScore(reactiveMax, "b").block()).isEqualTo(3.0);
        String reactiveWeighted = key("zset-union-reactive-weighted");
        assertThat(reactive.zUnionAndStore(left, List.of(right), reactiveWeighted,
                Aggregate.SUM, Weights.of(2, 3)).block()).isEqualTo(3L);
        assertThat(reactive.zScore(reactiveWeighted, "b").block()).isEqualTo(13.0);
        String reactiveIntersection = key("zset-intersection-reactive-min");
        assertThat(reactive.zIntersectAndStore(left, List.of(right), reactiveIntersection,
                Aggregate.MIN).block()).isEqualTo(1L);
        assertThat(reactive.zScore(reactiveIntersection, "b").block()).isEqualTo(2.0);
    }

    @Test
    void setIfAbsentTimeoutOverloadsDoNotOverwriteExistingValues() {
        String syncMillis = key("nx-sync-millis");
        assertThat(sync.setNx(syncMillis, "first", 30_000L)).isTrue();
        assertThat(sync.setNx(syncMillis, "second", 30_000L)).isFalse();
        assertThat(sync.getString(syncMillis)).isEqualTo("first");
        assertThat(sync.getExpire(syncMillis)).isBetween(1L, 30L);

        String syncUnit = key("nx-sync-unit");
        assertThat(sync.setNx(syncUnit, "first", 30L, TimeUnit.SECONDS)).isTrue();
        assertThat(sync.setNx(syncUnit, "second", 30L, TimeUnit.SECONDS)).isFalse();
        assertThat(sync.getString(syncUnit)).isEqualTo("first");
        assertThat(sync.getExpire(syncUnit)).isBetween(1L, 30L);

        String syncDuration = key("nx-sync-duration");
        assertThat(sync.setNx(syncDuration, "first", Duration.ofSeconds(30))).isTrue();
        assertThat(sync.setNx(syncDuration, "second", Duration.ofSeconds(30))).isFalse();
        assertThat(sync.getString(syncDuration)).isEqualTo("first");
        assertThat(sync.getExpire(syncDuration)).isBetween(1L, 30L);

        String reactiveMillis = key("nx-reactive-millis");
        assertThat(reactive.setNx(reactiveMillis, "first", 30_000L).block()).isTrue();
        assertThat(reactive.setNx(reactiveMillis, "second", 30_000L).block()).isFalse();
        assertThat(reactive.getString(reactiveMillis).block()).isEqualTo("first");
        assertTimed(reactiveMillis);

        String reactiveDuration = key("nx-reactive-duration");
        assertThat(reactive.setNx(reactiveDuration, "first", Duration.ofSeconds(30)).block()).isTrue();
        assertThat(reactive.setNx(reactiveDuration, "second", Duration.ofSeconds(30)).block()).isFalse();
        assertThat(reactive.getString(reactiveDuration).block()).isEqualTo("first");
        assertTimed(reactiveDuration);
    }

    @Test
    void valueBatchReadsResolvePatternsPrefixesAndTypedNumbers() {
        String prefix = key("value-batch-prefix");
        String first = RedisKey.getKeyStr(prefix, "one");
        String second = RedisKey.getKeyStr(prefix, "two");
        sync.set(first, "11");
        sync.set(second, "22");
        assertThat(sync.mGet(List.<Object>of("one", "two"), prefix)).containsExactly("11", "22");
        assertThat(reactive.mGet(List.<Object>of("one", "two"), prefix).block())
                .containsExactly("11", "22");
        String pattern = RedisKey.getKeyStr(prefix) + ":*";
        assertThat(sync.mGet(pattern)).containsExactlyInAnyOrder("11", "22");
        assertThat(reactive.mGet(pattern).block()).containsExactlyInAnyOrder("11", "22");
        assertThat(sync.mGetInteger(List.of(first, second))).containsExactly(11, 22);
        assertThat(reactive.mGetInteger(List.of(first, second)).block()).containsExactly(11, 22);
        assertThat(sync.mGetFor(List.of(first, second), value -> Integer.parseInt((String) value) + 1))
                .containsExactly(12, 23);
        assertThat(reactive.mGetFor(List.of(first, second), value -> Integer.parseInt((String) value) + 1).block())
                .containsExactly(12, 23);
        assertThat(sync.mGet(List.of())).isEmpty();
        assertThat(reactive.mGet(List.of()).block()).isNull();
    }

    @Test
    void hashWholeMapConversionsPreserveNumericKeysAndValues() {
        String hash = key("hash-map-conversions");
        assertThat(sync.hmSet(hash, Map.of("1", "10", "2", "20"))).isTrue();
        assertThat(sync.hmGetForString(hash)).containsExactlyInAnyOrderEntriesOf(
                Map.of("1", "10", "2", "20"));
        assertThat(sync.hmGetForInteger(hash)).containsExactlyInAnyOrderEntriesOf(
                Map.of(1, 10, 2, 20));
        assertThat(sync.hmGetForLong(hash)).containsExactlyInAnyOrderEntriesOf(
                Map.of(1L, 10L, 2L, 20L));
        assertThat(sync.hmGetForDouble(hash)).containsExactlyInAnyOrderEntriesOf(
                Map.of(1.0, 10.0, 2.0, 20.0));
        assertThat(sync.hmGetForString(hash, Long.class)).containsExactlyInAnyOrderEntriesOf(
                Map.of("1", 10L, "2", 20L));
        assertThat(sync.hmGetForString(hash, new TypeReference<Long>() {}))
                .containsExactlyInAnyOrderEntriesOf(Map.of("1", 10L, "2", 20L));
        assertThat(sync.hmGetFor(hash, value -> Integer.parseInt((String) value), Long.class))
                .containsExactlyInAnyOrderEntriesOf(Map.of(1, 10L, 2, 20L));
        assertThat(sync.hmGetFor(hash, value -> Integer.parseInt((String) value),
                new TypeReference<Long>() {})).containsExactlyInAnyOrderEntriesOf(
                Map.of(1, 10L, 2, 20L));
        assertThat(sync.hmGet(hash)).containsExactlyInAnyOrderEntriesOf(
                Map.of("1", "10", "2", "20"));
    }

    @Test
    void ownerTokenLockRejectsOtherOwnersAndReleasesOnlyItsOwnKey() {
        String lock = key("owner-lock");
        assertThat(sync.tryLock(lock, "owner-a", 30_000L, 0, 0L)).isTrue();
        assertThat(sync.tryLock(lock, "owner-b", 30_000L, 0, 0L)).isFalse();
        assertThat(sync.unlock(lock, "owner-b")).isFalse();
        assertThat(sync.hasKey(lock)).isTrue();
        assertThat(sync.unlock(lock, "owner-a")).isTrue();
        assertThat(sync.hasKey(lock)).isFalse();

        String reactiveLock = key("owner-lock-reactive");
        assertThat(reactive.tryLock(reactiveLock, "owner-a", 30_000L, 0, 0L)
                .block(Duration.ofSeconds(5))).isTrue();
        assertThat(reactive.tryLock(reactiveLock, "owner-b", 30_000L, 0, 0L)
                .block(Duration.ofSeconds(5))).isFalse();
        assertThat(reactive.unlock(reactiveLock, "owner-b").block(Duration.ofSeconds(5)))
                .isFalse();
        assertThat(reactive.hasKey(reactiveLock).block()).isTrue();
        assertThat(reactive.unlock(reactiveLock, "owner-a").block(Duration.ofSeconds(5)))
                .isTrue();
        assertThat(reactive.hasKey(reactiveLock).block()).isFalse();
    }

    @Test
    void simpleGlobalLocksRejectSecondAcquisitionUntilRelease() {
        String syncKey = key("simple-lock-sync");
        assertThat(sync.tryLock(syncKey, Duration.ofSeconds(30))).isTrue();
        assertThat(sync.tryLock(syncKey, 30_000L)).isFalse();
        assertThat(sync.hasKey(syncKey)).isTrue();
        assertThat(sync.unlock(syncKey)).isTrue();
        assertThat(sync.hasKey(syncKey)).isFalse();

        String reactiveKey = key("simple-lock-reactive");
        assertThat(reactive.tryLock(reactiveKey, Duration.ofSeconds(30))
                .block(Duration.ofSeconds(5))).isTrue();
        assertThat(reactive.tryLock(reactiveKey, 30_000L).block(Duration.ofSeconds(5)))
                .isFalse();
        assertThat(reactive.hasKey(reactiveKey).block()).isTrue();
        assertThat(reactive.unlock(reactiveKey).block(Duration.ofSeconds(5))).isTrue();
        assertThat(reactive.hasKey(reactiveKey).block()).isFalse();
    }

    private record WrongTypeOperation(String name, Consumer<String> invoke) {}

    @TestFactory
    Stream<DynamicTest> synchronousWrongTypeCommandsPreserveTheOriginalString() {
        return Stream.of(
                new WrongTypeOperation("list left push", key -> sync.lLeftPush(key, "a")),
                new WrongTypeOperation("list right push", key -> sync.lRightPush(key, "a")),
                new WrongTypeOperation("list range", key -> sync.lRange(key, 0, -1)),
                new WrongTypeOperation("list index", key -> sync.lIndex(key, 0)),
                new WrongTypeOperation("list left pop", key -> sync.lLeftPop(key)),
                new WrongTypeOperation("list right pop", key -> sync.lRightPop(key)),
                new WrongTypeOperation("list length", key -> sync.lSize(key)),
                new WrongTypeOperation("hash get", key -> sync.hGet(key, "field")),
                new WrongTypeOperation("hash put", key -> sync.hSet(key, "field", "value")),
                new WrongTypeOperation("hash put all", key -> sync.hmSet(key, Map.of("field", "value"))),
                new WrongTypeOperation("hash has field", key -> sync.hHasKey(key, "field")),
                new WrongTypeOperation("hash keys", key -> sync.hKeys(key)),
                new WrongTypeOperation("hash values", key -> sync.hValues(key)),
                new WrongTypeOperation("set add", key -> sync.sAdd(key, "a")),
                new WrongTypeOperation("set members", key -> sync.sGet(key)),
                new WrongTypeOperation("set has member", key -> sync.sHasKey(key, "a")),
                new WrongTypeOperation("sorted set add", key -> sync.zAdd(key, "a", 1.0)),
                new WrongTypeOperation("sorted set range", key -> sync.zRange(key, 0, -1)),
                new WrongTypeOperation("sorted set score", key -> sync.zScore(key, "a"))
        ).map(operation -> DynamicTest.dynamicTest(operation.name(), () -> {
            String key = key("wrong-type-" + operation.name().replace(' ', '-'));
            sync.set(key, "original");
            assertThatThrownBy(() -> operation.invoke().accept(key))
                    .isInstanceOf(RedisOperationException.class);
            assertThat(sync.getString(key)).isEqualTo("original");
        }));
    }

    private record JsonOperations(RedisOperationTemplate sync, ReactiveRedisOperationTemplate reactive) {}

    @SuppressWarnings("unchecked")
    private JsonOperations jsonOperations() {
        Jackson2JsonRedisSerializer<Object> json = new Jackson2JsonRedisSerializer<>(
                new ObjectMapper(), Object.class);
        ObjectProvider<RedisConnectionFactory> factories = mock(ObjectProvider.class);
        when(factories.getIfAvailable()).thenReturn(connection);
        ObjectProvider<Jackson2JsonRedisSerializer<Object>> serializers = mock(ObjectProvider.class);
        when(serializers.getIfAvailable()).thenReturn(json);
        RedisTemplate<String, Object> configured = new RedisCachingConfiguration()
                .redisTemplate(factories, serializers);
        ReactiveRedisTemplate<String, Object> configuredReactive =
                new RedisReactiveCachingConfiguration().reactiveRedisTemplate(connection, json);
        return new JsonOperations(new RedisOperationTemplate(configured, new ObjectMapper()),
                new ReactiveRedisOperationTemplate(configuredReactive, new ObjectMapper()));
    }

    @Test
    void jsonConfiguredLuaLockPreservesOwnerBytesAndSetsTtl() {
        JsonOperations operations = jsonOperations();
        String syncKey = key("json-lua-lock-sync");
        String reactiveKey = key("json-lua-lock-reactive");
        try {
            assertThat(operations.sync().tryLock(syncKey, "owner-a", 30_000L, 0, 0L)).isTrue();
            assertThat(operations.sync().getExpire(syncKey)).isBetween(1L, 30L);
            assertThat(operations.sync().unlock(syncKey, "owner-b")).isFalse();
            assertThat(operations.sync().hasKey(syncKey)).isTrue();
            assertThat(operations.sync().unlock(syncKey, "owner-a")).isTrue();

            assertThat(operations.reactive().tryLock(reactiveKey, "owner-a", 30_000L, 0, 0L)
                    .block(Duration.ofSeconds(5))).isTrue();
            assertThat(operations.reactive().getExpire(reactiveKey).block())
                    .isBetween(Duration.ofSeconds(1), Duration.ofSeconds(30));
            assertThat(operations.reactive().unlock(reactiveKey, "owner-b").block(Duration.ofSeconds(5)))
                    .isFalse();
            assertThat(operations.reactive().hasKey(reactiveKey).block()).isTrue();
            assertThat(operations.reactive().unlock(reactiveKey, "owner-a").block(Duration.ofSeconds(5)))
                    .isTrue();
        } finally {
            operations.sync().del(syncKey, reactiveKey);
        }
    }

    @Test
    void jsonConfiguredLuaCountersKeepNumericArgumentsUnquoted() {
        JsonOperations operations = jsonOperations();
        String syncKey = key("json-lua-stock-sync");
        String reactiveKey = key("json-lua-stock-reactive");
        try {
            operations.sync().set(syncKey, 10L);
            assertThat(operations.sync().luaIncr(syncKey, 2L)).isEqualTo(12L);
            assertThat(operations.sync().luaDecr(syncKey, 3L)).isEqualTo(9L);
            assertThat(operations.sync().getLong(syncKey)).isEqualTo(9L);

            operations.reactive().set(reactiveKey, 10L).block();
            assertThat(operations.reactive().luaIncr(reactiveKey, 2L).block()).isEqualTo(12L);
            assertThat(operations.reactive().luaDecr(reactiveKey, 3L).block()).isEqualTo(9L);
            assertThat(operations.reactive().getLong(reactiveKey).block()).isEqualTo(9L);

            String syncHash = key("json-lua-hash-sync");
            String reactiveHash = key("json-lua-hash-reactive");
            try {
                operations.sync().hSet(syncHash, "stock", 10L);
                assertThat(operations.sync().luaHincr(syncHash, "stock", 2L)).isEqualTo(12L);
                assertThat(operations.sync().luaHdecr(syncHash, "stock", 3L)).isEqualTo(9L);
                assertThat(operations.sync().hGetLong(syncHash, "stock")).isEqualTo(9L);

                operations.reactive().hSet(reactiveHash, "stock", "10").block();
                assertThat(operations.reactive().luaHincr(reactiveHash, "stock", 2L).block()).isEqualTo(12L);
                assertThat(operations.reactive().luaHdecr(reactiveHash, "stock", 3L).block()).isEqualTo(9L);
                assertThat(operations.reactive().hGetLong(reactiveHash, "stock").block()).isEqualTo(9L);
            } finally {
                operations.sync().del(syncHash, reactiveHash);
            }
        } finally {
            operations.sync().del(syncKey, reactiveKey);
        }
    }

    @Test
    void decimalLuaSentinelsDecodeForStringAndJsonSerializers() {
        JsonOperations json = jsonOperations();
        String stringKey = key("decimal-sentinel-string");
        String jsonKey = key("decimal-sentinel-json");
        try {
            assertThat(sync.luaIncr(stringKey, 1.25)).isEqualTo(-3.0);
            assertThat(reactive.luaIncr(stringKey, 1.25).block()).isEqualTo(-3.0);
            sync.set(stringKey, "1.5");
            assertThat(sync.luaDecr(stringKey, 2.0)).isEqualTo(-2.0);
            assertThat(reactive.luaDecr(stringKey, 0.0).block()).isEqualTo(-4.0);
            assertThat(sync.getDouble(stringKey)).isEqualTo(1.5);

            assertThat(json.sync().luaIncr(jsonKey, 1.25)).isEqualTo(-3.0);
            assertThat(json.reactive().luaIncr(jsonKey, 1.25).block()).isEqualTo(-3.0);
            json.sync().set(jsonKey, 1.5);
            assertThat(json.sync().luaDecr(jsonKey, 2.0)).isEqualTo(-2.0);
            assertThat(json.reactive().luaDecr(jsonKey, 0.0).block()).isEqualTo(-4.0);
            assertThat(json.sync().getDouble(jsonKey)).isEqualTo(1.5);
        } finally {
            sync.del(stringKey, jsonKey);
        }
    }

    @Test
    void synchronousSetAlgebraStoresTheComputedMembers() {
        String a = key("set-a"), b = key("set-b"), c = key("set-c");
        sync.sAdd(a, "a", "b");
        sync.sAdd(b, "b", "c");
        sync.sAdd(c, "b", "d");
        assertThat(sync.sDiff(a, b)).containsExactly("a");
        assertThat(sync.sIntersect(a, b)).containsExactly("b");
        assertThat(sync.sIntersect(a, List.of(b, c))).containsExactly("b");
        assertThat(sync.sIntersect(List.of(a, b, c))).containsExactly("b");
        assertThat(sync.sUnion(a, b)).containsExactlyInAnyOrder("a", "b", "c");
        assertThat(sync.sUnion(a, List.of(b, c))).containsExactlyInAnyOrder("a", "b", "c", "d");
        assertThat(sync.sUnion(List.of(a, b, c))).containsExactlyInAnyOrder("a", "b", "c", "d");

        String diff = key("set-diff"), intersection = key("set-intersection"), union = key("set-union");
        assertThat(sync.sDiffAndStore(a, b, diff)).isEqualTo(1L);
        assertThat(sync.sGet(diff)).containsExactly("a");
        assertThat(sync.sDiffAndStore(a, List.of(b, c), diff)).isEqualTo(1L);
        assertThat(sync.sDiffAndStore(List.of(a, b, c), diff)).isEqualTo(1L);
        assertThat(sync.sGet(diff)).containsExactly("a");
        assertThat(sync.sIntersectAndStore(a, b, intersection)).isEqualTo(1L);
        assertThat(sync.sIntersectAndStore(a, List.of(b, c), intersection)).isEqualTo(1L);
        assertThat(sync.sIntersectAndStore(List.of(a, b, c), intersection)).isEqualTo(1L);
        assertThat(sync.sGet(intersection)).containsExactly("b");
        assertThat(sync.sUnionAndStore(a, b, union)).isEqualTo(3L);
        assertThat(sync.sUnionAndStore(a, List.of(b, c), union)).isEqualTo(4L);
        assertThat(sync.sUnionAndStore(List.of(a, b, c), union)).isEqualTo(4L);
        assertThat(sync.sGet(union)).containsExactlyInAnyOrder("a", "b", "c", "d");
    }

    @Test
    void reactiveSetAlgebraStoresTheComputedMembers() {
        String a = key("reactive-set-a"), b = key("reactive-set-b"), c = key("reactive-set-c");
        reactive.sAdd(a, "a", "b").block();
        reactive.sAdd(b, "b", "c").block();
        reactive.sAdd(c, "b", "d").block();
        assertThat(reactive.sDiff(a, b).collectList().block()).containsExactly("a");
        assertThat(reactive.sIntersect(a, b).collectList().block()).containsExactly("b");
        assertThat(reactive.sIntersect(a, List.of(b, c)).collectList().block()).containsExactly("b");
        assertThat(reactive.sIntersect(List.of(a, b, c)).collectList().block()).containsExactly("b");
        assertThat(reactive.sUnion(a, b).collectList().block()).containsExactlyInAnyOrder("a", "b", "c");
        assertThat(reactive.sUnion(a, List.of(b, c)).collectList().block())
                .containsExactlyInAnyOrder("a", "b", "c", "d");
        assertThat(reactive.sUnion(List.of(a, b, c)).collectList().block())
                .containsExactlyInAnyOrder("a", "b", "c", "d");

        String diff = key("reactive-set-diff"), intersection = key("reactive-set-intersection");
        String union = key("reactive-set-union");
        assertThat(reactive.sDiffAndStore(a, b, diff).block()).isEqualTo(1L);
        assertThat(reactive.sGet(diff).collectList().block()).containsExactly("a");
        assertThat(reactive.sDiffAndStore(a, List.of(b, c), diff).block()).isEqualTo(1L);
        assertThat(reactive.sDiffAndStore(List.of(a, b, c), diff).block()).isEqualTo(1L);
        assertThat(reactive.sGet(diff).collectList().block()).containsExactly("a");
        assertThat(reactive.sIntersectAndStore(a, b, intersection).block()).isEqualTo(1L);
        assertThat(reactive.sIntersectAndStore(a, List.of(b, c), intersection).block()).isEqualTo(1L);
        assertThat(reactive.sIntersectAndStore(List.of(a, b, c), intersection).block()).isEqualTo(1L);
        assertThat(reactive.sGet(intersection).collectList().block()).containsExactly("b");
        assertThat(reactive.sUnionAndStore(a, b, union).block()).isEqualTo(3L);
        assertThat(reactive.sUnionAndStore(a, List.of(b, c), union).block()).isEqualTo(4L);
        assertThat(reactive.sUnionAndStore(List.of(a, b, c), union).block()).isEqualTo(4L);
        assertThat(reactive.sGet(union).collectList().block()).containsExactlyInAnyOrder("a", "b", "c", "d");
    }

    @Test
    void synchronousSortedSetAlgebraPreservesAggregatedScores() {
        String a = key("z-a"), b = key("z-b"), union = key("z-union"), intersection = key("z-intersection");
        sync.zAdd(a, "a", 1.0);
        sync.zAdd(a, "b", 2.0);
        sync.zAdd(b, "b", 3.0);
        sync.zAdd(b, "c", 4.0);
        assertThat(sync.zHas(a, "b")).isTrue();
        assertThat(sync.zHas(a, "c")).isFalse();
        assertThat(sync.zCount(a, 1.5, 2.5)).isEqualTo(1L);
        assertThat(sync.zUnionAndStore(a, b, union)).isEqualTo(3L);
        assertThat(sync.zRangeString(union, 0, -1)).containsExactly("a", "c", "b");
        assertThat(sync.zScore(union, "b")).isEqualTo(5.0);
        assertThat(sync.zIntersectAndStore(a, b, intersection)).isEqualTo(1L);
        assertThat(sync.zRangeString(intersection, 0, -1)).containsExactly("b");
        assertThat(sync.zScore(intersection, "b")).isEqualTo(5.0);
        assertThat(sync.zRem(a, "a")).isEqualTo(1L);
        assertThat(sync.zRemByScore(a, 2.0, 2.0)).isEqualTo(1L);
        assertThat(sync.zCard(a)).isZero();
    }

    @Test
    void reactiveSortedSetAlgebraPreservesAggregatedScores() {
        String a = key("rz-a"), b = key("rz-b"), union = key("rz-union"), intersection = key("rz-intersection");
        reactive.zAdd(a, "a", 1.0).block();
        reactive.zAdd(a, "b", 2.0).block();
        reactive.zAdd(b, "b", 3.0).block();
        reactive.zAdd(b, "c", 4.0).block();
        assertThat(reactive.zHas(a, "b").block()).isTrue();
        assertThat(reactive.zHas(a, "c").block()).isFalse();
        assertThat(reactive.zCount(a, Range.closed(1.5, 2.5)).block()).isEqualTo(1L);
        assertThat(reactive.zUnionAndStore(a, b, union).block()).isEqualTo(3L);
        assertThat(reactive.zRangeString(union, Range.closed(0L, -1L)).collectList().block())
                .containsExactly("a", "c", "b");
        assertThat(reactive.zScore(union, "b").block()).isEqualTo(5.0);
        assertThat(reactive.zIntersectAndStore(a, b, intersection).block()).isEqualTo(1L);
        assertThat(reactive.zRangeString(intersection, Range.closed(0L, -1L)).collectList().block())
                .containsExactly("b");
        assertThat(reactive.zScore(intersection, "b").block()).isEqualTo(5.0);
        assertThat(reactive.zRem(a, "a").block()).isEqualTo(1L);
        assertThat(reactive.zRemByScore(a, Range.closed(2.0, 2.0)).block()).isEqualTo(1L);
        assertThat(reactive.zCard(a).block()).isZero();
    }

    @Test
    void synchronousBlockingListOverloadsConsumeSeededElements() {
        String left = key("blocking-left"), right = key("blocking-right");
        sync.lRightPushAll(left, List.of("a", "b"));
        sync.lRightPushAll(right, List.of("c", "d"));
        assertThat(sync.lLeftPop(left, 1L, TimeUnit.SECONDS)).isEqualTo("a");
        assertThat(sync.lLeftPop(left, Duration.ofSeconds(1))).isEqualTo("b");
        assertThat(sync.lRightPop(right, 1L, TimeUnit.SECONDS)).isEqualTo("d");
        assertThat(sync.lRightPop(right, Duration.ofSeconds(1))).isEqualTo("c");
        assertThat(sync.hasKey(left)).isFalse();
        assertThat(sync.hasKey(right)).isFalse();

        String first = key("blocking-move-first"), second = key("blocking-move-second");
        String third = key("blocking-move-third"), destination = key("blocking-move-destination");
        sync.lRightPush(first, "one");
        sync.lRightPush(second, "two");
        sync.lRightPush(third, "three");
        assertThat(sync.lRightPopAndLeftPush(first, destination)).isEqualTo("one");
        assertThat(sync.lRightPopAndLeftPush(second, destination, 1L, TimeUnit.SECONDS)).isEqualTo("two");
        assertThat(sync.lRightPopAndLeftPush(third, destination, Duration.ofSeconds(1))).isEqualTo("three");
        assertThat(sync.lRangeString(destination, 0, -1)).containsExactly("three", "two", "one");
        assertThat(sync.hasKey(first)).isFalse();
        assertThat(sync.hasKey(second)).isFalse();
        assertThat(sync.hasKey(third)).isFalse();
    }

    @Test
    void listPopAndRemoveAlsoRemovesRemainingDuplicateValues() {
        String left = key("pop-and-remove-left"), right = key("pop-and-remove-right");
        sync.lRightPushAll(left, List.of("a", "b", "a"));
        sync.lRightPushAll(right, List.of("x", "y", "x"));
        assertThat(sync.lLeftPopAndLrem(left)).isEqualTo("a");
        assertThat(sync.lRightPopAndLrem(right)).isEqualTo("x");
        assertThat(sync.lRangeString(left, 0, -1)).containsExactly("b");
        assertThat(sync.lRangeString(right, 0, -1)).containsExactly("y");
    }

    @Test
    void reactiveLeftPushIfPresentBatchRetainsItsCountContract() {
        List<String> seed = List.of("s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9");
        String secondsKey = key("left-pushx-seconds");
        reactive.lRightPushAll(secondsKey, seed).block();
        assertThat(reactive.lLeftPushxAll(secondsKey, List.of("a", "b", "c"), 30L).block())
                .isEqualTo(3L);
        assertThat(reactive.lRangeString(secondsKey, 0, -1).collectList().block())
                .containsExactlyInAnyOrder("a", "b", "c", "s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9");
        assertTimed(secondsKey);

        String durationKey = key("left-pushx-duration");
        reactive.lRightPushAll(durationKey, seed).block();
        assertThat(reactive.lLeftPushxAll(durationKey, List.of("a", "b", "c"), Duration.ofSeconds(30)).block())
                .isEqualTo(3L);
        assertThat(reactive.lSize(durationKey).block()).isEqualTo(13L);
        assertTimed(durationKey);

        String missing = key("left-pushx-missing");
        assertThat(reactive.lLeftPushxAll(missing, List.of("a", "b", "c"), 30L).block()).isEqualTo(3L);
        assertThat(reactive.hasKey(missing).block()).isFalse();
        assertThat(reactive.lLeftPushxAll(missing, List.<String>of(), Duration.ofSeconds(30)).block()).isZero();
        assertThat(reactive.hasKey(missing).block()).isFalse();

        String badValue = key("left-pushx-later-error");
        reactive.lRightPush(badValue, "seed").block();
        assertThatThrownBy(() -> reactive.lLeftPushxAll(badValue, List.<Object>of("a", 123), 30L).block())
                .isInstanceOf(RuntimeException.class);
        assertThat(reactive.getExpire(badValue).block()).isEqualTo(Duration.ZERO);
    }

    @Test
    void synchronousTimedSetWritesAndDeletesUseRealServerState() {
        String seconds = key("set-seconds");
        assertThat(sync.sAddAndExpire(seconds, 30L, "a", "b")).isEqualTo(2L);
        assertThat(sync.sGet(seconds)).containsExactlyInAnyOrder("a", "b");
        assertThat(sync.getExpire(seconds)).isBetween(1L, 30L);

        String duration = key("set-duration");
        assertThat(sync.sAddAndExpire(duration, Duration.ofSeconds(30), "c")).isEqualTo(1L);
        assertThat(sync.sGet(duration)).containsExactly("c");
        assertThat(sync.getExpire(duration)).isBetween(1L, 30L);

        String legacy = key("set-legacy");
        assertThat(sync.sSetAndTime(legacy, 30L, "d", "e")).isEqualTo(2L);
        assertThat(sync.sGet(legacy)).containsExactlyInAnyOrder("d", "e");
        assertThat(sync.getExpire(legacy)).isBetween(1L, 30L);
        assertThat(sync.sDel(legacy)).isFalse();
        assertThat(sync.hasKey(legacy)).isFalse();
    }

    @Test
    void synchronousHashBatchOverloadsPreserveIdentityAndFieldValues() {
        String prefix = key("hash-prefix");
        String first = RedisKey.getKeyStr(prefix, "first");
        String second = RedisKey.getKeyStr(prefix, "second");
        sync.hmSet(first, Map.of("id", "first", "count", "11"));
        sync.hmSet(second, Map.of("id", "second", "count", "22"));

        assertThat(sync.hmGet(List.of("first", "second"), prefix))
                .containsExactly(Map.of("id", "first", "count", "11"),
                        Map.of("id", "second", "count", "22"));
        assertThat(sync.hmMultiGet(first, List.of("count", "missing")))
                .containsEntry("count", "11").containsEntry("missing", null);
        assertThat(sync.hmMultiGetFor(first, List.of("count", "missing"), String.class))
                .containsOnlyKeys("count").containsEntry("count", "11");
        assertThat(sync.hmMultiGet(List.of(first, second), List.of("count")))
                .containsExactly(Map.of("count", "11"), Map.of("count", "22"));
        assertThat(sync.hmMultiGet(List.of(first, second), "id", List.of("id", "count")))
                .containsKeys("first", "second");
        assertThat(sync.hGetAll(List.of(first, second)))
                .containsExactly(Map.of("id", "first", "count", "11"),
                        Map.of("id", "second", "count", "22"));
    }

    @Test
    void numericSetRandomOverloadsReturnOnlyStoredMembers() {
        String set = key("random-numeric-set");
        assertThat(sync.sAdd(set, "11", "22", "33")).isEqualTo(3L);
        assertThat(sync.sRandomString(set, 5)).hasSize(5).allMatch(Set.of("11", "22", "33")::contains);
        assertThat(sync.sRandomLong(set, 5)).hasSize(5).allMatch(Set.of(11L, 22L, 33L)::contains);
        assertThat(sync.sRandomInteger(set, 5)).hasSize(5).allMatch(Set.of(11, 22, 33)::contains);
        assertThat(sync.sRandomDouble(set, 5)).hasSize(5).allMatch(Set.of(11.0, 22.0, 33.0)::contains);
        assertThat(sync.sRandomFor(set, 5, String.class)).hasSize(5).allMatch(Set.of("11", "22", "33")::contains);
        assertThat(sync.sRandomDistinctString(set, 3)).containsExactlyInAnyOrder("11", "22", "33");
        assertThat(sync.sRandomDistinctLong(set, 3)).containsExactlyInAnyOrder(11L, 22L, 33L);
        assertThat(sync.sRandomDistinctInteger(set, 3)).containsExactlyInAnyOrder(11, 22, 33);
        assertThat(sync.sRandomDistinctDouble(set, 3)).containsExactlyInAnyOrder(11.0, 22.0, 33.0);
        assertThat(sync.sRandomDistinctFor(set, 3, String.class)).containsExactlyInAnyOrder("11", "22", "33");
        assertThat(sync.sGet(set)).containsExactlyInAnyOrder("11", "22", "33");
    }

    @Test
    void reactiveTimedSetAndDeleteOperationsExposeStoredState() {
        String set = key("reactive-set-legacy");
        assertThat(reactive.sSetAndTime(set, 30L, "one", "two").block()).isEqualTo(2L);
        assertThat(reactive.sGet(set).collectList().block()).containsExactlyInAnyOrder("one", "two");
        assertTimed(set);
        assertThat(reactive.sDel(set).block()).isTrue();
        assertThat(reactive.hasKey(set).block()).isFalse();
    }

    @Test
    void reactiveCommandsRejectNullKeysBeforeSendingRedisWrites() {
        assertThatThrownBy(() -> reactive.lLeftPush(null, "value").block())
                .isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> reactive.hGet((String) null, "field").block())
                .isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> reactive.sAdd(null, "value").block())
                .isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> reactive.zAdd(null, "value", 1.0).block())
                .isInstanceOf(RuntimeException.class);
    }

    private record InvalidKeyCommand(String name, Function<String, Publisher<?>> command) {}

    @TestFactory
    Stream<DynamicTest> reactiveListCommandsRejectNullKeyAtBoundary() {
        List<InvalidKeyCommand> commands = List.of(
                new InvalidKeyCommand("range", k -> reactive.lRange(k, 0, -1)),
                new InvalidKeyCommand("index", k -> reactive.lIndex(k, 0)),
                new InvalidKeyCommand("left push seconds", k -> reactive.lLeftPush(k, "a", 30L)),
                new InvalidKeyCommand("left push duration", k -> reactive.lLeftPush(k, "a", Duration.ofSeconds(30))),
                new InvalidKeyCommand("left push all", k -> reactive.lLeftPushAll(k, List.of("a", "b"))),
                new InvalidKeyCommand("left push all seconds", k -> reactive.lLeftPushAll(k, List.of("a"), 30L)),
                new InvalidKeyCommand("left push all duration", k -> reactive.lLeftPushAll(k, List.of("a"), Duration.ofSeconds(30))),
                new InvalidKeyCommand("left push if present", k -> reactive.lLeftPushx(k, "a")),
                new InvalidKeyCommand("left push if present seconds", k -> reactive.lLeftPushx(k, "a", 30L)),
                new InvalidKeyCommand("left push if present duration", k -> reactive.lLeftPushx(k, "a", Duration.ofSeconds(30))),
                new InvalidKeyCommand("left pop", reactive::lLeftPop),
                new InvalidKeyCommand("left pop duration", k -> reactive.lLeftPop(k, Duration.ofSeconds(1))),
                new InvalidKeyCommand("right push duration", k -> reactive.lRightPush(k, "a", Duration.ofSeconds(30))),
                new InvalidKeyCommand("right push all", k -> reactive.lRightPushAll(k, List.of("a", "b"))),
                new InvalidKeyCommand("right push all seconds", k -> reactive.lRightPushAll(k, List.of("a"), 30L)),
                new InvalidKeyCommand("right push all duration", k -> reactive.lRightPushAll(k, List.of("a"), Duration.ofSeconds(30))),
                new InvalidKeyCommand("right push if present", k -> reactive.lRightPushx(k, "a")),
                new InvalidKeyCommand("right push if present seconds", k -> reactive.lRightPushx(k, "a", 30L)),
                new InvalidKeyCommand("right push if present duration", k -> reactive.lRightPushx(k, "a", Duration.ofSeconds(30))),
                new InvalidKeyCommand("right pop", reactive::lRightPop),
                new InvalidKeyCommand("right pop duration", k -> reactive.lRightPop(k, Duration.ofSeconds(1))),
                new InvalidKeyCommand("set index", k -> reactive.lSet(k, 0, "a")),
                new InvalidKeyCommand("size", reactive::lSize),
                new InvalidKeyCommand("remove", k -> reactive.lRem(k, 1, "a")),
                new InvalidKeyCommand("trim", k -> reactive.lTrim(k, 0, 1))
        );
        return commands.stream().map(command -> DynamicTest.dynamicTest(command.name(), () ->
                assertThatThrownBy(() -> Mono.from(command.command().apply(null)).block())
                        .isInstanceOf(RedisOperationException.class)
                        .hasMessageContaining("Key must not be null")));
    }

    @TestFactory
    Stream<DynamicTest> reactiveHashCommandsRejectNullKeyAtBoundary() {
        List<InvalidKeyCommand> commands = List.of(
                new InvalidKeyCommand("decrement int", k -> reactive.hDecr(k, "count", 1)),
                new InvalidKeyCommand("decrement long", k -> reactive.hDecr(k, "count", 1L)),
                new InvalidKeyCommand("decrement double", k -> reactive.hDecr(k, "count", 1.0)),
                new InvalidKeyCommand("delete field", k -> reactive.hDel(k, "field")),
                new InvalidKeyCommand("get field", k -> reactive.hGet(k, "field")),
                new InvalidKeyCommand("has field", k -> reactive.hHasKey(k, "field")),
                new InvalidKeyCommand("get map", k -> reactive.hmGet(k)),
                new InvalidKeyCommand("get fields", k -> reactive.hMultiGet(k, List.of("field"))),
                new InvalidKeyCommand("get selected fields", k -> reactive.hmMultiGet(k, List.of("field"))),
                new InvalidKeyCommand("set map", k -> reactive.hmSet(k, Map.<Object,Object>of("field", "value"))),
                new InvalidKeyCommand("set map seconds", k -> reactive.hmSet(k, Map.<Object,Object>of("field", "value"), 30L)),
                new InvalidKeyCommand("set map duration", k -> reactive.hmSet(k, Map.<Object,Object>of("field", "value"), Duration.ofSeconds(30))),
                new InvalidKeyCommand("set field", k -> reactive.hSet(k, "field", "value")),
                new InvalidKeyCommand("set field seconds", k -> reactive.hSet(k, "field", "value", 30L)),
                new InvalidKeyCommand("set field duration", k -> reactive.hSet(k, "field", "value", Duration.ofSeconds(30))),
                new InvalidKeyCommand("set field if absent", k -> reactive.hSetNX(k, "field", "value")),
                new InvalidKeyCommand("size", reactive::hSize),
                new InvalidKeyCommand("increment int", k -> reactive.hIncr(k, "count", 1)),
                new InvalidKeyCommand("increment int seconds", k -> reactive.hIncr(k, "count", 1, 30L)),
                new InvalidKeyCommand("increment int duration", k -> reactive.hIncr(k, "count", 1, Duration.ofSeconds(30))),
                new InvalidKeyCommand("increment long", k -> reactive.hIncr(k, "count", 1L)),
                new InvalidKeyCommand("increment long seconds", k -> reactive.hIncr(k, "count", 1L, 30L)),
                new InvalidKeyCommand("increment long duration", k -> reactive.hIncr(k, "count", 1L, Duration.ofSeconds(30))),
                new InvalidKeyCommand("increment double", k -> reactive.hIncr(k, "count", 1.0)),
                new InvalidKeyCommand("increment double seconds", k -> reactive.hIncr(k, "count", 1.0, 30L)),
                new InvalidKeyCommand("increment double duration", k -> reactive.hIncr(k, "count", 1.0, Duration.ofSeconds(30))),
                new InvalidKeyCommand("field keys", reactive::hKeys)
        );
        return commands.stream().map(command -> DynamicTest.dynamicTest(command.name(), () ->
                assertThatThrownBy(() -> Mono.from(command.command().apply(null)).block())
                        .isInstanceOf(RedisOperationException.class)
                        .hasMessageContaining("Key must not be null")));
    }

    @TestFactory
    Stream<DynamicTest> reactiveSetAndSortedSetCommandsRejectNullKeyAtBoundary() {
        List<InvalidKeyCommand> commands = List.of(
                new InvalidKeyCommand("set add", k -> reactive.sAdd(k, "a")),
                new InvalidKeyCommand("set add seconds", k -> reactive.sAddAndExpire(k, 30L, "a")),
                new InvalidKeyCommand("set add duration", k -> reactive.sAddAndExpire(k, Duration.ofSeconds(30), "a")),
                new InvalidKeyCommand("set members", reactive::sGet),
                new InvalidKeyCommand("set difference", k -> reactive.sDiff(k, "other")),
                new InvalidKeyCommand("set difference store", k -> reactive.sDiffAndStore(k, "other", "target")),
                new InvalidKeyCommand("set difference collection store", k -> reactive.sDiffAndStore(k, List.of("other"), "target")),
                new InvalidKeyCommand("set contains", k -> reactive.sHasKey(k, "a")),
                new InvalidKeyCommand("set intersection", k -> reactive.sIntersect(k, "other")),
                new InvalidKeyCommand("set intersection collection", k -> reactive.sIntersect(k, List.of("other"))),
                new InvalidKeyCommand("set intersection store", k -> reactive.sIntersectAndStore(k, "other", "target")),
                new InvalidKeyCommand("set intersection collection store", k -> reactive.sIntersectAndStore(k, List.of("other"), "target")),
                new InvalidKeyCommand("set remove", k -> reactive.sRemove(k, "a")),
                new InvalidKeyCommand("set legacy timed", k -> reactive.sSetAndTime(k, 30L, "a")),
                new InvalidKeyCommand("set size", reactive::sSize),
                new InvalidKeyCommand("set union", k -> reactive.sUnion(k, "other")),
                new InvalidKeyCommand("set union collection", k -> reactive.sUnion(k, List.of("other"))),
                new InvalidKeyCommand("set union store", k -> reactive.sUnionAndStore(k, "other", "target")),
                new InvalidKeyCommand("set union collection store", k -> reactive.sUnionAndStore(k, List.of("other"), "target")),
                new InvalidKeyCommand("sorted add", k -> reactive.zAdd(k, "a", 1.0)),
                new InvalidKeyCommand("sorted size", reactive::zCard),
                new InvalidKeyCommand("sorted contains", k -> reactive.zHas(k, "a")),
                new InvalidKeyCommand("sorted count", k -> reactive.zCount(k, Range.closed(0.0, 2.0))),
                new InvalidKeyCommand("sorted increment", k -> reactive.zIncr(k, "a", 1.0)),
                new InvalidKeyCommand("sorted increment seconds", k -> reactive.zIncr(k, "a", 1.0, 30L)),
                new InvalidKeyCommand("sorted increment duration", k -> reactive.zIncr(k, "a", 1.0, Duration.ofSeconds(30))),
                new InvalidKeyCommand("sorted intersection store", k -> reactive.zIntersectAndStore(k, "other", "target")),
                new InvalidKeyCommand("sorted intersection collection store", k -> reactive.zIntersectAndStore(k, List.of("other"), "target")),
                new InvalidKeyCommand("sorted remove", k -> reactive.zRem(k, "a")),
                new InvalidKeyCommand("sorted remove score", k -> reactive.zRemByScore(k, Range.closed(0.0, 2.0))),
                new InvalidKeyCommand("sorted range", k -> reactive.zRange(k, Range.closed(0L, 1L))),
                new InvalidKeyCommand("sorted score range", k -> reactive.zRangeByScore(k, Range.closed(0.0, 2.0))),
                new InvalidKeyCommand("sorted scored range", k -> reactive.zRangeWithScores(k, Range.closed(0L, 1L))),
                new InvalidKeyCommand("sorted score and score range", k -> reactive.zRangeByScoreWithScores(k, Range.closed(0.0, 2.0))),
                new InvalidKeyCommand("sorted reverse range", k -> reactive.zRevrange(k, Range.closed(0L, 1L))),
                new InvalidKeyCommand("sorted reverse scored range", k -> reactive.zRevrangeWithScores(k, Range.closed(0L, 1L))),
                new InvalidKeyCommand("sorted reverse score range", k -> reactive.zRevrangeByScore(k, Range.closed(0.0, 2.0))),
                new InvalidKeyCommand("sorted reverse score and score range", k -> reactive.zRevrangeByScoreWithScores(k, Range.closed(0.0, 2.0))),
                new InvalidKeyCommand("sorted reverse rank", k -> reactive.zRevRank(k, "a")),
                new InvalidKeyCommand("sorted score", k -> reactive.zScore(k, "a")),
                new InvalidKeyCommand("sorted union store", k -> reactive.zUnionAndStore(k, "other", "target")),
                new InvalidKeyCommand("sorted union collection store", k -> reactive.zUnionAndStore(k, List.of("other"), "target"))
        );
        return commands.stream().map(command -> DynamicTest.dynamicTest(command.name(), () ->
                assertThatThrownBy(() -> Mono.from(command.command().apply(null)).block())
                        .isInstanceOf(RedisOperationException.class)
                        .hasMessageContaining("Key must not be null")));
    }

    @TestFactory
    Stream<DynamicTest> reactiveValueAndCounterCommandsRejectNullKeyAtBoundary() {
        List<InvalidKeyCommand> commands = List.of(
                new InvalidKeyCommand("expire duration", k -> reactive.expire(k, Duration.ofSeconds(30))),
                new InvalidKeyCommand("expire at instant", k -> reactive.expireAt(k, java.time.Instant.now().plusSeconds(30))),
                new InvalidKeyCommand("get expiry", reactive::getExpire),
                new InvalidKeyCommand("exists", reactive::hasKey),
                new InvalidKeyCommand("set", k -> reactive.set(k, "value")),
                new InvalidKeyCommand("set seconds", k -> reactive.set(k, "value", 30L)),
                new InvalidKeyCommand("set duration", k -> reactive.set(k, "value", Duration.ofSeconds(30))),
                new InvalidKeyCommand("set if absent seconds", k -> reactive.setNx(k, "value", 30L)),
                new InvalidKeyCommand("set if absent duration", k -> reactive.setNx(k, "value", Duration.ofSeconds(30))),
                new InvalidKeyCommand("increment long", k -> reactive.incr(k, 1L)),
                new InvalidKeyCommand("increment long seconds", k -> reactive.incr(k, 1L, 30L)),
                new InvalidKeyCommand("increment long duration", k -> reactive.incr(k, 1L, Duration.ofSeconds(30))),
                new InvalidKeyCommand("increment double", k -> reactive.incr(k, 1.0)),
                new InvalidKeyCommand("increment double seconds", k -> reactive.incr(k, 1.0, 30L)),
                new InvalidKeyCommand("increment double duration", k -> reactive.incr(k, 1.0, Duration.ofSeconds(30))),
                new InvalidKeyCommand("decrement long", k -> reactive.decr(k, 1L)),
                new InvalidKeyCommand("decrement long seconds", k -> reactive.decr(k, 1L, 30L)),
                new InvalidKeyCommand("decrement long duration", k -> reactive.decr(k, 1L, Duration.ofSeconds(30))),
                new InvalidKeyCommand("decrement double", k -> reactive.decr(k, 1.0)),
                new InvalidKeyCommand("decrement double seconds", k -> reactive.decr(k, 1.0, 30L)),
                new InvalidKeyCommand("decrement double duration", k -> reactive.decr(k, 1.0, Duration.ofSeconds(30)))
        );
        return commands.stream().map(command -> DynamicTest.dynamicTest(command.name(), () ->
                assertThatThrownBy(() -> Mono.from(command.command().apply(null)).block())
                        .isInstanceOf(RedisOperationException.class)
                        .hasMessageContaining("Key must not be null")));
    }

    @Test
    void reactiveSetIfAbsentRejectsNullKeyWithItsOwnValidationMessage() {
        assertThatThrownBy(() -> reactive.setNX(null, "value").block())
                .isInstanceOf(RedisOperationException.class)
                .hasMessageContaining("key must not be empty");
        assertThatThrownBy(() -> reactive.zDel(null).block())
                .isInstanceOf(RedisOperationException.class)
                .hasMessageContaining("Keys must not contain null elements");
    }

    @Test
    void reactiveHashBatchReadsExposeEveryStoredField() {
        String prefix = key("reactive-hash-prefix");
        String first = RedisKey.getKeyStr(prefix, "first");
        String second = RedisKey.getKeyStr(prefix, "second");
        reactive.hmSet(first, Map.<Object, Object>of("id", "first", "count", "11")).block();
        reactive.hmSet(second, Map.<Object, Object>of("id", "second", "count", "22")).block();

        assertThat(reactive.hGet(first, "missing", "fallback").block()).isEqualTo("fallback");
        assertThat(reactive.hGet(List.of(first, second), "count").collectList().block())
                .containsExactlyInAnyOrder("11", "22");
        assertThat(reactive.hGet(List.of("first", "second"), prefix, "count").collectList().block())
                .containsExactlyInAnyOrder("11", "22");
        assertThat(reactive.hmGet(List.of(first, second)).collectList().block())
                .extracting(Map.Entry::getValue).containsExactlyInAnyOrder("first", "second", "11", "22");
        assertThat(reactive.hmGet(List.of("first", "second"), prefix).collectList().block())
                .extracting(Map.Entry::getValue).containsExactlyInAnyOrder("first", "second", "11", "22");
        assertThat(reactive.hmMultiGet(List.of(first, second), List.<Object>of("count")).collectList().block())
                .containsExactlyInAnyOrder(Map.of("count", "11"), Map.of("count", "22"));
        assertThat(reactive.hmMultiGet(List.of(first, second), "id", List.<Object>of("id", "count")).block())
                .containsKeys("first", "second");
        assertThat(reactive.hmMultiGetAll(List.of(first, second)).collectList().block())
                .extracting(Map.Entry::getValue).containsExactlyInAnyOrder("first", "second", "11", "22");
        assertThat(reactive.hmMultiGetAll(List.of("first", "second"), prefix).collectList().block())
                .extracting(Map.Entry::getValue).containsExactlyInAnyOrder("first", "second", "11", "22");
    }

    @Test
    void reactiveListPopAndMoveOverloadsChangeStoredOrder() {
        String left = key("reactive-pop-left"), right = key("reactive-pop-right");
        reactive.lRightPushAll(left, List.of("a", "b", "a")).block();
        reactive.lRightPushAll(right, List.of("x", "y", "x")).block();
        assertThat(reactive.lLeftPopAndLrem(left).block()).isEqualTo("a");
        assertThat(reactive.lRightPopAndLrem(right).block()).isEqualTo("x");
        assertThat(reactive.lRangeString(left, 0, -1).collectList().block()).contains("b");
        assertThat(reactive.lRangeString(right, 0, -1).collectList().block()).contains("y");

        String counted = key("reactive-pop-count"), moved = key("reactive-pop-moved");
        reactive.lRightPushAll(counted, List.of("1", "2", "3")).block();
        assertThat(reactive.lLeftPop(counted, 2, String.class).collectList().block()).containsExactly("1", "2");
        assertThat(reactive.lRangeString(counted, 0, -1).collectList().block()).containsExactly("3");
        assertThat(reactive.lRightPopAndLeftPush(counted, moved, Duration.ofSeconds(1)).block()).isEqualTo("3");
        assertThat(reactive.lRangeString(moved, 0, -1).collectList().block()).containsExactly("3");
    }

    @Test
    void reactiveHyperLogLogMergeAndDeleteAffectCardinality() {
        String first = key("reactive-hll-first"), second = key("reactive-hll-second");
        String merged = key("reactive-hll-merged");
        assertThat(reactive.pfAdd(first, "a", "b").block()).isEqualTo(1L);
        assertThat(reactive.pfAdd(second, "b", "c").block()).isEqualTo(1L);
        assertThat(reactive.pfMerge(merged, first, second).block()).isTrue();
        assertThat(reactive.pfCount(merged).block()).isEqualTo(3L);
        assertThat(reactive.pfDel(merged).block()).isTrue();
        assertThat(reactive.hasKey(merged).block()).isFalse();
    }

    @Test
    void reactiveLexicalSortedRangesRespectBoundsAndLimit() {
        String sorted = key("reactive-lexical");
        reactive.zAdd(sorted, "alpha", 0.0).block();
        reactive.zAdd(sorted, "bravo", 0.0).block();
        reactive.zAdd(sorted, "charlie", 0.0).block();
        assertThat(reactive.zRangeByLex(sorted, Range.closed("alpha", "charlie")).collectList().block())
                .containsExactly("alpha", "bravo", "charlie");
        assertThat(reactive.zRangeByLex(sorted, Range.closed("alpha", "charlie"), Limit.limit().count(2))
                .collectList().block()).containsExactly("alpha", "bravo");
        assertThat(reactive.zRangeByScoreWithScores(sorted, Range.closed(0.0, 0.0)).collectList().block())
                .extracting(tuple -> tuple.getValue()).containsExactly("alpha", "bravo", "charlie");
    }

    @TestFactory
    Stream<DynamicTest> remainingReactiveCommandFamiliesRejectInvalidKeys() {
        List<InvalidKeyCommand> commands = List.of(
                new InvalidKeyCommand("delete", k -> reactive.del(k)),
                new InvalidKeyCommand("sorted lex range", k -> reactive.zRangeByLex(k, Range.closed("a", "z"))),
                new InvalidKeyCommand("sorted lex range limit", k -> reactive.zRangeByLex(k, Range.closed("a", "z"), Limit.limit().count(2))),
                new InvalidKeyCommand("hyperloglog add", k -> reactive.pfAdd(k, "a")),
                new InvalidKeyCommand("hyperloglog delete", reactive::pfDel),
                new InvalidKeyCommand("hyperloglog count", k -> reactive.pfCount(k)),
                new InvalidKeyCommand("hyperloglog merge", k -> reactive.pfMerge(k, "source")),
                new InvalidKeyCommand("set bit", k -> reactive.setBit(k, 0, true)),
                new InvalidKeyCommand("get bit", k -> reactive.getBit(k, 0)),
                new InvalidKeyCommand("move list", k -> reactive.lRightPopAndLeftPush(k, "destination")),
                new InvalidKeyCommand("move list duration", k -> reactive.lRightPopAndLeftPush(k, "destination", Duration.ofSeconds(1))),
                new InvalidKeyCommand("hash get fallback", k -> reactive.hGet(k, "field", "fallback")),
                new InvalidKeyCommand("sorted intersection weighted", k -> reactive.zIntersectAndStore(k, List.of("other"), "target", Aggregate.SUM, Weights.of(1, 1))),
                new InvalidKeyCommand("sorted union weighted", k -> reactive.zUnionAndStore(k, List.of("other"), "target", Aggregate.SUM, Weights.of(1, 1)))
        );
        return commands.stream().map(command -> DynamicTest.dynamicTest(command.name(), () ->
                assertThatThrownBy(() -> Mono.from(command.command().apply(null)).block())
                        .isInstanceOf(RuntimeException.class)));
    }

    @Test
    void reactiveTypedSetAndSortedRangesConvertStoredNumericMembers() {
        String set = key("reactive-typed-numeric-set");
        reactive.sAdd(set, "1", "2").block();
        assertThat(reactive.sGetString(set).collectList().block()).containsExactlyInAnyOrder("1", "2");
        assertThat(reactive.sGetDouble(set).collectList().block()).containsExactlyInAnyOrder(1.0, 2.0);
        assertThat(reactive.sGetInteger(set).collectList().block()).containsExactlyInAnyOrder(1, 2);
        assertThat(reactive.sGetFor(set, String.class).collectList().block()).containsExactlyInAnyOrder("1", "2");

        String sorted = key("reactive-typed-numeric-sorted");
        reactive.zAdd(sorted, "1", 1.0).block();
        reactive.zAdd(sorted, "2", 2.0).block();
        Range<Long> ranks = Range.closed(0L, 1L);
        Range<Double> scores = Range.closed(1.0, 2.0);
        assertThat(reactive.zRangeDouble(sorted, ranks).collectList().block()).containsExactly(1.0, 2.0);
        assertThat(reactive.zRangeFor(sorted, ranks, String.class).collectList().block()).containsExactly("1", "2");
        assertThat(reactive.zRangeLongByScore(sorted, scores).collectList().block()).containsExactly(1L, 2L);
        assertThat(reactive.zRangeIntegerByScore(sorted, scores).collectList().block()).containsExactly(1, 2);
        assertThat(reactive.zRangeByScoreFor(sorted, scores, String.class).collectList().block()).containsExactly("1", "2");
        assertThat(reactive.zRevrangeString(sorted, ranks).collectList().block()).containsExactly("2", "1");
        assertThat(reactive.zRevrangeDouble(sorted, ranks).collectList().block()).containsExactly(2.0, 1.0);
        assertThat(reactive.zRevrangeLong(sorted, ranks).collectList().block()).containsExactly(2L, 1L);
        assertThat(reactive.zRevrangeInteger(sorted, ranks).collectList().block()).containsExactly(2, 1);
        assertThat(reactive.zRevrangeFor(sorted, ranks, String.class).collectList().block()).containsExactly("2", "1");
        assertThat(reactive.zRevrangeDoubleByScore(sorted, scores).collectList().block()).containsExactly(2.0, 1.0);
        assertThat(reactive.zRevrangeLongByScore(sorted, scores).collectList().block()).containsExactly(2L, 1L);
        assertThat(reactive.zRevrangeIntegerByScore(sorted, scores).collectList().block()).containsExactly(2, 1);
        assertThat(reactive.zRevrangeForByScore(sorted, scores, String.class).collectList().block())
                .containsExactly("2", "1");
    }

    @Test
    void reactiveKeyPrefixDiscoveryReturnsOnlyMatchingStoredKeys() {
        String prefix = key("reactive-prefix");
        String first = prefix + ":first", second = prefix + ":second";
        reactive.set(first, "a").block();
        reactive.set(second, "b").block();
        assertThat(reactive.getValueKeyByPrefix(prefix).collectList().block())
                .containsExactlyInAnyOrder(first, second);
    }

    @Test
    void reactiveSerializerHelpersRoundTripKeysValuesAndHashFields() {
        ByteBuffer keyBytes = reactive.getRawKey("test:key");
        ByteBuffer valueBytes = reactive.getRawValue("value");
        ByteBuffer hashKeyBytes = reactive.getRawHashKey("field");
        ByteBuffer hashValueBytes = reactive.getRawHashValue("hash-value");
        assertThat(reactive.getObjectMapper()).isNotNull();
        assertThat(reactive.getDeserializeKey(keyBytes)).isEqualTo("test:key");
        assertThat(reactive.getDeserializeKeys(Set.of(keyBytes))).containsExactly("test:key");
        assertThat(reactive.getDeserializeValue(valueBytes)).isEqualTo("value");
        assertThat(reactive.getDeserializeString(valueBytes)).isEqualTo("value");
        assertThat(reactive.getDeserializeValues(reactive.getRawValues(List.of("a", "b"))))
                .containsExactly("a", "b");
        assertThat(reactive.getDeserializeValues(Set.of(valueBytes))).containsExactly("value");
        assertThat((String) reactive.getDeserializeHashKey(hashKeyBytes)).isEqualTo("field");
        assertThat((String) reactive.getDeserializeHashValue(hashValueBytes)).isEqualTo("hash-value");
        assertThat(reactive.<String>getDeserializeHashKeys(Set.of(hashKeyBytes))).containsExactly("field");
        assertThat(reactive.<String>getDeserializeHashValues(List.of(hashValueBytes))).containsExactly("hash-value");
        assertThat(reactive.getRawHashKeys("field")).hasSize(1);
        assertThat(reactive.getRawValue(valueBytes)).isSameAs(valueBytes);
    }

    @Test
    void reactiveServerMetadataReadsFromTheOwnedRedisProcess() {
        long now = reactive.timeNow().block();
        assertThat(now).isPositive();
        assertThat(reactive.period(now + 10_000).block()).isBetween(1L, 10_000L);
        assertThat(reactive.dbSize().block()).isNotNegative();
        assertThat(reactive.lastSave().block()).isNotNegative();
    }

    @Test
    void reactiveVoidHashScanAndDeleteActuallyRun() {
        String hash = key("reactive-void-hash");
        reactive.hmSet(hash, Map.<Object, Object>of("a", "1", "b", "2")).block();
        List<Object> fields = new ArrayList<>();
        reactive.hScan(hash, signal -> {
            if (signal.isOnNext()) fields.add(signal.get().getKey());
        });
        assertThat(fields).containsExactlyInAnyOrder("a", "b");

        List<Object> matched = new ArrayList<>();
        reactive.hScan(hash, "a", signal -> {
            if (signal.isOnNext()) matched.add(signal.get().getKey());
        });
        assertThat(matched).containsExactly("a");
        reactive.hDel(hash);
        assertThat(reactive.hasKey(hash).block()).isFalse();
    }

    @Test
    void reactiveVoidSetAndSortedScansVisitStoredMembers() {
        String set = key("reactive-void-set");
        reactive.sAdd(set, "a", "b").block();
        List<Object> setMembers = new ArrayList<>();
        reactive.sScan(set, "a", signal -> {
            if (signal.isOnNext()) setMembers.add(reactive.getDeserializeValue(signal.get()));
        });
        assertThat(setMembers).containsExactly("a");

        String sorted = key("reactive-void-sorted");
        reactive.zAdd(sorted, "a", 1.0).block();
        reactive.zAdd(sorted, "b", 2.0).block();
        List<Object> members = new ArrayList<>();
        reactive.zScan(sorted, signal -> {
            if (signal.isOnNext()) members.add(signal.get().getValue());
        });
        assertThat(members).containsExactlyInAnyOrder("a", "b");
        List<Object> matched = new ArrayList<>();
        reactive.zScan(sorted, "b", signal -> {
            if (signal.isOnNext()) matched.add(signal.get().getValue());
        });
        assertThat(matched).containsExactly("b");
        assertThat(reactive.zDel(sorted).block()).isTrue();
        assertThat(reactive.hasKey(sorted).block()).isFalse();
    }

    @Test
    void reactiveVoidCommandsRejectNonBlockingThreadsBeforeRedisChanges() {
        String hash = key("guarded-hash"), set = key("guarded-set"), sorted = key("guarded-sorted");
        reactive.hSet(hash, "field", "value").block();
        reactive.sAdd(set, "member").block();
        reactive.zAdd(sorted, "member", 1.0).block();
        AtomicInteger callbacks = new AtomicInteger();
        List<Runnable> commands = List.of(
                () -> reactive.hDel(hash),
                () -> reactive.hScan(hash, signal -> callbacks.incrementAndGet()),
                () -> reactive.hScan(hash, "*", signal -> callbacks.incrementAndGet()),
                () -> reactive.sScan(set, signal -> callbacks.incrementAndGet()),
                () -> reactive.sScan(set, "*", signal -> callbacks.incrementAndGet()),
                () -> reactive.zScan(sorted, signal -> callbacks.incrementAndGet()),
                () -> reactive.zScan(sorted, "*", signal -> callbacks.incrementAndGet())
        );
        for (Runnable command : commands) {
            assertThatThrownBy(() -> Mono.fromRunnable(command).subscribeOn(Schedulers.parallel()).block())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("blocking-capable thread");
        }
        assertThat(callbacks.get()).isZero();
        assertThat(reactive.hGet(hash, "field").block()).isEqualTo("value");
        assertThat(reactive.sGet(set).collectList().block()).containsExactly("member");
        assertThat(reactive.zScore(sorted, "member").block()).isEqualTo(1.0);
    }

    @Test
    void reactiveVoidScanDeliversTerminalSignalAndPropagatesConsumerFailure() {
        String missing = key("empty-scan");
        AtomicInteger completed = new AtomicInteger();
        reactive.hScan(missing, signal -> {
            if (signal.isOnComplete()) completed.incrementAndGet();
        });
        assertThat(completed.get()).isEqualTo(1);

        String hash = key("failing-scan");
        reactive.hSet(hash, "field", "value").block();
        assertThatThrownBy(() -> reactive.hScan(hash, signal -> {
            if (signal.isOnNext()) throw new IllegalStateException("consumer rejected");
        })).isInstanceOf(IllegalStateException.class).hasMessageContaining("consumer rejected");
        assertThat(reactive.hGet(hash, "field").block()).isEqualTo("value");
    }

    @TestFactory
    Stream<DynamicTest> reactiveCountersRejectNegativeFactorsWithoutCreatingKeys() {
        List<InvalidKeyCommand> commands = List.of(
                new InvalidKeyCommand("increment long", k -> reactive.incr(k, -1L)),
                new InvalidKeyCommand("increment long seconds", k -> reactive.incr(k, -1L, 30L)),
                new InvalidKeyCommand("increment long duration", k -> reactive.incr(k, -1L, Duration.ofSeconds(30))),
                new InvalidKeyCommand("increment double", k -> reactive.incr(k, -1.0)),
                new InvalidKeyCommand("increment double seconds", k -> reactive.incr(k, -1.0, 30L)),
                new InvalidKeyCommand("increment double duration", k -> reactive.incr(k, -1.0, Duration.ofSeconds(30))),
                new InvalidKeyCommand("decrement long", k -> reactive.decr(k, -1L)),
                new InvalidKeyCommand("decrement long seconds", k -> reactive.decr(k, -1L, 30L)),
                new InvalidKeyCommand("decrement long duration", k -> reactive.decr(k, -1L, Duration.ofSeconds(30))),
                new InvalidKeyCommand("decrement double", k -> reactive.decr(k, -1.0)),
                new InvalidKeyCommand("decrement double seconds", k -> reactive.decr(k, -1.0, 30L)),
                new InvalidKeyCommand("decrement double duration", k -> reactive.decr(k, -1.0, Duration.ofSeconds(30))),
                new InvalidKeyCommand("hash increment int", k -> reactive.hIncr(k, "field", -1)),
                new InvalidKeyCommand("hash increment int seconds", k -> reactive.hIncr(k, "field", -1, 30L)),
                new InvalidKeyCommand("hash increment int duration", k -> reactive.hIncr(k, "field", -1, Duration.ofSeconds(30))),
                new InvalidKeyCommand("hash increment long", k -> reactive.hIncr(k, "field", -1L)),
                new InvalidKeyCommand("hash increment long seconds", k -> reactive.hIncr(k, "field", -1L, 30L)),
                new InvalidKeyCommand("hash increment long duration", k -> reactive.hIncr(k, "field", -1L, Duration.ofSeconds(30))),
                new InvalidKeyCommand("hash increment double", k -> reactive.hIncr(k, "field", -1.0)),
                new InvalidKeyCommand("hash increment double seconds", k -> reactive.hIncr(k, "field", -1.0, 30L)),
                new InvalidKeyCommand("hash increment double duration", k -> reactive.hIncr(k, "field", -1.0, Duration.ofSeconds(30))),
                new InvalidKeyCommand("hash decrement int", k -> reactive.hDecr(k, "field", -1)),
                new InvalidKeyCommand("hash decrement long", k -> reactive.hDecr(k, "field", -1L)),
                new InvalidKeyCommand("hash decrement double", k -> reactive.hDecr(k, "field", -1.0))
        );
        return commands.stream().map(command -> DynamicTest.dynamicTest(command.name(), () -> {
            String key = key("negative-factor");
            assertThatThrownBy(() -> Mono.from(command.command().apply(key)).block())
                    .isInstanceOf(RedisOperationException.class).hasMessageContaining("必须>=0");
            assertThat(reactive.hasKey(key).block()).isFalse();
        }));
    }

    @Test
    void reactiveEmptyInputsAndInvalidExpiryDoNotCreateKeys() {
        String key = key("reactive-empty-inputs");
        assertThat(reactive.expire(key, (Duration) null).block()).isFalse();
        assertThat(reactive.expireAt(key, (java.time.Instant) null).block()).isFalse();
        assertThat(reactive.set(key, "value", (Duration) null).block()).isFalse();
        assertThat(reactive.set(key, "value", Duration.ofSeconds(-1)).block()).isFalse();
        assertThat(reactive.hasKey(key).block()).isFalse();
        assertThat(reactive.getKey(null).collectList().block()).isEmpty();
        assertThat(reactive.mGet((String) null).block()).isNull();
        assertThat(reactive.mGet(List.<String>of()).block()).isNull();
        assertThat(reactive.mGet(List.<Object>of(), "prefix").block()).isNull();
        assertThat(reactive.del().block()).isZero();
        assertThat(reactive.lLeftPop(key, 0).collectList().block()).isEmpty();
        assertThat(reactive.hmGet(List.<String>of()).collectList().block()).isEmpty();
        assertThat(reactive.hmGet(List.<String>of(), "prefix").collectList().block()).isEmpty();
        assertThat(reactive.hmMultiGet(List.<String>of(), List.<Object>of("field")).collectList().block()).isEmpty();
        assertThat(reactive.hmMultiGet(List.<String>of(), "id", List.<Object>of("field")).block()).isNull();
        assertThat(reactive.hmMultiGetAll(List.<String>of()).collectList().block()).isEmpty();
        assertThat(reactive.hmMultiGetAll(List.<String>of(), "prefix").collectList().block()).isEmpty();
        assertThat(reactive.hmMultiSet(key, List.<Object>of(), "value").block()).isFalse();
        assertThat(reactive.hasKey(key).block()).isFalse();
    }

    @Test
    void reactiveCollectionListPushOverloadsPreserveOrderAndExpiry() {
        String left = key("reactive-list-collection-left");
        assertThat(reactive.lLeftPush(left, List.of("a", "b"), 30L).block()).isEqualTo(2L);
        assertThat(reactive.lRangeString(left, 0, -1).collectList().block()).containsExactly("b", "a");
        assertTimed(left);
        assertThat(reactive.lLeftPushx(left, List.of("c", "d"), Duration.ofSeconds(30)).block()).isEqualTo(2L);
        assertThat(reactive.lRangeString(left, 0, -1).collectList().block()).containsExactly("d", "c", "b", "a");

        String right = key("reactive-list-collection-right");
        assertThat(reactive.lRightPush(right, List.of("a", "b"), Duration.ofSeconds(30)).block()).isEqualTo(2L);
        assertThat(reactive.lRangeString(right, 0, -1).collectList().block()).containsExactly("a", "b");
        assertTimed(right);
        assertThat(reactive.lRightPushx(right, List.of("c", "d"), 30L).block()).isEqualTo(3L);
        assertThat(reactive.lRangeString(right, 0, -1).collectList().block()).containsExactly("a", "b", "c", "d");
    }

    @Test
    void reactiveTypedClassConversionsAndTupleDecodingPreserveValues() {
        String value = key("reactive-class-value");
        reactive.set(value, "12").block();
        assertThat(reactive.getFor(value, String.class).block()).isEqualTo("12");
        assertThat(reactive.mGetFor(List.of(value), String.class).block()).containsExactly("12");

        String list = key("reactive-class-list");
        reactive.lRightPushAll(list, List.of("a", "b")).block();
        assertThat(reactive.lRangeFor(list, 0, -1, String.class).collectList().block())
                .containsExactly("a", "b");

        String first = key("reactive-class-hash-first"), second = key("reactive-class-hash-second");
        reactive.hSet(first, "field", "a").block();
        reactive.hSet(second, "field", "b").block();
        assertThat(reactive.hGetFor(first, "field", String.class).block()).isEqualTo("a");
        assertThat(reactive.hGetFor(List.of(first, second), "field", String.class).collectList().block())
                .containsExactlyInAnyOrder("a", "b");

        var tuple = reactive.getDeserializeTuple(new DefaultTuple("member".getBytes(java.nio.charset.StandardCharsets.UTF_8), 2.5));
        assertThat(tuple.getValue()).isEqualTo("member");
        assertThat(tuple.getScore()).isEqualTo(2.5);
    }

    @Test
    void reactiveLuaResourceAndScriptObjectsReadStoredValue() {
        String key = key("reactive-lua-resource");
        reactive.set(key, "stored").block();
        String source = "return redis.call('GET', KEYS[1])";
        assertThat(reactive.executeLuaScript(new ByteArrayResource(source.getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                String.class, List.of(key)).block()).isEqualTo("stored");
        assertThat(reactive.executeLuaScript(RedisScript.of(source, String.class), List.of(key)).block())
                .isEqualTo("stored");
    }

    @Test
    void reactivePublishAndSubscribeDeliversMessageOnOwnedRedis() throws Exception {
        String channel = key("reactive-channel");
        var received = CompletableFuture.supplyAsync(() ->
                reactive.listenTo(new ChannelTopic(channel)).next().block(Duration.ofSeconds(3)));
        long subscribers = 0;
        for (int attempt = 0; attempt < 30 && subscribers == 0; attempt++) {
            subscribers = reactive.sendMessage(channel, "payload").block();
            if (subscribers == 0) Thread.sleep(30);
        }
        assertThat(subscribers).isPositive();
        assertThat(received.get(3, TimeUnit.SECONDS).getMessage()).isEqualTo("payload");
    }

    @Test
    void reactiveLegacyLockCommandsReleaseOnlyTheirTestKeys() {
        String simple = key("reactive-simple-lock");
        assertThat(reactive.tryLock(simple, 1_000L).block()).isTrue();
        assertThat(reactive.tryLock(simple, Duration.ofSeconds(1)).block()).isFalse();
        assertThat(reactive.unlock(simple).block()).isTrue();
        assertThat(reactive.hasKey(simple).block()).isFalse();

        String blocking = key("reactive-blocking-lock");
        assertThat(reactive.tryBlockLock(blocking, 1).block()).isTrue();
        assertThat(reactive.tryBlockLock(blocking, 1).block()).isFalse();
        assertThat(reactive.unBlockLock(blocking, "request-1").block()).isTrue();
        assertThat(reactive.hasKey(blocking).block()).isFalse();
        assertThat(reactive.lRangeString(blocking + ":list", 0, -1).collectList().block())
                .containsExactly("request-1");
    }

    @TestFactory
    Stream<DynamicTest> reactiveCommandsRejectNullCollectionsWithoutChangingRedis() {
        List<InvalidKeyCommand> commands = List.of(
                new InvalidKeyCommand("left push all", k -> reactive.lLeftPushAll(k, null)),
                new InvalidKeyCommand("left push all seconds", k -> reactive.lLeftPushAll(k, null, 30L)),
                new InvalidKeyCommand("left push all duration", k -> reactive.lLeftPushAll(k, null, Duration.ofSeconds(30))),
                new InvalidKeyCommand("left push if present all seconds", k -> reactive.lLeftPushxAll(k, null, 30L)),
                new InvalidKeyCommand("left push if present all duration", k -> reactive.lLeftPushxAll(k, null, Duration.ofSeconds(30))),
                new InvalidKeyCommand("right push all", k -> reactive.lRightPushAll(k, null)),
                new InvalidKeyCommand("right push all seconds", k -> reactive.lRightPushAll(k, null, 30L)),
                new InvalidKeyCommand("right push all duration", k -> reactive.lRightPushAll(k, null, Duration.ofSeconds(30))),
                new InvalidKeyCommand("hash delete fields", k -> reactive.hDel(k, (Object[]) null)),
                new InvalidKeyCommand("hash get fields", k -> reactive.hMultiGet(k, null)),
                new InvalidKeyCommand("hash map get fields", k -> reactive.hmMultiGet(k, null)),
                new InvalidKeyCommand("hash map set", k -> reactive.hmSet(k, null)),
                new InvalidKeyCommand("sorted add tuples", k -> reactive.zAdd(k, (Set<ZSetOperations.TypedTuple<Object>>) null)),
                new InvalidKeyCommand("sorted range", k -> reactive.zRange(k, null)),
                new InvalidKeyCommand("sorted range by score", k -> reactive.zRangeByScore(k, null)),
                new InvalidKeyCommand("sorted range by lex", k -> reactive.zRangeByLex(k, null)),
                new InvalidKeyCommand("sorted reverse range", k -> reactive.zRevrange(k, null)),
                new InvalidKeyCommand("sorted reverse score range", k -> reactive.zRevrangeByScore(k, null))
        );
        return commands.stream().map(command -> DynamicTest.dynamicTest(command.name(), () -> {
            String key = key("null-collection");
            assertThatThrownBy(() -> Mono.from(command.command().apply(key)).block())
                    .isInstanceOf(RuntimeException.class);
            assertThat(reactive.hasKey(key).block()).isFalse();
        }));
    }

    @Test
    @SuppressWarnings("unchecked")
    void reactiveCommandFamiliesNormalizeImmediateClientSetupFailures() {
        ReactiveRedisTemplate<String, Object> unavailable = mock(ReactiveRedisTemplate.class,
                invocation -> { throw new IllegalStateException("backend unavailable"); });
        ReactiveRedisOperationTemplate failed = new ReactiveRedisOperationTemplate(unavailable, new ObjectMapper());
        String key = key("unavailable-client");
        List<InvalidKeyCommand> commands = List.of(
                new InvalidKeyCommand("key lookup", k -> failed.getKey(k + "*")),
                new InvalidKeyCommand("vague key lookup", failed::getVagueKey),
                new InvalidKeyCommand("prefix lookup", failed::getValueKeyByPrefix),
                new InvalidKeyCommand("get", failed::get),
                new InvalidKeyCommand("pattern multi get", k -> failed.mGet(k + "*")),
                new InvalidKeyCommand("collection multi get", k -> failed.mGet(List.of(k))),
                new InvalidKeyCommand("prefixed multi get", k -> failed.mGet(List.<Object>of("one"), k)),
                new InvalidKeyCommand("blocking left pop", k -> failed.lLeftPop(k, Duration.ofSeconds(1))),
                new InvalidKeyCommand("blocking right pop", k -> failed.lRightPop(k, Duration.ofSeconds(1))),
                new InvalidKeyCommand("left push distinct", k -> failed.lLeftPushDistinct(k, "a")),
                new InvalidKeyCommand("left pop and remove", failed::lLeftPopAndLrem),
                new InvalidKeyCommand("right push distinct", k -> failed.lRightPushDistinct(k, "a")),
                new InvalidKeyCommand("right pop and remove", failed::lRightPopAndLrem),
                new InvalidKeyCommand("right push seconds", k -> failed.lRightPush(k, "a", 30L)),
                new InvalidKeyCommand("random set member", k -> failed.sRandomSet(k, 1)),
                new InvalidKeyCommand("distinct random set member", k -> failed.sRandomSetDistinct(k, 1)),
                new InvalidKeyCommand("set difference collection store", k -> failed.sDiffAndStore(List.of(k, "other"), "target")),
                new InvalidKeyCommand("set intersection collection", k -> failed.sIntersect(List.of(k, "other"))),
                new InvalidKeyCommand("set intersection collection store", k -> failed.sIntersectAndStore(List.of(k, "other"), "target")),
                new InvalidKeyCommand("set union collection", k -> failed.sUnion(List.of(k, "other"))),
                new InvalidKeyCommand("set union collection store", k -> failed.sUnionAndStore(List.of(k, "other"), "target")),
                new InvalidKeyCommand("sorted intersection", k -> failed.zIntersectAndStore(k, List.of("other"), "target")),
                new InvalidKeyCommand("sorted union", k -> failed.zUnionAndStore(k, List.of("other"), "target")),
                new InvalidKeyCommand("sorted weighted intersection", k -> failed.zIntersectAndStore(
                        k, List.of("other"), "target", Aggregate.SUM, Weights.of(1, 1))),
                new InvalidKeyCommand("sorted weighted union", k -> failed.zUnionAndStore(
                        k, List.of("other"), "target", Aggregate.SUM, Weights.of(1, 1))),
                new InvalidKeyCommand("publish", k -> failed.sendMessage(k, "payload")),
                new InvalidKeyCommand("subscribe", k -> failed.listenTo(new ChannelTopic(k))),
                new InvalidKeyCommand("unblock lock", k -> failed.unBlockLock(k, "owner")),
                new InvalidKeyCommand("delete set", failed::sDel),
                new InvalidKeyCommand("delete sorted set", failed::zDel),
                new InvalidKeyCommand("Lua string", k -> failed.executeLuaScript("return 1", Long.class, List.of(k))),
                new InvalidKeyCommand("Lua object", k -> failed.executeLuaScript(RedisScript.of("return 1", Long.class), List.of(k))),
                new InvalidKeyCommand("Lua resource", k -> failed.executeLuaScript(
                        new ByteArrayResource("return 1".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                        Long.class, List.of(k)))
        );
        for (InvalidKeyCommand command : commands) {
            assertThatThrownBy(() -> Mono.from(command.command().apply(key)).block())
                    .as(command.name())
                    .isInstanceOf(RedisOperationException.class)
                    .hasMessageContaining("backend unavailable");
        }
    }

    private record InvalidSyncKeyCommand(String name, Consumer<String> command) {}

    @TestFactory
    Stream<DynamicTest> synchronousKeyValueAndCounterCommandsRejectNullKeys() {
        List<InvalidSyncKeyCommand> commands = List.of(
                new InvalidSyncKeyCommand("has key", sync::hasKey),
                new InvalidSyncKeyCommand("expire seconds", k -> sync.expire(k, 30L)),
                new InvalidSyncKeyCommand("expire duration", k -> sync.expire(k, Duration.ofSeconds(30))),
                new InvalidSyncKeyCommand("expire at", k -> sync.expireAt(k, new Date(System.currentTimeMillis() + 30_000))),
                new InvalidSyncKeyCommand("get expiry", sync::getExpire),
                new InvalidSyncKeyCommand("get expiry units", k -> sync.getExpire(k, TimeUnit.SECONDS)),
                new InvalidSyncKeyCommand("set", k -> sync.set(k, "value")),
                new InvalidSyncKeyCommand("set seconds", k -> sync.set(k, "value", 30L)),
                new InvalidSyncKeyCommand("set duration", k -> sync.set(k, "value", Duration.ofSeconds(30))),
                new InvalidSyncKeyCommand("set if absent", k -> sync.setNx(k, "value")),
                new InvalidSyncKeyCommand("set if absent millis", k -> sync.setNx(k, "value", 30_000L)),
                new InvalidSyncKeyCommand("set if absent units", k -> sync.setNx(k, "value", 30L, TimeUnit.SECONDS)),
                new InvalidSyncKeyCommand("set if absent duration", k -> sync.setNx(k, "value", Duration.ofSeconds(30))),
                new InvalidSyncKeyCommand("get", sync::get),
                new InvalidSyncKeyCommand("increment long", k -> sync.incr(k, 1L)),
                new InvalidSyncKeyCommand("increment long seconds", k -> sync.incr(k, 1L, 30L)),
                new InvalidSyncKeyCommand("increment long duration", k -> sync.incr(k, 1L, Duration.ofSeconds(30))),
                new InvalidSyncKeyCommand("increment double", k -> sync.incr(k, 1.0)),
                new InvalidSyncKeyCommand("increment double seconds", k -> sync.incr(k, 1.0, 30L)),
                new InvalidSyncKeyCommand("increment double duration", k -> sync.incr(k, 1.0, Duration.ofSeconds(30))),
                new InvalidSyncKeyCommand("decrement long", k -> sync.decr(k, 1L)),
                new InvalidSyncKeyCommand("decrement long seconds", k -> sync.decr(k, 1L, 30L)),
                new InvalidSyncKeyCommand("decrement long duration", k -> sync.decr(k, 1L, Duration.ofSeconds(30))),
                new InvalidSyncKeyCommand("decrement double", k -> sync.decr(k, 1.0)),
                new InvalidSyncKeyCommand("decrement double seconds", k -> sync.decr(k, 1.0, 30L)),
                new InvalidSyncKeyCommand("decrement double duration", k -> sync.decr(k, 1.0, Duration.ofSeconds(30))),
                new InvalidSyncKeyCommand("delete", k -> sync.del(k))
        );
        return commands.stream().map(command -> DynamicTest.dynamicTest(command.name(), () ->
                assertThatThrownBy(() -> command.command().accept(null))
                        .isInstanceOf(RedisOperationException.class)));
    }

    @TestFactory
    Stream<DynamicTest> synchronousListCommandsRejectNullKeys() {
        List<InvalidSyncKeyCommand> commands = List.of(
                new InvalidSyncKeyCommand("range", k -> sync.lRange(k, 0, -1)),
                new InvalidSyncKeyCommand("index", k -> sync.lIndex(k, 0)),
                new InvalidSyncKeyCommand("left push distinct", k -> sync.lLeftPushDistinct(k, "a")),
                new InvalidSyncKeyCommand("left push", k -> sync.lLeftPush(k, "a")),
                new InvalidSyncKeyCommand("left push seconds", k -> sync.lLeftPush(k, "a", 30L)),
                new InvalidSyncKeyCommand("left push duration", k -> sync.lLeftPush(k, "a", Duration.ofSeconds(30))),
                new InvalidSyncKeyCommand("left push all", k -> sync.lLeftPushAll(k, List.of("a", "b"))),
                new InvalidSyncKeyCommand("left push all seconds", k -> sync.lLeftPushAll(k, List.of("a"), 30L)),
                new InvalidSyncKeyCommand("left push all duration", k -> sync.lLeftPushAll(k, List.of("a"), Duration.ofSeconds(30))),
                new InvalidSyncKeyCommand("left push if present", k -> sync.lLeftPushx(k, "a")),
                new InvalidSyncKeyCommand("left push if present seconds", k -> sync.lLeftPushx(k, "a", 30L)),
                new InvalidSyncKeyCommand("left push if present duration", k -> sync.lLeftPushx(k, "a", Duration.ofSeconds(30))),
                new InvalidSyncKeyCommand("left push if present all seconds", k -> sync.lLeftPushxAll(k, List.of("a"), 30L)),
                new InvalidSyncKeyCommand("left push if present all duration", k -> sync.lLeftPushxAll(k, List.of("a"), Duration.ofSeconds(30))),
                new InvalidSyncKeyCommand("left pop", sync::lLeftPop),
                new InvalidSyncKeyCommand("left pop and remove", sync::lLeftPopAndLrem),
                new InvalidSyncKeyCommand("left pop timeout", k -> sync.lLeftPop(k, 1L, TimeUnit.SECONDS)),
                new InvalidSyncKeyCommand("left pop duration", k -> sync.lLeftPop(k, Duration.ofSeconds(1))),
                new InvalidSyncKeyCommand("left pop count", k -> sync.lLeftPop(k, 1)),
                new InvalidSyncKeyCommand("right push distinct", k -> sync.lRightPushDistinct(k, "a")),
                new InvalidSyncKeyCommand("right push", k -> sync.lRightPush(k, "a")),
                new InvalidSyncKeyCommand("right push seconds", k -> sync.lRightPush(k, "a", 30L)),
                new InvalidSyncKeyCommand("right push duration", k -> sync.lRightPush(k, "a", Duration.ofSeconds(30))),
                new InvalidSyncKeyCommand("right push all", k -> sync.lRightPushAll(k, List.of("a", "b"))),
                new InvalidSyncKeyCommand("right push all seconds", k -> sync.lRightPushAll(k, List.of("a"), 30L)),
                new InvalidSyncKeyCommand("right push all duration", k -> sync.lRightPushAll(k, List.of("a"), Duration.ofSeconds(30))),
                new InvalidSyncKeyCommand("right push if present", k -> sync.lRightPushx(k, "a")),
                new InvalidSyncKeyCommand("right push if present seconds", k -> sync.lRightPushx(k, "a", 30L)),
                new InvalidSyncKeyCommand("right push if present duration", k -> sync.lRightPushx(k, "a", Duration.ofSeconds(30))),
                new InvalidSyncKeyCommand("right push if present all seconds", k -> sync.lRightPushxAll(k, List.of("a"), 30L)),
                new InvalidSyncKeyCommand("right push if present all duration", k -> sync.lRightPushxAll(k, List.of("a"), Duration.ofSeconds(30))),
                new InvalidSyncKeyCommand("right pop", sync::lRightPop),
                new InvalidSyncKeyCommand("right pop and remove", sync::lRightPopAndLrem),
                new InvalidSyncKeyCommand("right pop timeout", k -> sync.lRightPop(k, 1L, TimeUnit.SECONDS)),
                new InvalidSyncKeyCommand("right pop duration", k -> sync.lRightPop(k, Duration.ofSeconds(1))),
                new InvalidSyncKeyCommand("right pop count", k -> sync.lRightPop(k, 1)),
                new InvalidSyncKeyCommand("move right to left", k -> sync.lRightPopAndLeftPush(k, "destination")),
                new InvalidSyncKeyCommand("move right to left timeout", k -> sync.lRightPopAndLeftPush(k, "destination", 1L, TimeUnit.SECONDS)),
                new InvalidSyncKeyCommand("move right to left duration", k -> sync.lRightPopAndLeftPush(k, "destination", Duration.ofSeconds(1))),
                new InvalidSyncKeyCommand("set index", k -> sync.lSet(k, 0, "a")),
                new InvalidSyncKeyCommand("size", sync::lSize),
                new InvalidSyncKeyCommand("trim", k -> sync.lTrim(k, 0, 1)),
                new InvalidSyncKeyCommand("remove", k -> sync.lRem(k, 1, "a"))
        );
        return commands.stream().map(command -> DynamicTest.dynamicTest(command.name(), () ->
                assertThatThrownBy(() -> command.command().accept(null))
                        .isInstanceOf(RedisOperationException.class)));
    }

    @TestFactory
    Stream<DynamicTest> synchronousHashCommandsRejectNullKeys() {
        List<InvalidSyncKeyCommand> commands = List.of(
                new InvalidSyncKeyCommand("decrement int", k -> sync.hDecr(k, "field", 1)),
                new InvalidSyncKeyCommand("decrement long", k -> sync.hDecr(k, "field", 1L)),
                new InvalidSyncKeyCommand("decrement double", k -> sync.hDecr(k, "field", 1.0)),
                new InvalidSyncKeyCommand("delete field", k -> sync.hDel(k, "field")),
                new InvalidSyncKeyCommand("delete hash", k -> sync.hDel(k)),
                new InvalidSyncKeyCommand("has field", k -> sync.hHasKey(k, "field")),
                new InvalidSyncKeyCommand("get field", k -> sync.hGet(k, "field")),
                new InvalidSyncKeyCommand("get fields", k -> sync.hmGet(k, List.of("field"))),
                new InvalidSyncKeyCommand("get all", k -> sync.hmGet(k)),
                new InvalidSyncKeyCommand("multi get", k -> sync.hmMultiGet(k, List.of("field"))),
                new InvalidSyncKeyCommand("map set", k -> sync.hmSet(k, Map.of("field", "value"))),
                new InvalidSyncKeyCommand("map set seconds", k -> sync.hmSet(k, Map.of("field", "value"), 30L)),
                new InvalidSyncKeyCommand("map set duration", k -> sync.hmSet(k, Map.of("field", "value"), Duration.ofSeconds(30))),
                new InvalidSyncKeyCommand("set field", k -> sync.hSet(k, "field", "value")),
                new InvalidSyncKeyCommand("set field seconds", k -> sync.hSet(k, "field", "value", 30L)),
                new InvalidSyncKeyCommand("set field duration", k -> sync.hSet(k, "field", "value", Duration.ofSeconds(30))),
                new InvalidSyncKeyCommand("set field if absent", k -> sync.hSetNX(k, "field", "value")),
                new InvalidSyncKeyCommand("field keys", sync::hKeys),
                new InvalidSyncKeyCommand("size", sync::hSize),
                new InvalidSyncKeyCommand("values", sync::hValues),
                new InvalidSyncKeyCommand("increment int", k -> sync.hIncr(k, "field", 1)),
                new InvalidSyncKeyCommand("increment int seconds", k -> sync.hIncr(k, "field", 1, 30L)),
                new InvalidSyncKeyCommand("increment int duration", k -> sync.hIncr(k, "field", 1, Duration.ofSeconds(30))),
                new InvalidSyncKeyCommand("increment long", k -> sync.hIncr(k, "field", 1L)),
                new InvalidSyncKeyCommand("increment long seconds", k -> sync.hIncr(k, "field", 1L, 30L)),
                new InvalidSyncKeyCommand("increment long duration", k -> sync.hIncr(k, "field", 1L, Duration.ofSeconds(30))),
                new InvalidSyncKeyCommand("increment double", k -> sync.hIncr(k, "field", 1.0)),
                new InvalidSyncKeyCommand("increment double seconds", k -> sync.hIncr(k, "field", 1.0, 30L)),
                new InvalidSyncKeyCommand("increment double duration", k -> sync.hIncr(k, "field", 1.0, Duration.ofSeconds(30)))
        );
        return commands.stream().map(command -> DynamicTest.dynamicTest(command.name(), () ->
                assertThatThrownBy(() -> command.command().accept(null))
                        .isInstanceOf(RedisOperationException.class)));
    }

    @TestFactory
    Stream<DynamicTest> synchronousSetCommandsRejectNullKeys() {
        List<InvalidSyncKeyCommand> commands = List.of(
                new InvalidSyncKeyCommand("add", k -> sync.sAdd(k, "a")),
                new InvalidSyncKeyCommand("add seconds", k -> sync.sAddAndExpire(k, 30L, "a")),
                new InvalidSyncKeyCommand("add duration", k -> sync.sAddAndExpire(k, Duration.ofSeconds(30), "a")),
                new InvalidSyncKeyCommand("delete", sync::sDel),
                new InvalidSyncKeyCommand("members", sync::sGet),
                new InvalidSyncKeyCommand("difference", k -> sync.sDiff(k, "other")),
                new InvalidSyncKeyCommand("difference store", k -> sync.sDiffAndStore(k, "other", "target")),
                new InvalidSyncKeyCommand("contains", k -> sync.sHasKey(k, "a")),
                new InvalidSyncKeyCommand("intersection", k -> sync.sIntersect(k, "other")),
                new InvalidSyncKeyCommand("intersection store", k -> sync.sIntersectAndStore(k, "other", "target")),
                new InvalidSyncKeyCommand("random", k -> sync.sRandom(k, 1)),
                new InvalidSyncKeyCommand("random distinct", k -> sync.sRandomDistinct(k, 1)),
                new InvalidSyncKeyCommand("remove", k -> sync.sRemove(k, "a")),
                new InvalidSyncKeyCommand("legacy timed", k -> sync.sSetAndTime(k, 30L, "a")),
                new InvalidSyncKeyCommand("size", sync::sSize),
                new InvalidSyncKeyCommand("union", k -> sync.sUnion(k, "other")),
                new InvalidSyncKeyCommand("union store", k -> sync.sUnionAndStore(k, "other", "target"))
        );
        return commands.stream().map(command -> DynamicTest.dynamicTest(command.name(), () ->
                assertThatThrownBy(() -> command.command().accept(null))
                        .isInstanceOf(RedisOperationException.class)));
    }

    @TestFactory
    Stream<DynamicTest> synchronousSortedSetCommandsRejectNullKeys() {
        Set<ZSetOperations.TypedTuple<Object>> tuples = Set.of(new DefaultTypedTuple<>("a", 1.0));
        List<InvalidSyncKeyCommand> commands = List.of(
                new InvalidSyncKeyCommand("add", k -> sync.zAdd(k, "a", 1.0)),
                new InvalidSyncKeyCommand("add seconds", k -> sync.zAdd(k, "a", 1.0, 30L)),
                new InvalidSyncKeyCommand("add duration", k -> sync.zAdd(k, "a", 1.0, Duration.ofSeconds(30))),
                new InvalidSyncKeyCommand("add tuples", k -> sync.zAdd(k, tuples)),
                new InvalidSyncKeyCommand("add tuples seconds", k -> sync.zAdd(k, tuples, 30L)),
                new InvalidSyncKeyCommand("add tuples duration", k -> sync.zAdd(k, tuples, Duration.ofSeconds(30))),
                new InvalidSyncKeyCommand("size", sync::zCard),
                new InvalidSyncKeyCommand("contains", k -> sync.zHas(k, "a")),
                new InvalidSyncKeyCommand("count", k -> sync.zCount(k, 0.0, 2.0)),
                new InvalidSyncKeyCommand("delete", sync::zDel),
                new InvalidSyncKeyCommand("increment", k -> sync.zIncr(k, "a", 1.0)),
                new InvalidSyncKeyCommand("increment seconds", k -> sync.zIncr(k, "a", 1.0, 30L)),
                new InvalidSyncKeyCommand("increment duration", k -> sync.zIncr(k, "a", 1.0, Duration.ofSeconds(30))),
                new InvalidSyncKeyCommand("intersection aggregate", k -> sync.zIntersectAndStore(k, List.of("other"), "target", Aggregate.SUM)),
                new InvalidSyncKeyCommand("intersection weighted", k -> sync.zIntersectAndStore(k, List.of("other"), "target", Aggregate.SUM, Weights.of(1, 1))),
                new InvalidSyncKeyCommand("remove", k -> sync.zRem(k, "a")),
                new InvalidSyncKeyCommand("remove score", k -> sync.zRemByScore(k, 0.0, 2.0)),
                new InvalidSyncKeyCommand("range", k -> sync.zRange(k, 0, -1)),
                new InvalidSyncKeyCommand("score range", k -> sync.zRangeByScore(k, 0.0, 2.0)),
                new InvalidSyncKeyCommand("scored range", k -> sync.zRangeWithScores(k, 0, -1)),
                new InvalidSyncKeyCommand("score and score range", k -> sync.zRangeByScoreWithScores(k, 0.0, 2.0)),
                new InvalidSyncKeyCommand("lexical range", k -> sync.zRangeByLex(k, Range.closed("a", "z"))),
                new InvalidSyncKeyCommand("lexical range limit", k -> sync.zRangeByLex(k, Range.closed("a", "z"), Limit.limit().count(2))),
                new InvalidSyncKeyCommand("reverse range", k -> sync.zRevrange(k, 0, -1)),
                new InvalidSyncKeyCommand("reverse scored range", k -> sync.zRevrangeWithScores(k, 0, -1)),
                new InvalidSyncKeyCommand("reverse score range", k -> sync.zRevrangeByScore(k, 0.0, 2.0)),
                new InvalidSyncKeyCommand("reverse score and score range", k -> sync.zRevrangeByScoreWithScores(k, 0.0, 2.0)),
                new InvalidSyncKeyCommand("reverse rank", k -> sync.zRevRank(k, "a")),
                new InvalidSyncKeyCommand("score", k -> sync.zScore(k, "a")),
                new InvalidSyncKeyCommand("union aggregate", k -> sync.zUnionAndStore(k, List.of("other"), "target", Aggregate.SUM)),
                new InvalidSyncKeyCommand("union weighted", k -> sync.zUnionAndStore(k, List.of("other"), "target", Aggregate.SUM, Weights.of(1, 1)))
        );
        return commands.stream().map(command -> DynamicTest.dynamicTest(command.name(), () ->
                assertThatThrownBy(() -> command.command().accept(null))
                        .isInstanceOf(RedisOperationException.class)));
    }

    @TestFactory
    Stream<DynamicTest> synchronousStreamBitmapAndHyperLogLogCommandsRejectNullKeys() {
        List<InvalidSyncKeyCommand> commands = List.of(
                new InvalidSyncKeyCommand("stream add", k -> sync.xAdd(k, Map.of("event", "created"))),
                new InvalidSyncKeyCommand("stream trim", k -> sync.xTrim(k, 1)),
                new InvalidSyncKeyCommand("stream delete string ID", k -> sync.xDel(k, "1-0")),
                new InvalidSyncKeyCommand("stream delete record ID", k -> sync.xDel(k, org.springframework.data.redis.connection.stream.RecordId.of("1-0"))),
                new InvalidSyncKeyCommand("stream length", sync::xLen),
                new InvalidSyncKeyCommand("stream range", k -> sync.xRange(k, Range.unbounded())),
                new InvalidSyncKeyCommand("stream limited range", k -> sync.xRange(k, Range.unbounded(), Limit.limit().count(1))),
                new InvalidSyncKeyCommand("stream typed range", k -> sync.xRangeFor(String.class, k, Range.unbounded())),
                new InvalidSyncKeyCommand("stream typed limited range", k -> sync.xRangeFor(String.class, k, Range.unbounded(), Limit.limit().count(1))),
                new InvalidSyncKeyCommand("stream group", k -> sync.addGroup(k, "readers")),
                new InvalidSyncKeyCommand("hyperloglog add", k -> sync.pfAdd(k, "a")),
                new InvalidSyncKeyCommand("hyperloglog delete", sync::pfDel),
                new InvalidSyncKeyCommand("hyperloglog count", k -> sync.pfCount(k)),
                new InvalidSyncKeyCommand("hyperloglog merge", k -> sync.pfMerge(k, "source")),
                new InvalidSyncKeyCommand("set bit", k -> sync.setBit(k, 0, true)),
                new InvalidSyncKeyCommand("get bit", k -> sync.getBit(k, 0)),
                new InvalidSyncKeyCommand("append", k -> sync.append(k, "suffix"))
        );
        return commands.stream().map(command -> DynamicTest.dynamicTest(command.name(), () ->
                assertThatThrownBy(() -> command.command().accept(null))
                        .isInstanceOf(RedisOperationException.class)));
    }

    @Test
    void synchronousStreamTrimGroupAndDeleteOverloadsChangeStoredRecords() {
        String stream = key("sync-stream-variants");
        var first = sync.xAdd(stream, Map.of("event", "one"));
        var second = sync.xAdd(stream, Map.of("event", "two"));
        var third = sync.xAdd(stream, Map.of("event", "three"));
        assertThat(sync.xRange(stream, Range.unbounded(), Limit.limit().count(2)))
                .extracting(record -> record.getValue().get("event"))
                .containsExactly("one", "two");
        assertThat(sync.addGroup(stream, "readers")).isNotNull();
        assertThat(sync.xTrim(stream, 2)).isEqualTo(1L);
        assertThat(sync.xLen(stream)).isEqualTo(2L);
        assertThat(sync.xDel(stream, first)).isZero();
        assertThat(sync.xDel(stream, second)).isEqualTo(1L);
        assertThat(sync.xDel(stream, third.getValue())).isEqualTo(1L);
        assertThat(sync.xLen(stream)).isZero();
    }

    @Test
    void synchronousServerReadsAndHyperLogLogDeleteUseOwnedRedis() {
        long now = sync.timeNow();
        assertThat(now).isPositive();
        assertThat(sync.period(now + 10_000)).isBetween(1L, 10_000L);
        assertThat(sync.dbSize()).isNotNegative();
        assertThat(sync.lastSave()).isNotNegative();

        String hll = key("sync-hll-delete");
        assertThat(sync.pfAdd(hll, "a", "b")).isEqualTo(1L);
        assertThat(sync.pfCount(hll)).isEqualTo(2L);
        assertThat(sync.pfDel(hll)).isTrue();
        assertThat(sync.hasKey(hll)).isFalse();
    }

    @Test
    void synchronousJsonValueHelpersConvertPersistedObjects() {
        String map = key("sync-json-map"), list = key("sync-json-list");
        sync.set(map, "{\"name\":\"Ada\",\"count\":2}");
        sync.set(list, "[\"a\",\"b\"]");
        assertThat(sync.getMap(map)).containsEntry("name", "Ada").containsEntry("count", 2);
        assertThat(sync.getList(list)).containsExactly("a", "b");
        assertThat(sync.getList(list, String.class)).containsExactly("a", "b");
        assertThat(sync.getMap(key("missing-json-map"))).isNull();
        assertThat(sync.getList(key("missing-json-list"))).isNull();
        assertThat(sync.getList(key("missing-typed-list"), String.class)).isNull();
    }

    @Test
    void synchronousPipelinedHashReadsAndScriptObjectsSeeStoredValues() {
        String first = key("sync-pipeline-first"), second = key("sync-pipeline-second");
        sync.hSet(first, "field", "one");
        sync.hSet(second, "field", "two");
        assertThat(sync.hGet(List.of(first, second), "field")).containsExactly("one", "two");
        String prefix = key("sync-pipeline-prefix");
        String prefixed = RedisKey.getKeyStr(prefix, "item");
        sync.hSet(prefixed, "field", "prefixed");
        assertThat(sync.hGet(List.of("item"), prefix, "field")).containsExactly("prefixed");

        StringRedisSerializer strings = new StringRedisSerializer();
        RedisCallback<Object> read = connection -> {
            connection.hGet(strings.serialize(first), strings.serialize("field"));
            return null;
        };
        assertThat(sync.executePipelined(read)).containsExactly("one");
        assertThat(sync.executePipelined(read, strings)).containsExactly("one");

        String script = "return redis.call('HGET', KEYS[1], ARGV[1])";
        assertThat(sync.executeLuaScript(RedisScript.of(script, String.class), List.of(first), "field"))
                .isEqualTo("one");
        assertThat(sync.executeLuaScript(new ByteArrayResource(script.getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                String.class, List.of(first), "field")).isEqualTo("one");
    }

    @Test
    void synchronousBlockingLockAndPublishUseOnlyOwnedRedisKeys() throws Exception {
        String lock = key("sync-block-lock");
        assertThat(sync.tryBlockLock(lock, 1)).isTrue();
        assertThat(sync.hasKey(lock)).isTrue();
        assertThat(sync.unBlockLock(lock, "request-1")).isTrue();
        assertThat(sync.hasKey(lock)).isFalse();
        assertThat(sync.lRangeString(lock + ":list", 0, -1)).containsExactly("request-1");

        String channel = key("sync-channel");
        var received = CompletableFuture.supplyAsync(() ->
                reactive.listenTo(new ChannelTopic(channel)).next().block(Duration.ofSeconds(3)));
        for (int attempt = 0; attempt < 10 && !received.isDone(); attempt++) {
            sync.sendMessage(channel, "payload");
            Thread.sleep(30);
        }
        assertThat(received.get(3, TimeUnit.SECONDS).getMessage()).isEqualTo("payload");
    }

    @Test
    void synchronousTypedListAndHashReadsConvertStoredValues() {
        String list = key("sync-typed-pop"), hash = key("sync-typed-hash");
        sync.lRightPushAll(list, List.of("1", "2", "3", "4"));
        assertThat(sync.lLeftPop(list, 2, Integer.class)).containsExactly(1, 2);
        assertThat(sync.lLeftPop(list, 2, new TypeReference<Integer>() {})).containsExactly(3, 4);
        sync.hSet(hash, "a", "1");
        sync.hSet(hash, "b", "2");
        assertThat(sync.hmGetFor(hash, new TypeReference<Map<String, Integer>>() {}))
                .containsEntry("a", 1).containsEntry("b", 2);
        assertThat(sync.hmMultiGetFor(hash, List.of("a", "b", "missing"), new TypeReference<Integer>() {}))
                .containsEntry("a", 1).containsEntry("b", 2).doesNotContainKey("missing");
        assertThat(sync.hmMultiGetFor(hash, List.of("a"), Integer.class)).containsEntry("a", 1);
        assertThat(sync.hmGetFor(key("sync-missing-hash"), new TypeReference<Map<String, Integer>>() {})).isNull();
    }

    @Test
    void synchronousScanPatternOverloadsVisitOnlyMatchingStoredMembers() {
        String hash = key("sync-scan-hash"), set = key("sync-scan-set"), sorted = key("sync-scan-zset");
        sync.hSet(hash, "keep:a", "one");
        sync.hSet(hash, "drop:b", "two");
        sync.sAdd(set, "keep:a", "drop:b");
        sync.zAdd(sorted, "keep:a", 1.0);
        sync.zAdd(sorted, "drop:b", 2.0);
        List<Map.Entry<Object, Object>> hashEntries = new ArrayList<>();
        sync.hScan(hash, "keep:*", hashEntries::add);
        assertThat(hashEntries).containsExactly(Map.entry("keep:a", "one"));
        hashEntries.clear();
        sync.hScan(hash, ScanOptions.scanOptions().match("drop:*").build(), hashEntries::add);
        assertThat(hashEntries).containsExactly(Map.entry("drop:b", "two"));
        List<String> setMembers = new ArrayList<>();
        sync.sScan(set, "keep:*", bytes -> setMembers.add(new String(bytes, java.nio.charset.StandardCharsets.UTF_8)));
        assertThat(setMembers).containsExactly("keep:a");
        setMembers.clear();
        sync.sScan(set, ScanOptions.scanOptions().match("drop:*").build(),
                bytes -> setMembers.add(new String(bytes, java.nio.charset.StandardCharsets.UTF_8)));
        assertThat(setMembers).containsExactly("drop:b");
        List<String> sortedMembers = new ArrayList<>();
        sync.zScan(sorted, "keep:*", tuple -> sortedMembers.add(new String(tuple.getValue(), java.nio.charset.StandardCharsets.UTF_8)));
        assertThat(sortedMembers).containsExactly("keep:a");
        sortedMembers.clear();
        sync.zScan(sorted, ScanOptions.scanOptions().match("drop:*").build(),
                tuple -> sortedMembers.add(new String(tuple.getValue(), java.nio.charset.StandardCharsets.UTF_8)));
        assertThat(sortedMembers).containsExactly("drop:b");
    }

    @Test
    void synchronousTypedValueAndListWrappersReadPersistedJsonAndNumbers() {
        String first = key("typed-json-first"), second = key("typed-json-second");
        sync.set(first, "{\"name\":\"Ada\"}");
        sync.set(second, "{\"name\":\"Lin\"}");
        assertThat(sync.getFor(first, new TypeReference<Map<String, String>>() {}))
                .containsEntry("name", "Ada");
        assertThat(sync.mGetMap(List.of(first, second))).extracting(map -> map.get("name"))
                .containsExactly("Ada", "Lin");
        assertThat(sync.mGetFor(List.of(first, second), new TypeReference<Map<String, String>>() {}))
                .extracting(map -> map.get("name")).containsExactly("Ada", "Lin");

        String list = key("typed-json-list");
        sync.lRightPushAll(list, List.of("{\"name\":\"Ada\"}", "{\"name\":\"Lin\"}"));
        assertThat(sync.lRangeMap(list, 0, -1)).extracting(map -> map.get("name"))
                .containsExactly("Ada", "Lin");
        assertThat(sync.lRangeFor(list, 0, -1, new TypeReference<Map<String, String>>() {}))
                .extracting(map -> map.get("name")).containsExactly("Ada", "Lin");

        String numeric = key("typed-numeric"), numbers = key("typed-number-list");
        sync.set(numeric, "8");
        assertThat(sync.getInteger(numeric, 0)).isEqualTo(8);
        assertThat(sync.getDouble(numeric, 0.0)).isEqualTo(8.0);
        assertThat(sync.getFor(numeric, Integer.class)).isEqualTo(8);
        sync.set(numbers, "[1,2]");
        assertThat(sync.mGetList(List.of(numbers))).containsExactly(List.of(1, 2));
        String arrayList = key("typed-array-list");
        sync.lRightPushAll(arrayList, List.of("[1,2]", "[3,4]"));
        assertThat(sync.lRangeList(arrayList, 0, -1))
                .containsExactly(List.of(1, 2), List.of(3, 4));
    }

    @Test
    void synchronousTypedHashAndSetWrappersReadPersistedNumbers() {
        String first = key("typed-hash-a"), second = key("typed-hash-b");
        sync.hSet(first, "field", "1");
        sync.hSet(second, "field", "2");
        assertThat(sync.hGetFor(first, "field", Integer.class)).isEqualTo(1);
        assertThat(sync.hGetFor(first, "field", new TypeReference<Integer>() {})).isEqualTo(1);
        assertThat(sync.hGetFor(List.of(first, second), "field", Integer.class)).containsExactly(1, 2);
        assertThat(sync.hGetFor(List.of(first, second), "field", new TypeReference<Integer>() {}))
                .containsExactly(1, 2);
        assertThat(sync.hMultiGetInteger(List.of(first, second), "field")).containsExactly(1, 2);
        assertThat(sync.hMultiGetDouble(List.of(first, second), "field")).containsExactly(1.0, 2.0);
        assertThat(sync.hMultiGetFor(List.of(first, second), "field", Integer.class)).containsExactly(1, 2);
        assertThat(sync.hMultiGetFor(List.of(first, second), "field", new TypeReference<Integer>() {}))
                .containsExactly(1, 2);
        assertThat(sync.hmGetFor(first, Map.class)).containsEntry("field", "1");

        String set = key("typed-set");
        sync.sAdd(set, "1", "2");
        assertThat(sync.sGetString(set)).containsExactlyInAnyOrder("1", "2");
        assertThat(sync.sGetInteger(set)).containsExactlyInAnyOrder(1, 2);
        assertThat(sync.sGetDouble(set)).containsExactlyInAnyOrder(1.0, 2.0);
        assertThat(sync.sGetFor(set, Integer.class)).containsExactlyInAnyOrder(1, 2);
        assertThat(sync.sGetFor(set, new TypeReference<Integer>() {})).containsExactlyInAnyOrder(1, 2);
        assertThat(sync.sRandomFor(set, 1, new TypeReference<Integer>() {})).allSatisfy(value ->
                assertThat(value).isIn(1, 2));
        assertThat(sync.sRandomDistinctFor(set, 2, new TypeReference<Integer>() {}))
                .containsExactlyInAnyOrder(1, 2);
    }

    @Test
    void synchronousSortedSetWatchReportsVisibleRankChanges() {
        String sorted = key("sync-watch-scores");
        assertThat(sync.zIncrAndWatch(sorted, "a", 1.0, 0, 1)).isTrue();
        sync.zAdd(sorted, "b", 2.0);
        assertThat(sync.zIncrAndWatch(sorted, "a", 0.1, 0, 1)).isFalse();
        assertThat(sync.zScore(sorted, "a")).isCloseTo(1.1, org.assertj.core.data.Offset.offset(0.0001));
        assertThat(sync.zIncrAndWatch(sorted, "a", 2.0, 0, 1)).isTrue();
        assertThat(sync.zRevrangeString(sorted, 0, 1)).containsExactly("a", "b");
    }

    @Test
    void synchronousOwnerLockRetrySucceedsAfterTheOwnerReleases() throws Exception {
        String lock = key("sync-retry-owner-lock");
        assertThat(sync.tryLock(lock, "first", Duration.ofSeconds(5), 0, 0L)).isTrue();
        CompletableFuture<Boolean> waiter = CompletableFuture.supplyAsync(() ->
                sync.tryLock(lock, "second", 5_000L, 30, 20L));
        Thread.sleep(80);
        assertThat(sync.unlock(lock, "first")).isTrue();
        assertThat(waiter.get(3, TimeUnit.SECONDS)).isTrue();
        assertThat(sync.unlock(lock, "first")).isFalse();
        assertThat(sync.unlock(lock, "second")).isTrue();
        assertThat(sync.hasKey(lock)).isFalse();
    }

    @Test
    void synchronousTypedSortedSetRangesPreserveOrderAndScores() {
        String sorted = key("typed-zset-ranges");
        sync.zAdd(sorted, "1", 1.0);
        sync.zAdd(sorted, "2", 2.0);
        sync.zAdd(sorted, "3", 3.0);
        assertThat(sync.zRangeDouble(sorted, 0, -1)).containsExactly(1.0, 2.0, 3.0);
        assertThat(sync.zRangeFor(sorted, 0, -1, Integer.class)).containsExactly(1, 2, 3);
        assertThat(sync.zRangeFor(sorted, 0, -1, new TypeReference<Integer>() {}))
                .containsExactly(1, 2, 3);
        assertThat(sync.zRangeLongByScore(sorted, 1.0, 2.0)).containsExactly(1L, 2L);
        assertThat(sync.zRangeIntegerByScore(sorted, 1.0, 2.0)).containsExactly(1, 2);
        assertThat(sync.zRangeByScoreFor(sorted, 1.0, 2.0, Integer.class)).containsExactly(1, 2);
        assertThat(sync.zRangeByScoreFor(sorted, 1.0, 2.0, new TypeReference<Integer>() {}))
                .containsExactly(1, 2);
        assertThat(sync.zRevrangeString(sorted, 0, -1)).containsExactly("3", "2", "1");
        assertThat(sync.zRevrangeDouble(sorted, 0, -1)).containsExactly(3.0, 2.0, 1.0);
        assertThat(sync.zRevrangeLong(sorted, 0, -1)).containsExactly(3L, 2L, 1L);
        assertThat(sync.zRevrangeFor(sorted, 0, -1, Integer.class)).containsExactly(3, 2, 1);
        assertThat(sync.zRevrangeFor(sorted, 0, -1, new TypeReference<Integer>() {}))
                .containsExactly(3, 2, 1);
        assertThat(sync.zRevrangeDoubleByScore(sorted, 1.0, 2.0)).containsExactly(2.0, 1.0);
        assertThat(sync.zRevrangeLongByScore(sorted, 1.0, 2.0)).containsExactly(2L, 1L);
        assertThat(sync.zRevrangeIntegerByScore(sorted, 1.0, 2.0)).containsExactly(2, 1);
        assertThat(sync.zRevrangeForByScore(sorted, 1.0, 2.0, Integer.class)).containsExactly(2, 1);
        assertThat(sync.zRevrangeForByScore(sorted, 1.0, 2.0, new TypeReference<Integer>() {}))
                .containsExactly(2, 1);
        assertThat(sync.zScore(sorted, "missing", -1.0)).isEqualTo(-1.0);
    }

    @Test
    void synchronousSwapAndBulkReadOverloadsReturnStoredOldValues() {
        String a = key("sync-swap-a"), b = key("sync-swap-b");
        sync.set(a, "1");
        sync.set(b, "2");
        assertThat(sync.mGetFor(List.of(a, b), Integer.class)).containsExactly(1, 2);
        JsonOperations json = jsonOperations();
        String numeric = key("sync-json-swap");
        json.sync().set(numeric, 1);
        try {
            assertThat(json.sync().getIntegerAndSet(numeric, 3)).isEqualTo(1);
            assertThat(json.sync().getLongAndSet(numeric, 4L)).isEqualTo(3L);
            assertThat(json.sync().getDoubleAndSet(numeric, 5.0)).isEqualTo(4.0);
            assertThat(json.sync().getForAndSet(numeric, 6, Double.class)).isEqualTo(5.0);
            assertThat(json.sync().getForAndSet(numeric, 7, new TypeReference<Integer>() {})).isEqualTo(6);
            assertThat(json.sync().getInteger(numeric)).isEqualTo(7);
        } finally {
            sync.del(numeric);
        }
        String list = key("sync-push-if-present");
        assertThat(sync.lLeftPushx(list, "before")).isZero();
        sync.lRightPush(list, "middle");
        assertThat(sync.lLeftPushx(list, "before")).isEqualTo(2L);
        assertThat(sync.lRightPushx(list, "after")).isEqualTo(3L);
        assertThat(sync.lRangeFor(list, 0, -1, String.class))
                .containsExactly("before", "middle", "after");
    }

    @Test
    void synchronousBulkHashReadsAndDefaultsReflectActualFields() {
        String first = key("sync-hash-bulk-a"), second = key("sync-hash-bulk-b");
        sync.hSet(first, "id", "1");
        sync.hSet(second, "id", "2");
        assertThat(sync.hGet(first, "missing", "fallback")).isEqualTo("fallback");
        assertThat(sync.hGet(first, "id", "fallback")).isEqualTo("1");
        assertThat(sync.hmGet(List.of(first, second)))
                .containsExactly(Map.of("id", "1"), Map.of("id", "2"));
        assertThat(sync.hGetAllFor(List.of(first, second), Map.class))
                .containsExactly(Map.of("id", "1"), Map.of("id", "2"));
        assertThat(sync.hGetAllFor(List.of(first, second), new TypeReference<Map<String, String>>() {}))
                .containsExactly(Map.of("id", "1"), Map.of("id", "2"));
    }
}
