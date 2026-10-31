package org.springframework.data.redis.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.Map;

import org.springframework.data.redis.core.RedisOperationException;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapperBuilderCustomizer;

/**
 * Tests for {@link ObjectMappers}.
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 */
class ObjectMappersTest {

    @Test
    void defaultObjectMapperWithEmptyCustomizers() {
        List<JsonMapperBuilderCustomizer> customizers = new ArrayList<>();
        ObjectMapper mapper = ObjectMappers.defaultObjectMapper(customizers);
        assertThat(mapper).isNotNull();
    }

    @Test
    void defaultObjectMapperWithCustomizer() {
        List<JsonMapperBuilderCustomizer> customizers = new ArrayList<>();
        customizers.add(builder -> builder.findAndAddModules());
        ObjectMapper mapper = ObjectMappers.defaultObjectMapper(customizers);
        assertThat(mapper).isNotNull();
    }

    @Test
    void getMapperForStringClass() {
        ObjectMapper mapper = new ObjectMapper();
        Function<Object, String> func = ObjectMappers.getMapperFor(mapper, String.class);
        assertThat(func).isNotNull();
        assertThat(func.apply("hello")).isEqualTo("hello");
    }

    @Test
    void getMapperForIntegerClass() {
        ObjectMapper mapper = new ObjectMapper();
        Function<Object, Integer> func = ObjectMappers.getMapperFor(mapper, Integer.class);
        assertThat(func).isNotNull();
        assertThat(func.apply(42)).isEqualTo(42);
    }

    @Test
    void getMapperForBooleanClass() {
        ObjectMapper mapper = new ObjectMapper();
        Function<Object, Boolean> func = ObjectMappers.getMapperFor(mapper, Boolean.class);
        assertThat(func).isNotNull();
        assertThat(func.apply(true)).isTrue();
    }

    @Test
    void getMapperForLongClass() {
        ObjectMapper mapper = new ObjectMapper();
        Function<Object, Long> func = ObjectMappers.getMapperFor(mapper, Long.class);
        assertThat(func).isNotNull();
        assertThat(func.apply(100L)).isEqualTo(100L);
    }

    @Test
    void getMapperForDoubleClass() {
        ObjectMapper mapper = new ObjectMapper();
        Function<Object, Double> func = ObjectMappers.getMapperFor(mapper, Double.class);
        assertThat(func).isNotNull();
        assertThat(func.apply(3.14)).isEqualTo(3.14);
    }

    @Test
    void getMapperForFloatClass() {
        ObjectMapper mapper = new ObjectMapper();
        Function<Object, Float> func = ObjectMappers.getMapperFor(mapper, Float.class);
        assertThat(func).isNotNull();
        assertThat(func.apply(1.5f)).isEqualTo(1.5f);
    }

    @Test
    void getMapperForBigDecimalClass() {
        ObjectMapper mapper = new ObjectMapper();
        Function<Object, BigDecimal> func = ObjectMappers.getMapperFor(mapper, BigDecimal.class);
        assertThat(func).isNotNull();
        assertThat(func.apply(new BigDecimal("99.99"))).isEqualTo(new BigDecimal("99.99"));
    }

    @Test
    void getMapperForByteClass() {
        ObjectMapper mapper = new ObjectMapper();
        Function<Object, Byte> func = ObjectMappers.getMapperFor(mapper, Byte.class);
        assertThat(func).isNotNull();
        assertThat(func.apply((byte) 1)).isEqualTo((byte) 1);
    }

    @Test
    void getMapperForShortClass() {
        ObjectMapper mapper = new ObjectMapper();
        Function<Object, Short> func = ObjectMappers.getMapperFor(mapper, Short.class);
        assertThat(func).isNotNull();
        assertThat(func.apply((short) 10)).isEqualTo((short) 10);
    }

    @Test
    void getMapperForCharacterClass() {
        ObjectMapper mapper = new ObjectMapper();
        Function<Object, Character> func = ObjectMappers.getMapperFor(mapper, Character.class);
        assertThat(func).isNotNull();
        assertThat(func.apply('A')).isEqualTo('A');
    }

    @Test
    void getMapperForNullValue() {
        ObjectMapper mapper = new ObjectMapper();
        Function<Object, String> func = ObjectMappers.getMapperFor(mapper, String.class);
        assertThat(func.apply(null)).isNull();
    }

    @Test
    void getMapperForCustomClassFromString() {
        ObjectMapper mapper = new ObjectMapper();
        Function<Object, TestBean> func = ObjectMappers.getMapperFor(mapper, TestBean.class);
        assertThat(func).isNotNull();
        TestBean result = func.apply("{\"name\":\"test\"}");
        assertThat(result).isNotNull();
        assertThat(result.getName()).isEqualTo("test");
    }

    @Test
    void getMapperForCustomClassFromObject() {
        ObjectMapper mapper = new ObjectMapper();
        Function<Object, TestBean> func = ObjectMappers.getMapperFor(mapper, TestBean.class);
        assertThat(func).isNotNull();
        TestBean input = new TestBean();
        input.setName("test");
        TestBean result = func.apply(input);
        assertThat(result).isNotNull();
    }

    @Test
    void getMapperForSameType() {
        ObjectMapper mapper = new ObjectMapper();
        Function<Object, String> func = ObjectMappers.getMapperFor(mapper, String.class);
        assertThat(func.apply("hello")).isEqualTo("hello");
    }

    // Tests for getMapperFor with JavaType
    @Test
    void getMapperForJavaTypeFromString() {
        ObjectMapper mapper = new ObjectMapper();
        JavaType javaType = mapper.constructType(TestBean.class);
        Function<Object, TestBean> func = ObjectMappers.getMapperFor(mapper, javaType);
        assertThat(func).isNotNull();
        TestBean result = func.apply("{\"name\":\"test\"}");
        assertThat(result).isNotNull();
        assertThat(result.getName()).isEqualTo("test");
    }

    @Test
    void getMapperForJavaTypeFromObject() {
        ObjectMapper mapper = new ObjectMapper();
        JavaType javaType = mapper.constructType(TestBean.class);
        Function<Object, TestBean> func = ObjectMappers.getMapperFor(mapper, javaType);
        assertThat(func).isNotNull();
        TestBean input = new TestBean();
        input.setName("test");
        TestBean result = func.apply(input);
        assertThat(result).isNotNull();
    }

    @Test
    void getMapperForJavaTypeNull() {
        ObjectMapper mapper = new ObjectMapper();
        JavaType javaType = mapper.constructType(TestBean.class);
        Function<Object, TestBean> func = ObjectMappers.getMapperFor(mapper, javaType);
        assertThat(func.apply(null)).isNull();
    }

    @Test
    void getMapperForJavaTypeSameType() {
        ObjectMapper mapper = new ObjectMapper();
        JavaType javaType = mapper.constructType(String.class);
        Function<Object, String> func = ObjectMappers.getMapperFor(mapper, javaType);
        assertThat(func.apply("hello")).isEqualTo("hello");
    }

    // Tests for getMapperFor with TypeReference
    @Test
    void getMapperForTypeReferenceFromString() {
        ObjectMapper mapper = new ObjectMapper();
        TypeReference<TestBean> typeRef = new TypeReference<TestBean>() {};
        Function<Object, TestBean> func = ObjectMappers.getMapperFor(mapper, typeRef);
        assertThat(func).isNotNull();
        TestBean result = func.apply("{\"name\":\"test\"}");
        assertThat(result).isNotNull();
        assertThat(result.getName()).isEqualTo("test");
    }

    @Test
    void getMapperForTypeReferenceNull() {
        ObjectMapper mapper = new ObjectMapper();
        TypeReference<TestBean> typeRef = new TypeReference<TestBean>() {};
        Function<Object, TestBean> func = ObjectMappers.getMapperFor(mapper, typeRef);
        assertThat(func.apply(null)).isNull();
    }

    @Test
    void getMapperForTypeReferenceSameType() {
        ObjectMapper mapper = new ObjectMapper();
        TypeReference<String> typeRef = new TypeReference<String>() {};
        Function<Object, String> func = ObjectMappers.getMapperFor(mapper, typeRef);
        assertThat(func.apply("hello")).isEqualTo("hello");
    }

    // Tests for getMapperFor with java.lang.reflect.Type
    @Test
    void getMapperForTypeFromString() {
        ObjectMapper mapper = new ObjectMapper();
        Function<Object, TestBean> func = ObjectMappers.getMapperFor(mapper, (java.lang.reflect.Type) TestBean.class);
        assertThat(func).isNotNull();
        TestBean result = func.apply("{\"name\":\"test\"}");
        assertThat(result).isNotNull();
        assertThat(result.getName()).isEqualTo("test");
    }

    @Test
    void getMapperForTypeNull() {
        ObjectMapper mapper = new ObjectMapper();
        Function<Object, TestBean> func = ObjectMappers.getMapperFor(mapper, (java.lang.reflect.Type) TestBean.class);
        assertThat(func.apply(null)).isNull();
    }

    @Test
    void parameterizedTypeMapperConvertsStringsAndObjectsAndReportsBadJson() {
        ObjectMapper mapper = new ObjectMapper();
        java.lang.reflect.Type type = new TypeReference<List<TestBean>>() {}.getType();
        Function<Object, List<TestBean>> convert = ObjectMappers.getMapperFor(mapper, type);
        assertThat(convert.apply("[{\"name\":\"Ada\"}]"))
                .extracting(TestBean::getName).containsExactly("Ada");
        assertThat(convert.apply(List.of(Map.of("name", "Lin"))))
                .extracting(TestBean::getName).containsExactly("Lin");
        assertThat(convert.apply(null)).isNull();
        assertThatThrownBy(() -> convert.apply("not-json"))
                .isInstanceOf(RedisOperationException.class);
    }

    @Test
    void classAndJavaTypeMappersReportMalformedJson() {
        ObjectMapper mapper = new ObjectMapper();
        assertThatThrownBy(() -> ObjectMappers.getMapperFor(mapper, TestBean.class).apply("not-json"))
                .isInstanceOf(RedisOperationException.class);
        JavaType type = mapper.constructType(TestBean.class);
        assertThatThrownBy(() -> ObjectMappers.getMapperFor(mapper, type).apply("not-json"))
                .isInstanceOf(RedisOperationException.class);
    }

    public static class TestBean {
        private String name;

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
    }
}
