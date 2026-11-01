package com.fasterxml.jackson.databind.ser;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link RedisBeanSerializerModifier}.
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 */
class RedisBeanSerializerModifierTest {

    public static class NullFields {
        public List<String> items;
        public Integer count;
        public Boolean enabled;
        public String label;
        public LocalDate created;
    }

    @Test
    void modifierSerializesNullFieldsWithTheirDocumentedDefaults() throws Exception {
        ObjectMapper mapper = JsonMapper.builder()
                .serializerFactory(BeanSerializerFactory.instance.withSerializerModifier(new RedisBeanSerializerModifier()))
                .build();
        var document = mapper.readTree(mapper.writeValueAsString(new NullFields()));
        assertThat(document.get("items").isArray()).isTrue();
        assertThat(document.get("items")).isEmpty();
        assertThat(document.get("count").asInt()).isZero();
        assertThat(document.get("enabled").asBoolean()).isFalse();
        assertThat(document.get("label").asText()).isEmpty();
        assertThat(LocalDate.parse(document.get("created").asText().substring(0, 10))).isNotNull();
    }

    private RedisBeanSerializerModifier modifier;

    @BeforeEach
    void setUp() {
        modifier = new RedisBeanSerializerModifier();
    }

    @Test
    void isArrayTypeWithArray() {
        assertThat(modifier.isArrayType(String[].class)).isTrue();
    }

    @Test
    void isArrayTypeWithList() {
        assertThat(modifier.isArrayType(List.class)).isTrue();
    }

    @Test
    void isArrayTypeWithSet() {
        assertThat(modifier.isArrayType(Set.class)).isTrue();
    }

    @Test
    void isArrayTypeWithCollection() {
        assertThat(modifier.isArrayType(Collection.class)).isTrue();
    }

    @Test
    void isArrayTypeWithNonArray() {
        assertThat(modifier.isArrayType(String.class)).isFalse();
    }

    @Test
    void isStringTypeWithString() {
        assertThat(modifier.isStringType(String.class)).isTrue();
    }

    @Test
    void isStringTypeWithCharSequence() {
        assertThat(modifier.isStringType(CharSequence.class)).isTrue();
    }

    @Test
    void isStringTypeWithCharacter() {
        assertThat(modifier.isStringType(Character.class)).isTrue();
    }

    @Test
    void isStringTypeWithNonString() {
        assertThat(modifier.isStringType(Integer.class)).isFalse();
    }

    @Test
    void isDateTypeWithDate() {
        assertThat(modifier.isDateType(Date.class)).isTrue();
    }

    @Test
    void isDateTypeWithSqlDate() {
        assertThat(modifier.isDateType(java.sql.Date.class)).isTrue();
    }

    @Test
    void isDateTypeWithLocalDate() {
        assertThat(modifier.isDateType(LocalDate.class)).isTrue();
    }

    @Test
    void isDateTypeWithLocalDateTime() {
        assertThat(modifier.isDateType(LocalDateTime.class)).isTrue();
    }

    @Test
    void isDateTypeWithLocalTime() {
        assertThat(modifier.isDateType(LocalTime.class)).isTrue();
    }

    @Test
    void isDateTypeWithNonDate() {
        assertThat(modifier.isDateType(String.class)).isFalse();
    }

    @Test
    void isNumberTypeWithInteger() {
        assertThat(modifier.isNumberType(Integer.class)).isTrue();
    }

    @Test
    void isNumberTypeWithLong() {
        assertThat(modifier.isNumberType(Long.class)).isTrue();
    }

    @Test
    void isNumberTypeWithDouble() {
        assertThat(modifier.isNumberType(Double.class)).isTrue();
    }

    @Test
    void isNumberTypeWithNonNumber() {
        assertThat(modifier.isNumberType(String.class)).isFalse();
    }

    @Test
    void isBooleanTypeWithBoolean() {
        assertThat(modifier.isBooleanType(Boolean.class)).isTrue();
    }

    @Test
    void isBooleanTypeWithNonBoolean() {
        assertThat(modifier.isBooleanType(String.class)).isFalse();
    }

    @Test
    void isBooleanTypeWithPrimitiveBoolean() {
        assertThat(modifier.isBooleanType(boolean.class)).isFalse();
    }
}
