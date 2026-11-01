package com.fasterxml.jackson.databind.json;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.junit.jupiter.api.Test;
import redistpl.plus.spring.boot.RedisJacksonProperties;

class Jsr310JsonMapperBuilderCustomizerTest {
    record Label(String value) {}

    static final class LabelSerializer extends StdSerializer<Label> {
        LabelSerializer() { super(Label.class); }

        @Override
        public void serialize(Label value, JsonGenerator generator, SerializerProvider provider) throws IOException {
            generator.writeString("label:" + value.value());
        }
    }

    static final class LabelDeserializer extends StdDeserializer<Label> {
        LabelDeserializer() { super(Label.class); }

        @Override
        public Label deserialize(JsonParser parser, DeserializationContext context) throws IOException {
            return new Label(parser.getValueAsString().substring("label:".length()));
        }
    }

    @Test
    void configuredDateFormatRoundTripsAndTimestampInputUsesLocalZone() throws Exception {
        RedisJacksonProperties properties = new RedisJacksonProperties();
        properties.setDateFormat("yyyy/MM/dd HH:mm:ss");
        Jsr310JsonMapperBuilderCustomizer customizer = new Jsr310JsonMapperBuilderCustomizer(properties);
        JsonMapper.Builder builder = JsonMapper.builder();
        customizer.customize(builder);
        JsonMapper mapper = builder.build();
        LocalDateTime value = LocalDateTime.of(2026, 10, 31, 12, 34, 56);
        assertThat(mapper.writeValueAsString(value)).isEqualTo("\"2026/10/31 12:34:56\"");
        assertThat(mapper.readValue("\"2026/10/31 12:34:56\"", LocalDateTime.class)).isEqualTo(value);
        assertThat(mapper.readValue("0", LocalDateTime.class)).isNull();
        assertThat(mapper.readValue("1609459200000", LocalDateTime.class))
                .isEqualTo(LocalDateTime.ofInstant(Instant.ofEpochMilli(1609459200000L), ZoneId.systemDefault()));
        assertThat(customizer.getOrder()).isEqualTo(1);
    }

    @Test
    void customSerializerAndDeserializerHandleRegisteredType() throws Exception {
        Jsr310JsonMapperBuilderCustomizer customizer =
                new Jsr310JsonMapperBuilderCustomizer(new RedisJacksonProperties());
        customizer.serializers(new LabelSerializer()).deserializers(new LabelDeserializer());
        JsonMapper.Builder builder = JsonMapper.builder();
        customizer.customize(builder);
        JsonMapper mapper = builder.build();
        assertThat(mapper.writeValueAsString(new Label("ready"))).isEqualTo("\"label:ready\"");
        assertThat(mapper.readValue("\"label:ready\"", Label.class)).isEqualTo(new Label("ready"));
    }

    @Test
    void unknownSerializerTypeIsRejected() {
        Jsr310JsonMapperBuilderCustomizer customizer =
                new Jsr310JsonMapperBuilderCustomizer(new RedisJacksonProperties());
        assertThatThrownBy(() -> customizer.serializers(new com.fasterxml.jackson.databind.JsonSerializer<>() {
            @Override
            public void serialize(Object value, JsonGenerator generator, SerializerProvider provider) throws IOException {
                generator.writeNull();
            }
        })).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown handled type");
    }
}
