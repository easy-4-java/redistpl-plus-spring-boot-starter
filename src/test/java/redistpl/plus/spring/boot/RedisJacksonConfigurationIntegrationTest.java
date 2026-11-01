package redistpl.plus.spring.boot;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.junit.jupiter.api.Test;
import org.springframework.context.support.GenericApplicationContext;

class RedisJacksonConfigurationIntegrationTest {
    record Label(String value) {}

    public static final class Sample {
        public String getFirstName() { return "Ada"; }
        public String getOptionalValue() { return null; }
    }

    @Test
    void configurationAppliesNamingInclusionDateFormatAndRegisteredModule() throws Exception {
        RedisJacksonProperties properties = new RedisJacksonProperties();
        properties.setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL);
        properties.setPropertyNamingStrategy("SNAKE_CASE");
        properties.setDateFormat("yyyy-MM-dd HH:mm:ss");
        properties.setTimeZone(TimeZone.getTimeZone("UTC"));
        properties.setLocale(Locale.US);
        properties.getSerialization().put(SerializationFeature.INDENT_OUTPUT, true);
        properties.getDeserialization().put(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
        properties.getMapper().put(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
        properties.getParser().put(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        properties.getGenerator().put(JsonGenerator.Feature.ESCAPE_NON_ASCII, true);

        try (GenericApplicationContext context = new GenericApplicationContext()) {
            SimpleModule module = new SimpleModule();
            module.addSerializer(Label.class, new JsonSerializer<>() {
                @Override
                public void serialize(Label value, JsonGenerator generator, SerializerProvider serializers)
                        throws IOException {
                    generator.writeString("label:" + value.value());
                }
            });
            context.registerBean(SimpleModule.class, () -> module);
            context.refresh();
            RedisJacksonConfiguration.JsonMapperBuilderCustomizerConfiguration.StandardJsonMapperBuilderCustomizer
                    customizer = new RedisJacksonConfiguration.JsonMapperBuilderCustomizerConfiguration
                    .StandardJsonMapperBuilderCustomizer(context, properties);
            JsonMapper.Builder builder = JsonMapper.builder();
            customizer.customize(builder);
            ObjectMapper mapper = builder.build();

            assertThat(customizer.getOrder()).isZero();
            assertThat(mapper.writeValueAsString(new Sample()))
                    .contains("first_name").doesNotContain("optional_value");
            assertThat(mapper.writeValueAsString(new Label("ready"))).isEqualTo("\"label:ready\"");
            assertThat(mapper.writeValueAsString(new Date(0))).isEqualTo("\"1970-01-01 00:00:00\"");
            assertThat(mapper.isEnabled(SerializationFeature.INDENT_OUTPUT)).isTrue();
            assertThat(mapper.isEnabled(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)).isTrue();
            assertThat(mapper.isEnabled(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES)).isTrue();
            assertThat(mapper.isEnabled(JsonParser.Feature.ALLOW_SINGLE_QUOTES)).isTrue();
            assertThat(mapper.isEnabled(JsonGenerator.Feature.ESCAPE_NON_ASCII)).isTrue();
        }
    }

    @Test
    void configuredDateFormatClassAndDisabledFeaturesAffectSerialization() throws Exception {
        RedisJacksonProperties properties = new RedisJacksonProperties();
        properties.setDateFormat(SimpleDateFormat.class.getName());
        properties.getSerialization().put(SerializationFeature.FAIL_ON_EMPTY_BEANS, true);
        properties.getSerialization().put(SerializationFeature.INDENT_OUTPUT, false);
        properties.getDeserialization().put(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        properties.getMapper().put(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, false);
        properties.getParser().put(JsonParser.Feature.ALLOW_SINGLE_QUOTES, false);
        properties.getGenerator().put(JsonGenerator.Feature.ESCAPE_NON_ASCII, false);
        try (GenericApplicationContext context = new GenericApplicationContext()) {
            context.refresh();
            var customizer = new RedisJacksonConfiguration.JsonMapperBuilderCustomizerConfiguration
                    .StandardJsonMapperBuilderCustomizer(context, properties);
            JsonMapper.Builder builder = JsonMapper.builder();
            customizer.customize(builder);
            ObjectMapper mapper = builder.build();
            assertThat(mapper.getDateFormat()).isInstanceOf(SimpleDateFormat.class);
            assertThat(mapper.isEnabled(SerializationFeature.FAIL_ON_EMPTY_BEANS)).isTrue();
            assertThat(mapper.isEnabled(SerializationFeature.INDENT_OUTPUT)).isFalse();
            assertThat(mapper.isEnabled(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)).isFalse();
            assertThat(mapper.isEnabled(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES)).isFalse();
            assertThat(mapper.isEnabled(JsonParser.Feature.ALLOW_SINGLE_QUOTES)).isFalse();
            assertThat(mapper.isEnabled(JsonGenerator.Feature.ESCAPE_NON_ASCII)).isFalse();
        }
    }
}
