package redistpl.plus.spring.boot;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.cfg.ConfigFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.json.JsonMapperBuilderCustomizer;
import com.fasterxml.jackson.databind.json.Jsr310JsonMapperBuilderCustomizer;
import hitool.core.lang3.time.DateFormats;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.cache.autoconfigure.CacheAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.util.ObjectMappers;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * RedisJacksonConfiguration.
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(ObjectMapper.class)
@AutoConfigureAfter({CacheAutoConfiguration.class})
@AutoConfigureBefore(DataRedisAutoConfiguration.class)
@EnableConfigurationProperties({RedisJacksonProperties.class})
public class RedisJacksonConfiguration {

	private static final Map<ConfigFeature, Boolean> FEATURE_DEFAULTS;

	static {

		Map<ConfigFeature, Boolean> featureDefaults = new HashMap<>();
		featureDefaults.put(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
		featureDefaults.put(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS, false);
		featureDefaults.put(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
		featureDefaults.put(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

		featureDefaults.put(MapperFeature.ALLOW_FINAL_FIELDS_AS_MUTATORS, true);
		// 使用getter取代setter探测属性，这是针对集合类型，可以直接修改集合的属性
		featureDefaults.put(MapperFeature.USE_GETTERS_AS_SETTERS, true);

		FEATURE_DEFAULTS = Collections.unmodifiableMap(featureDefaults);

	}

	@Bean
    /**
     * <p>Jackson2 json redis serializer.</p>
     * @param customizerProvider
     * @return the jackson2 json redis serializer
     */
	public Jackson2JsonRedisSerializer<Object> jackson2JsonRedisSerializer(ObjectProvider<JsonMapperBuilderCustomizer> customizerProvider) {
		// 1、获取自定义的JsonMapperBuilderCustomizer
		List<JsonMapperBuilderCustomizer> customizers = customizerProvider.orderedStream().collect(Collectors.toList());
		// 2、初始化 ObjectMapper
		ObjectMapper objectMapper = ObjectMappers.defaultObjectMapper(customizers);
		// 3、初始化 Jackson2JsonRedisSerializer<Object>，用于RedisTemplate的序列化和反序列化
		//GenericJackson2JsonRedisSerializer genericJackson2JsonRedisSerializer = new GenericJackson2JsonRedisSerializer(objectMapperProvider.getIfAvailable());
		Jackson2JsonRedisSerializer<Object> jackson2JsonRedisSerializer = new Jackson2JsonRedisSerializer<>(objectMapper, Object.class);
		return jackson2JsonRedisSerializer;
	}


	@Configuration(proxyBeanMethods = false)
	@ConditionalOnClass(JsonMapper.class)
	@EnableConfigurationProperties(RedisJacksonProperties.class)
	static class JsonMapperBuilderCustomizerConfiguration {

		@Bean
		public RedisJacksonConfiguration.JsonMapperBuilderCustomizerConfiguration.StandardJsonMapperBuilderCustomizer standardJsonMapperBuilderCustomizer(
				ApplicationContext applicationContext, RedisJacksonProperties jacksonProperties) {
			return new RedisJacksonConfiguration.JsonMapperBuilderCustomizerConfiguration.StandardJsonMapperBuilderCustomizer(applicationContext, jacksonProperties);
		}

		@Bean
		@ConditionalOnMissingBean
    /**
     * <p>Jsr310 json mapper builder customizer.</p>
     * @param jacksonProperties
     * @return the jsr310 json mapper builder customizer
     */
		public Jsr310JsonMapperBuilderCustomizer jsr310JsonMapperBuilderCustomizer(RedisJacksonProperties jacksonProperties) {
			return new Jsr310JsonMapperBuilderCustomizer(jacksonProperties);
		}

		static final class StandardJsonMapperBuilderCustomizer
				implements JsonMapperBuilderCustomizer, Ordered {

			private final ApplicationContext applicationContext;

			private final RedisJacksonProperties jacksonProperties;

			StandardJsonMapperBuilderCustomizer(ApplicationContext applicationContext,
												RedisJacksonProperties jacksonProperties) {
				this.applicationContext = applicationContext;
				this.jacksonProperties = jacksonProperties;
			}

			@Override
    /**
     * <p>Returns the order.</p>
     * @return the get order
     */
			public int getOrder() {
				return 0;
			}

			@Override
    /**
     * <p>Customize.</p>
     * @param builder
     */
			public void customize(JsonMapper.Builder builder) {

				if (this.jacksonProperties.getDefaultPropertyInclusion() != null) {
					builder.serializationInclusion(this.jacksonProperties.getDefaultPropertyInclusion());
				}
				if (this.jacksonProperties.getTimeZone() != null) {
					builder.defaultTimeZone(this.jacksonProperties.getTimeZone());
				}
				configureFeatures(builder, FEATURE_DEFAULTS);
				configureVisibility(builder, this.jacksonProperties.getVisibility());
				configureFeatures(builder, this.jacksonProperties.getDeserialization());
				configureFeatures(builder, this.jacksonProperties.getSerialization());
				configureFeatures(builder, this.jacksonProperties.getMapper());
				configureFeatures(builder, this.jacksonProperties.getParser());
				configureFeatures(builder, this.jacksonProperties.getGenerator());
				configureDateFormat(builder);
				configurePropertyNamingStrategy(builder);
				configureModules(builder);
				configureLocale(builder);
			}

			private void configureFeatures(JsonMapper.Builder builder, Map<?, Boolean> features) {
				features.forEach((feature, value) -> {
					if (value != null) {
						if (value) {
							if(feature instanceof DeserializationFeature) {
								builder.enable((DeserializationFeature) feature);
							} else if(feature instanceof SerializationFeature) {
								builder.enable((SerializationFeature) feature);
							} else if(feature instanceof MapperFeature) {
								builder.enable((MapperFeature) feature);
							} else if(feature instanceof JsonParser.Feature) {
								builder.enable((JsonParser.Feature) feature);
							} else if(feature instanceof JsonGenerator.Feature) {
								builder.enable((JsonGenerator.Feature) feature);
							}
						}
						else {
							if(feature instanceof DeserializationFeature) {
								builder.disable((DeserializationFeature) feature);
							} else if(feature instanceof SerializationFeature) {
								builder.disable((SerializationFeature) feature);
							} else if(feature instanceof MapperFeature) {
								builder.disable((MapperFeature) feature);
							} else if(feature instanceof JsonParser.Feature) {
								builder.disable((JsonParser.Feature) feature);
							} else if(feature instanceof JsonGenerator.Feature) {
								builder.disable((JsonGenerator.Feature) feature);
							}
						}
					}
				});
			}

			private void configureVisibility(JsonMapper.Builder builder,
											 Map<PropertyAccessor, JsonAutoDetect.Visibility> visibilities) {
				visibilities.forEach(builder::visibility);
			}

			private void configureDateFormat(JsonMapper.Builder builder) {
				// We support a fully qualified class name extending DateFormat or a date
				// pattern string value
				String dateFormat = Optional.ofNullable(this.jacksonProperties.getDateFormat()).orElse(DateFormats.DATE_LONGFORMAT);
				if (dateFormat != null) {
					try {
						Class<?> dateFormatClass = ClassUtils.forName(dateFormat, null);
						builder.defaultDateFormat((DateFormat) BeanUtils.instantiateClass(dateFormatClass));
					}
					catch (ClassNotFoundException ex) {
						SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
						// Since Jackson 2.6.3 we always need to set a TimeZone (see
						// gh-4170). If none in our properties fallback to the Jackson's
						// default
						TimeZone timeZone = this.jacksonProperties.getTimeZone();
						if (timeZone == null) {
							timeZone = new ObjectMapper().getSerializationConfig().getTimeZone();
						}
						simpleDateFormat.setTimeZone(timeZone);
						builder.defaultDateFormat(simpleDateFormat);
					}
				}
			}

			private void configurePropertyNamingStrategy(JsonMapper.Builder builder) {
				// We support a fully qualified class name extending Jackson's
				// PropertyNamingStrategy or a string value corresponding to the constant
				// names in PropertyNamingStrategy which hold default provided
				// implementations
				String strategy = this.jacksonProperties.getPropertyNamingStrategy();
				if (strategy != null) {
					try {
						configurePropertyNamingStrategyClass(builder, ClassUtils.forName(strategy, null));
					}
					catch (ClassNotFoundException ex) {
						configurePropertyNamingStrategyField(builder, strategy);
					}
				}
			}

			private void configurePropertyNamingStrategyClass(JsonMapper.Builder builder,
															  Class<?> propertyNamingStrategyClass) {
				builder.propertyNamingStrategy((PropertyNamingStrategy) BeanUtils.instantiateClass(propertyNamingStrategyClass));
			}

			private void configurePropertyNamingStrategyField(JsonMapper.Builder builder, String fieldName) {
				// Find the field (this way we automatically support new constants
				// that may be added by Jackson in the future)
				Field legacyField = ReflectionUtils.findField(PropertyNamingStrategy.class, fieldName,
						PropertyNamingStrategy.class);
				Field field = legacyField != null ? legacyField
						: ReflectionUtils.findField(PropertyNamingStrategies.class, fieldName,
								PropertyNamingStrategy.class);
				Assert.notNull(field, () -> "Constant named '" + fieldName + "' not found on "
						+ PropertyNamingStrategy.class.getName());
				try {
					builder.propertyNamingStrategy((PropertyNamingStrategy) field.get(null));
				}
				catch (Exception ex) {
					throw new IllegalStateException(ex);
				}
			}

			private void configureModules(JsonMapper.Builder builder) {
				Collection<Module> moduleBeans = getBeans(this.applicationContext, Module.class);
				builder.addModules(moduleBeans.toArray(new Module[0]));
			}

			private void configureLocale(JsonMapper.Builder builder) {
				Locale locale = this.jacksonProperties.getLocale();
				if (locale != null) {
					builder.defaultLocale(locale);
				}
			}

			private static <T> Collection<T> getBeans(ListableBeanFactory beanFactory, Class<T> type) {
				return BeanFactoryUtils.beansOfTypeIncludingAncestors(beanFactory, type).values();
			}


		}

	}
}

