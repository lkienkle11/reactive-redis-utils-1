package com.microservices.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.experimental.FieldDefaults;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Component
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class ReactiveRedisUtils {
    @Getter
    ReactiveRedisTemplate<String, Object> reactiveRedisTemplate;
    ObjectMapper objectMapper;

    public ReactiveRedisUtils(ReactiveRedisTemplate<String, Object> reactiveRedisTemplate) {
        this.reactiveRedisTemplate = reactiveRedisTemplate;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.objectMapper.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
    }

    public Mono<Boolean> hasKey(String key) {
        return reactiveRedisTemplate.hasKey(key);
    }

    public Mono<Object> getFromRedis(String key) {
        return reactiveRedisTemplate.opsForValue().get(key);
    }

    public <T> Mono<T> getFromRedis(String key, Class<T> clazz) {
        return reactiveRedisTemplate.opsForValue().get(key)
                .map(data -> objectMapper.convertValue(data, clazz));
    }

    public Flux<Object> multiGetFromRedis(Collection<String> keys) {
        if (keys == null || keys.isEmpty()) {
            return Flux.empty();
        }

        return reactiveRedisTemplate.opsForValue()
                .multiGet(keys)
                .flatMapMany(Flux::fromIterable) // Chuyển List<Object> thành Flux<Object>
                .switchIfEmpty(Flux.empty());
    }

    public Mono<Void> saveToRedis(String key, Object value) {
        return reactiveRedisTemplate
                .opsForValue()
                .set(key, value)
                .flatMap(saved -> {
                    if (!saved) {
                        return Mono.error(new IllegalStateException("Failed to save key: " + key));
                    }
                    return Mono.empty();
                });
    }

    public Mono<Void> saveToRedis(String key, Object value, long timeout, TimeUnit unit) {
        Duration ttl = Duration.ofMillis(unit.toMillis(timeout));
        return reactiveRedisTemplate
                .opsForValue()
                .set(key, value, ttl)
                .flatMap(saved -> {
                    if (!saved) {
                        return Mono.error(new IllegalStateException("Failed to save key: " + key));
                    }
                    return Mono.empty();
                });
    }

    public Mono<Void> multiSaveToRedis(Map<String, Object> map) {
        return multiSaveToRedis(map, null);
    }

    public Mono<Void> multiSaveToRedis(Map<String, Object> map, Duration ttl) {
        if (map == null || map.isEmpty()) {
            return Mono.empty();
        }

        // Lưu tất cả key-value bằng multiSet
        return reactiveRedisTemplate.opsForValue()
                .multiSet(map)
                .then(Mono.defer(() -> {
                    if (ttl != null && !ttl.isZero()) {
                        // Thiết lập TTL chung cho tất cả khóa
                        return Mono.when(map.keySet().stream()
                                .map(key -> reactiveRedisTemplate.expire(key, ttl))
                                .toList());
                    }
                    return Mono.empty();
                }));
    }

    public Mono<Void> multiSaveToRedisWithIndividualTTL(Map<String, Object> map, Map<String, Duration> ttlMap) {
        if (map == null || map.isEmpty()) {
            return Mono.empty();
        }

        // Lưu tất cả key-value bằng multiSet
        return reactiveRedisTemplate.opsForValue()
                .multiSet(map)
                .then(Mono.defer(() -> {
                    if (ttlMap != null && !ttlMap.isEmpty()) {
                        // Thiết lập TTL riêng cho từng khóa
                        return Mono.when(map.keySet().stream()
                                .filter(ttlMap::containsKey)
                                .map(key -> reactiveRedisTemplate.expire(key, ttlMap.get(key)))
                                .toList());
                    }
                    return Mono.empty();
                }));
    }

    public Flux<Object> getFromSet(String key) {
        return reactiveRedisTemplate
                .opsForSet()
                .members(key);
    }

    public <T> Flux<T> getFromSet(String key, Class<T> clazz) {
        return reactiveRedisTemplate
                .opsForSet()
                .members(key)
                .map(data -> objectMapper.convertValue(data, clazz));
    }

    public Mono<Long> saveToSet(String key, Object value) {
        return reactiveRedisTemplate.opsForSet().add(key, value);
    }

    public Mono<Long> saveToSet(String key, Object value, long timeout, TimeUnit unit) {
        Duration ttl = Duration.ofMillis(unit.toMillis(timeout));

        return reactiveRedisTemplate
                .opsForSet()
                .add(key, value)
                .flatMap(saved -> {
                    if (saved == 0L) {
                        return Mono.error(new IllegalStateException("Failed to add value to set: " + key));
                    }
                    return reactiveRedisTemplate
                            .expire(key, ttl)
                            .flatMap(expired -> {
                                if (!expired) {
                                    return Mono.error(new IllegalStateException("Failed to set TTL for key: " + key));
                                }
                                return Mono.just(saved);
                            });
                });
    }

    public Mono<Boolean> isMember(String key, Object value) {
        return reactiveRedisTemplate.opsForSet().isMember(key, value);
    }

    public Mono<List<Object>> multiGet(List<String> keys) {
        return reactiveRedisTemplate.opsForValue().multiGet(keys);
    }

    public Mono<Long> publish(String channel, Object message) {
        return reactiveRedisTemplate.convertAndSend(channel, message);
    }

    public Flux<String> subscribe(String channel) {
        return reactiveRedisTemplate
                .listenToChannel(channel)
                .map(pubSubMessage -> {
                    Object raw = pubSubMessage.getMessage();
                    try {
                        if (raw instanceof String) {
                            return (String) raw;
                        }
                        return objectMapper.writeValueAsString(raw);
                    } catch (JsonProcessingException e) {
                        return "ERROR_PARSING:" + e.getMessage();
                    }
                });
    }

    public <T> Flux<T> subscribe(String channel, Class<T> clazz) {
        return subscribe(channel)
                .map(json -> objectMapper.convertValue(json, clazz));
    }
}
