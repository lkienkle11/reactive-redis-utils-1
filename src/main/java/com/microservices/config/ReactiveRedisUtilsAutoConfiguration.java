package com.microservices.config;

import com.microservices.utils.ReactiveRedisUtils;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.web.server.WebFilter;

@AutoConfiguration
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.REACTIVE)
@ConditionalOnClass({WebFilter.class})
public class ReactiveRedisUtilsAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    public ReactiveRedisUtils reactiveRedisUtils(
            ReactiveRedisTemplate<String,Object> reactiveRedisTemplate) {
        return new ReactiveRedisUtils(reactiveRedisTemplate);
    }
}
