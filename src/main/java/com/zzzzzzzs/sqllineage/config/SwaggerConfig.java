package com.zzzzzzzs.sqllineage.config;

import com.github.xiaoymin.knife4j.spring.annotations.EnableKnife4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.service.Contact;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

@Configuration // 让Spring来加载该类配置
@EnableKnife4j // 增强
@EnableSwagger2 // 启用Swagger2
public class SwaggerConfig {
  @Bean
  public Docket dataManagerApi() {
    return new Docket(DocumentationType.SWAGGER_2)
        .groupName("sqlLineage")
        .apiInfo(apiInfo())
        .select()
        .apis(RequestHandlerSelectors.basePackage("com.zzzzzzzs.sqllineage.controller"))
        .paths(PathSelectors.any())
        .build();
  }

  private ApiInfo apiInfo() {
    return new ApiInfoBuilder()
        .title("sqllineage")
        .description("sqllineage")
        .termsOfServiceUrl("-----")
        .contact(new Contact("zs", "----", "zhaoshuo0701@163.com"))
        .version("0.0.1")
        .build();
  }
}
