package com.zzzzzzzs.sqllineage.config;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;
import java.util.Arrays;

/**
 * @author zs
 * @date 2021/12/11
 */
@Component
@Aspect
@Slf4j
public class LogAspect {

  /**
   * 定义一个切入点 只拦截controller. 解释下： ~ 第一个 * 代表任意修饰符及任意返回值. ~ 第二个 * 定义在web包或者子包 ~ 第三个 * 任意方法 ~ ..
   * 匹配任意数量的参数.
   */
  @Pointcut("execution(* com.zzzzzzzs.sqllineage.controller..*.*(..))")
  public void logPointcut() {}

  // around(和上面的方法名一样)
  @org.aspectj.lang.annotation.Around("logPointcut()")
  public Object doAround(ProceedingJoinPoint joinPoint) throws Throwable {
    log.info(
        "=====================================Method  start====================================");
    ServletRequestAttributes attributes =
        (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
    HttpServletRequest request = attributes.getRequest();
    long start = System.currentTimeMillis();
    try {
      Object result = joinPoint.proceed();
      long end = System.currentTimeMillis();
      log.info("请求地址:" + request.getRequestURI());
      log.info("用户IP:" + request.getRemoteAddr());
      log.info(
          "CLASS_METHOD : "
              + joinPoint.getSignature().getDeclaringTypeName()
              + "."
              + joinPoint.getSignature().getName());
      log.info("参数: " + Arrays.toString(joinPoint.getArgs()));
      log.info("执行时间: " + (end - start) + " ms!");
      log.info(
          "=====================================Method  End====================================");
      return result;
    } catch (Throwable e) {
      long end = System.currentTimeMillis();
      log.info("URL:" + request.getRequestURI());
      log.info("IP:" + request.getRemoteAddr());
      log.info(
          "CLASS_METHOD : "
              + joinPoint.getSignature().getDeclaringTypeName()
              + "."
              + joinPoint.getSignature().getName());
      log.info("ARGS : " + Arrays.toString(joinPoint.getArgs()));
      log.info("执行时间: " + (end - start) + " ms!");
      log.info(
          "=====================================Method  End====================================");
      throw e;
    }
  }
}
