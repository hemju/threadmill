package com.hemju.threadmill.dashboard.spring;

import java.util.Objects;
import java.util.Optional;

import org.springframework.core.ResolvableType;

import com.hemju.threadmill.core.engine.JobRunner;
import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobPayload;
import com.hemju.threadmill.core.serialization.JobSerializer;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.dashboard.api.DashboardApiException;
import com.hemju.threadmill.dashboard.api.DashboardJobDefinitionValidator;

/** Spring-aware validation of dashboard-supplied handler and payload definitions. */
final class SpringDashboardJobDefinitionValidator implements DashboardJobDefinitionValidator {

  private final ClassLoader classLoader;
  private final Optional<JobSerializer> serializer;

  SpringDashboardJobDefinitionValidator(
      ClassLoader classLoader, Optional<JobSerializer> serializer) {
    this.classLoader = Objects.requireNonNull(classLoader, "classLoader");
    this.serializer = Objects.requireNonNull(serializer, "serializer");
  }

  @Override
  public void validate(JobSpec replacement) {
    Objects.requireNonNull(replacement, "replacement");
    Class<?> handlerType = loadHandler(replacement.handlerType());
    Class<?> payloadType =
        ResolvableType.forClass(handlerType).as(JobHandler.class).getGeneric(0).resolve();
    if (payloadType == null || !JobPayload.class.isAssignableFrom(payloadType)) {
      throw badRequest("cannot infer the JobPayload type for handler " + handlerType.getName());
    }
    validateArguments(replacement, payloadType);
  }

  private Class<?> loadHandler(String handlerTypeName) {
    try {
      Class<?> handlerType = Class.forName(handlerTypeName, false, classLoader);
      if (!JobHandler.class.isAssignableFrom(handlerType)) {
        throw badRequest("type " + handlerTypeName + " does not implement JobHandler");
      }
      return handlerType;
    } catch (ClassNotFoundException e) {
      throw badRequest("handler type not found: " + handlerTypeName);
    } catch (LinkageError e) {
      throw badRequest("handler type could not be loaded: " + handlerTypeName);
    }
  }

  private void validateArguments(JobSpec replacement, Class<?> payloadType) {
    var arguments = replacement.arguments();
    if (arguments.isEmpty()) {
      if (!payloadType.isAssignableFrom(JobRunner.EmptyPayload.class)) {
        throw badRequest(
            "handler " + replacement.handlerType() + " requires payload " + payloadType.getName());
      }
      return;
    }
    if (arguments.size() != 1) {
      throw badRequest("job definitions currently accept exactly one payload argument");
    }
    JobArgument argument = arguments.getFirst();
    if (argument == null) {
      throw badRequest("payload argument must not be null");
    }
    JobArgument migrated =
        serializer.map(value -> value.migrateArgument(argument)).orElse(argument);
    String resolvedType = serializer
        .map(value -> value.resolveTypeTag(migrated.typeTag()))
        .orElse(migrated.typeTag());
    Class<?> argumentType = loadPayload(resolvedType);
    if (!payloadType.isAssignableFrom(argumentType)) {
      throw badRequest("payload type "
          + argumentType.getName()
          + " is not compatible with handler "
          + replacement.handlerType()
          + " (expects "
          + payloadType.getName()
          + ")");
    }
    serializer.ifPresent(
        value -> deserialize(value, argument, argumentType, replacement.handlerType()));
  }

  private Class<?> loadPayload(String payloadTypeName) {
    try {
      Class<?> payloadType = Class.forName(payloadTypeName, false, classLoader);
      if (!JobPayload.class.isAssignableFrom(payloadType)) {
        throw badRequest("argument type " + payloadTypeName + " does not implement JobPayload");
      }
      return payloadType;
    } catch (ClassNotFoundException e) {
      throw badRequest("payload type not found: " + payloadTypeName);
    } catch (LinkageError e) {
      throw badRequest("payload type could not be loaded: " + payloadTypeName);
    }
  }

  @SuppressWarnings("unchecked")
  private static void deserialize(
      JobSerializer serializer,
      JobArgument argument,
      Class<?> argumentType,
      String handlerTypeName) {
    try {
      serializer.deserializePayload(argument, (Class<? extends JobPayload>) argumentType);
    } catch (RuntimeException e) {
      throw badRequest("payload cannot be deserialized as "
          + argumentType.getName()
          + " for handler "
          + handlerTypeName);
    }
  }

  private static DashboardApiException badRequest(String message) {
    return DashboardApiException.badRequest(message);
  }
}
