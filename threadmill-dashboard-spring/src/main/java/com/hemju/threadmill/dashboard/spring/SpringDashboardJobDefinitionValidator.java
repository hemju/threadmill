package com.hemju.threadmill.dashboard.spring;

import static com.hemju.threadmill.dashboard.api.DashboardApiException.badRequest;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Objects;

import org.springframework.core.ResolvableType;
import org.springframework.util.ClassUtils;

import com.hemju.threadmill.core.handler.JobHandler;
import com.hemju.threadmill.core.handler.JobPayload;
import com.hemju.threadmill.core.serialization.JobSerializer;
import com.hemju.threadmill.core.serialization.TypeNameAliases;
import com.hemju.threadmill.core.spec.JobArgument;
import com.hemju.threadmill.core.spec.JobSpec;
import com.hemju.threadmill.dashboard.api.DashboardJobDefinitionValidator;

/** Spring-aware validation of dashboard-supplied handler and payload definitions. */
final class SpringDashboardJobDefinitionValidator implements DashboardJobDefinitionValidator {

  private final ClassLoader classLoader;
  private final JobSerializer serializer;
  private final TypeNameAliases aliases;

  SpringDashboardJobDefinitionValidator(
      ClassLoader classLoader, JobSerializer serializer, TypeNameAliases aliases) {
    this.classLoader = Objects.requireNonNull(classLoader, "classLoader");
    this.serializer = Objects.requireNonNull(serializer, "serializer");
    this.aliases = Objects.requireNonNull(aliases, "aliases");
  }

  @Override
  public void validate(JobSpec replacement) {
    Objects.requireNonNull(replacement, "replacement");
    Class<?> handlerType = loadHandler(replacement.handlerType());
    Class<?> payloadType;
    try {
      if (implementsJobHandlerRaw(handlerType)) {
        throw badRequest("handler "
            + replacement.handlerType()
            + " implements raw JobHandler; use JobHandler<P> or JobAction");
      }
      payloadType = ResolvableType.forClass(handlerType)
          .as(JobHandler.class)
          .getGeneric(0)
          .resolve();
    } catch (LinkageError | TypeNotPresentException e) {
      throw badRequest(
          "cannot resolve the JobPayload type for handler " + replacement.handlerType());
    }
    if (payloadType == null || !JobPayload.class.isAssignableFrom(payloadType)) {
      throw badRequest("cannot infer the JobPayload type for handler " + handlerType.getName());
    }
    validateArguments(replacement, payloadType);
  }

  private Class<?> loadHandler(String handlerTypeName) {
    String resolvedName = aliases.resolve(handlerTypeName);
    try {
      Class<?> handlerType = ClassUtils.forName(resolvedName, classLoader);
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
      throw badRequest("handler "
          + replacement.handlerType()
          + " requires exactly one payload argument of type "
          + payloadType.getName());
    }
    if (arguments.size() != 1) {
      throw badRequest("job definitions currently accept exactly one payload argument");
    }
    JobArgument argument = arguments.getFirst();
    JobArgument migrated = serializer.migrateArgument(argument);
    String resolvedType = serializer.resolveTypeTag(migrated.typeTag());
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
    deserialize(serializer, argument, argumentType, replacement.handlerType());
  }

  private Class<?> loadPayload(String payloadTypeName) {
    try {
      Class<?> payloadType = ClassUtils.forName(payloadTypeName, classLoader);
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

  private static boolean implementsJobHandlerRaw(Class<?> handlerType) {
    Class<?> cursor = handlerType;
    while (cursor != null && cursor != Object.class) {
      for (Type iface : cursor.getGenericInterfaces()) {
        if (iface == JobHandler.class) return true;
        if (iface instanceof ParameterizedType parameterized
            && parameterized.getRawType() == JobHandler.class) {
          return false;
        }
      }
      cursor = cursor.getSuperclass();
    }
    return false;
  }
}
