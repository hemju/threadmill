package com.hemju.threadmill.spring;

import java.util.Locale;

import org.springframework.boot.autoconfigure.condition.ConditionOutcome;
import org.springframework.boot.autoconfigure.condition.SpringBootCondition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.util.ClassUtils;

/**
 * Decides which JDBC store auto-configuration owns the application's
 * {@code DataSource} when more than one store module is on the classpath.
 *
 * <p>Precedence: {@code threadmill.store.jdbc-type}, then the scheme of
 * {@code spring.datasource.url}, then the store modules present (PostgreSQL
 * when both are, preserving the behaviour from before the Oracle store).
 * Decided from configuration rather than a database round trip, because
 * conditions run before any bean exists; the chosen store still verifies the
 * server at startup and fails fast on a mismatch.
 */
final class JdbcStoreSelection {

  static final String PROPERTY = "threadmill.store.jdbc-type";

  private static final String POSTGRES_STORE =
      "com.hemju.threadmill.store.postgres.PostgresJobStore";
  private static final String ORACLE_STORE = "com.hemju.threadmill.store.oracle.OracleJobStore";

  private JdbcStoreSelection() {}

  static JdbcStoreType select(Environment environment, ClassLoader classLoader) {
    String configured = environment.getProperty(PROPERTY);
    if (configured != null && !configured.isBlank()) {
      return JdbcStoreType.valueOf(configured.trim().replace('-', '_').toUpperCase(Locale.ROOT));
    }
    String url = environment.getProperty("spring.datasource.url", "");
    if (url.startsWith("jdbc:oracle:")) {
      return JdbcStoreType.ORACLE;
    }
    if (url.startsWith("jdbc:postgresql:")) {
      return JdbcStoreType.POSTGRES;
    }
    boolean postgres = ClassUtils.isPresent(POSTGRES_STORE, classLoader);
    boolean oracle = ClassUtils.isPresent(ORACLE_STORE, classLoader);
    return oracle && !postgres ? JdbcStoreType.ORACLE : JdbcStoreType.POSTGRES;
  }

  private abstract static class Selected extends SpringBootCondition {
    private final JdbcStoreType type;

    Selected(JdbcStoreType type) {
      this.type = type;
    }

    @Override
    public ConditionOutcome getMatchOutcome(
        ConditionContext context, AnnotatedTypeMetadata metadata) {
      JdbcStoreType selected = select(context.getEnvironment(), context.getClassLoader());
      String message = "Threadmill JDBC store selection chose " + selected;
      return selected == type ? ConditionOutcome.match(message) : ConditionOutcome.noMatch(message);
    }
  }

  /** Matches when the PostgreSQL store owns the {@code DataSource}. */
  static final class PostgresSelected extends Selected {
    PostgresSelected() {
      super(JdbcStoreType.POSTGRES);
    }
  }

  /** Matches when the Oracle store owns the {@code DataSource}. */
  static final class OracleSelected extends Selected {
    OracleSelected() {
      super(JdbcStoreType.ORACLE);
    }
  }
}
