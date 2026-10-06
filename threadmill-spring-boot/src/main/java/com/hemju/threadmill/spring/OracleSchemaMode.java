package com.hemju.threadmill.spring;

/** Oracle schema action for Spring Boot auto-configured Threadmill stores. */
public enum OracleSchemaMode {
  /**
   * Apply every pending Threadmill migration before creating the store,
   * resuming a migration that a crash interrupted.
   */
  MIGRATE,

  /** Validate that the current schema exactly matches the shipped migration set. */
  VALIDATE,

  /** Do not inspect or modify the schema. */
  NONE,

  /** Drop Threadmill-owned schema objects, then apply all migrations. */
  DROP_AND_MIGRATE
}
