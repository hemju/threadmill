package com.hemju.threadmill.spring;

/**
 * Column types for application-supplied text in an auto-configured Oracle
 * store ({@code threadmill.store.oracle.text-type}). Mirrors
 * {@code com.hemju.threadmill.store.oracle.OracleTextType}, which this
 * properties type cannot reference because the Oracle module is optional.
 */
public enum OracleStoreTextType {

  /**
   * {@code VARCHAR2}/{@code CLOB} columns in the database character set, which
   * must be {@code AL32UTF8}. The default.
   */
  STANDARD,

  /**
   * {@code NVARCHAR2}/{@code NCLOB} columns in the {@code AL16UTF16} national
   * character set, for databases whose character set cannot hold all of
   * Unicode, such as {@code WE8MSWIN1252}.
   */
  NATIONAL
}
