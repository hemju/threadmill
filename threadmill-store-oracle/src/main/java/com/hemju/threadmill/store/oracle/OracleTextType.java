package com.hemju.threadmill.store.oracle;

/**
 * Column types Threadmill uses for text that applications supply: queue and
 * recurring-task names, handler and payload type names, concurrency and
 * deduplication keys, mutex names and holders, pause reasons, and the job
 * bodies and recurring payloads.
 *
 * <p>Columns holding Threadmill's own fixed ASCII values (job states,
 * concurrency modes, trigger kinds, missed-run policies, time-zone ids, lease
 * names) are {@code VARCHAR2} with either type.
 *
 * <p>The schema is created for one text type and the store must be configured
 * with the same one: the type decides both the column types and how values
 * are bound to them. A value bound in the database character set loses every
 * character that set cannot represent before it reaches an {@code NVARCHAR2}
 * column, and a national-character bind compared with a {@code VARCHAR2}
 * column converts the column, so its index can no longer be used.
 */
public enum OracleTextType {

  /**
   * {@code VARCHAR2} and {@code CLOB} columns in the database character set,
   * which must be {@code AL32UTF8}. The default, and the right choice for every
   * Unicode database.
   */
  STANDARD,

  /**
   * {@code NVARCHAR2} and {@code NCLOB} columns in the national character set,
   * which must be {@code AL16UTF16}. For databases whose character set cannot
   * hold all of Unicode, for example {@code WE8MSWIN1252}: every character,
   * including supplementary ones such as emoji, round-trips whatever the
   * database character set is. Lengths of these columns count UTF-16 code
   * units, the unit of Java's {@link String#length()}.
   */
  NATIONAL
}
