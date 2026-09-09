package com.hemju.threadmill.core.internal;

/** Internal allocation-free sizing with the JDK UTF-8 encoder's replacement semantics. */
public final class Utf8 {
  private Utf8() {}

  public static long length(String value) {
    long bytes = 0;
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if (c < 0x80) bytes++;
      else if (c < 0x800) bytes += 2;
      else if (Character.isHighSurrogate(c)
          && i + 1 < value.length()
          && Character.isLowSurrogate(value.charAt(i + 1))) {
        bytes += 4;
        i++;
      } else bytes += Character.isSurrogate(c) ? 1 : 3;
    }
    return bytes;
  }
}
