package com.hemju.threadmill.core;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class JobLogTest {

    @Test
    void trimsOldestEntriesWhenEntryLimitIsExceeded() {
        var log = new JobLog();
        log.configureBounds(2, 1_000);

        log.info("first");
        log.info("second");
        log.info("third");

        assertThat(log.snapshot()).extracting(JobLog.Entry::message).containsExactly("second", "third");
    }

    @Test
    void trimsOldestEntriesWhenByteLimitIsExceeded() {
        var log = new JobLog();
        log.configureBounds(10, 5);

        log.info("1234");
        log.info("56");

        assertThat(log.snapshot()).extracting(JobLog.Entry::message).containsExactly("56");
    }
}
