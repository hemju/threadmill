package com.hemju.threadmill.core.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

class LocalWakeBusTest {

    @Test
    void wakeWithNoSinksIsNoop() {
        var bus = new LocalWakeBus();
        bus.wake("alpha"); // must not throw
    }

    @Test
    void wakeDispatchesToEverySink() {
        var bus = new LocalWakeBus();
        var calls = new ArrayList<String>();
        bus.register(calls::add);
        bus.register(q -> calls.add("two:" + q));

        bus.wake("alpha");

        assertThat(calls).containsExactly("alpha", "two:alpha");
    }

    @Test
    void faultySinkDoesNotPreventLaterSinks() {
        var bus = new LocalWakeBus();
        var seen = new ArrayList<String>();
        bus.register(q -> {
            throw new RuntimeException("boom");
        });
        bus.register(seen::add);

        bus.wake("alpha"); // must not propagate

        assertThat(seen).containsExactly("alpha");
    }

    @Test
    void multipleWakesSinkAsManyTimes() {
        var bus = new LocalWakeBus();
        var counts = new ArrayList<String>();
        bus.register(counts::add);

        bus.wake("alpha");
        bus.wake("beta");
        bus.wake("alpha");

        assertThat(counts).containsExactly("alpha", "beta", "alpha");
    }

    @Test
    void registeringDoesNotLeakSinksAcrossInstances() {
        var bus1 = new LocalWakeBus();
        var bus2 = new LocalWakeBus();
        List<String> sink1Calls = new ArrayList<>();
        bus1.register(sink1Calls::add);

        bus2.wake("alpha");

        assertThat(sink1Calls).isEmpty();
    }

    @Test
    void registrationHandleRemovesSink() {
        var bus = new LocalWakeBus();
        var calls = new ArrayList<String>();
        Runnable unregister = bus.register(calls::add);

        bus.wake("alpha");
        unregister.run();
        bus.wake("beta");

        assertThat(calls).containsExactly("alpha");
    }
}
