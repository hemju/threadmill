package com.hemju.threadmill.core.schedule;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.BitSet;
import java.util.Objects;

/**
 * A standard 5-field cron expression: {@code minute hour dayOfMonth month dayOfWeek}.
 *
 * <p>Supports the standard pieces: wildcard, single value, range {@code a-b},
 * list {@code a,b,c}, and step suffix {@code /n}. Day-of-week is Sunday=0 through
 * Saturday=6 (also Sunday=7 accepted). Day-of-month and day-of-week match
 * with OR-semantics when both are restricted, per the original cron
 * convention.
 *
 * <p>This parser is intentionally minimal. Richer expressions (business
 * days, last-day-of-month, etc.) can be added by subclassing or composing
 * with this class — its API is deliberately small for that reason.
 */
public final class CronExpression {

    private final String expression;
    private final BitSet minutes;
    private final BitSet hours;
    private final BitSet daysOfMonth;
    private final BitSet months;
    private final BitSet daysOfWeek;
    private final boolean domRestricted;
    private final boolean dowRestricted;

    private CronExpression(
            String expression,
            BitSet minutes,
            BitSet hours,
            BitSet daysOfMonth,
            BitSet months,
            BitSet daysOfWeek,
            boolean domRestricted,
            boolean dowRestricted) {
        this.expression = expression;
        this.minutes = minutes;
        this.hours = hours;
        this.daysOfMonth = daysOfMonth;
        this.months = months;
        this.daysOfWeek = daysOfWeek;
        this.domRestricted = domRestricted;
        this.dowRestricted = dowRestricted;
    }

    public String expression() {
        return expression;
    }

    public static CronExpression parse(String expression) {
        Objects.requireNonNull(expression, "expression");
        String[] parts = expression.trim().split("\\s+");
        if (parts.length != 5) {
            throw new IllegalArgumentException("cron expression must have 5 fields, got " + parts.length);
        }
        BitSet min = parseField(parts[0], 0, 59);
        BitSet hour = parseField(parts[1], 0, 23);
        BitSet dom = parseField(parts[2], 1, 31);
        BitSet mon = parseField(parts[3], 1, 12);
        BitSet dow = parseField(normaliseSunday(parts[4]), 0, 6);
        return new CronExpression(expression, min, hour, dom, mon, dow, !isStar(parts[2]), !isStar(parts[4]));
    }

    /**
     * Compute the next fire time strictly after {@code after}. Returns the
     * computed {@link Instant} in the supplied zone.
     */
    public Instant nextAfter(Instant after, ZoneId zone) {
        Objects.requireNonNull(after, "after");
        Objects.requireNonNull(zone, "zone");
        ZonedDateTime zdt = after.atZone(zone).withSecond(0).withNano(0).plusMinutes(1);
        for (int safety = 0; safety < 525_600; safety++) { // <= 1 year of minutes
            int month = zdt.get(ChronoField.MONTH_OF_YEAR);
            int dom = zdt.get(ChronoField.DAY_OF_MONTH);
            int dow = zdt.get(ChronoField.DAY_OF_WEEK) % 7; // ISO Mon=1..Sun=7 → cron Sun=0..Sat=6
            int hour = zdt.get(ChronoField.HOUR_OF_DAY);
            int minute = zdt.get(ChronoField.MINUTE_OF_HOUR);

            if (!months.get(month)) {
                zdt = zdt.plusMonths(1).withDayOfMonth(1).withHour(0).withMinute(0);
                continue;
            }
            boolean dayOk = matchesDay(dom, dow);
            if (!dayOk) {
                zdt = zdt.plusDays(1).withHour(0).withMinute(0);
                continue;
            }
            if (!hours.get(hour)) {
                zdt = zdt.plusHours(1).withMinute(0);
                continue;
            }
            if (!minutes.get(minute)) {
                zdt = zdt.plusMinutes(1);
                continue;
            }
            return zdt.toInstant();
        }
        throw new IllegalStateException("cron expression " + expression + " produced no next fire within a year");
    }

    private boolean matchesDay(int dom, int dow) {
        if (domRestricted && dowRestricted) {
            // Classic cron OR semantics: either restriction satisfied.
            return daysOfMonth.get(dom) || daysOfWeek.get(dow);
        }
        return daysOfMonth.get(dom) && daysOfWeek.get(dow);
    }

    private static BitSet parseField(String field, int min, int max) {
        var b = new BitSet(max + 1);
        for (String part : field.split(",")) {
            int step = 1;
            String body = part;
            int slash = part.indexOf('/');
            if (slash >= 0) {
                step = Integer.parseInt(part.substring(slash + 1));
                body = part.substring(0, slash);
            }
            int start;
            int end;
            if ("*".equals(body)) {
                start = min;
                end = max;
            } else if (body.contains("-")) {
                String[] r = body.split("-");
                start = Integer.parseInt(r[0]);
                end = Integer.parseInt(r[1]);
            } else {
                int v = Integer.parseInt(body);
                start = v;
                end = v;
            }
            if (start < min || end > max || start > end) {
                throw new IllegalArgumentException("Cron field '" + field + "' out of range [" + min + "," + max + "]");
            }
            for (int i = start; i <= end; i += step) {
                b.set(i);
            }
        }
        return b;
    }

    private static String normaliseSunday(String dow) {
        // Cron historically accepts 7 for Sunday; normalise to 0.
        if ("7".equals(dow)) return "0";
        return dow.replace("7", "0");
    }

    private static boolean isStar(String field) {
        return "*".equals(field) || "*/1".equals(field);
    }

    @Override
    public String toString() {
        return expression;
    }
}
