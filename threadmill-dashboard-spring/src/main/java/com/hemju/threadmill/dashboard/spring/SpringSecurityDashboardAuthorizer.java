package com.hemju.threadmill.dashboard.spring;

import java.util.EnumSet;
import java.util.Locale;
import java.util.Set;

import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;

import com.hemju.threadmill.dashboard.api.DashboardPermission;

/** Default authorizer mapping Spring Security authorities to dashboard permissions. */
public final class SpringSecurityDashboardAuthorizer implements DashboardAuthorizer {

    @Override
    public Set<DashboardPermission> permissions(Authentication authentication) {
        if (authentication == null || !authentication.isAuthenticated()) return Set.of();
        var out = EnumSet.noneOf(DashboardPermission.class);
        for (GrantedAuthority authority : authentication.getAuthorities()) {
            String value = authority.getAuthority().toUpperCase(Locale.ROOT);
            if (value.equals("ROLE_THREADMILL_ADMIN") || value.equals("THREADMILL_ADMIN")) {
                return EnumSet.allOf(DashboardPermission.class);
            }
            if (value.startsWith("ROLE_THREADMILL_")) value = value.substring("ROLE_".length());
            if (value.startsWith("THREADMILL_")) {
                addPermission(out, value.substring("THREADMILL_".length()));
            }
        }
        if (!out.isEmpty()) out.add(DashboardPermission.READ);
        return out;
    }

    private static void addPermission(EnumSet<DashboardPermission> out, String value) {
        try {
            out.add(DashboardPermission.valueOf(value));
        } catch (IllegalArgumentException ignored) {
            // Ignore unrelated host authorities.
        }
    }
}
