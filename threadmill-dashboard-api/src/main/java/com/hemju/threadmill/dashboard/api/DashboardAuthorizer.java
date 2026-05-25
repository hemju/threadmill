package com.hemju.threadmill.dashboard.api;

import java.util.Set;

import org.springframework.security.core.Authentication;

/** Maps the host application's authenticated user to Threadmill dashboard permissions. */
public interface DashboardAuthorizer {

    Set<DashboardPermission> permissions(Authentication authentication);

    default String displayName(Authentication authentication) {
        return authentication == null ? "anonymous" : authentication.getName();
    }
}
