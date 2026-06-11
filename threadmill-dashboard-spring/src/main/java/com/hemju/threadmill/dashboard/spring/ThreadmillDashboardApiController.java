package com.hemju.threadmill.dashboard.spring;

import java.time.Instant;
import java.util.List;
import java.util.Set;

import jakarta.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.security.web.csrf.CsrfToken;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import com.hemju.threadmill.core.JobId;
import com.hemju.threadmill.core.JobState;
import com.hemju.threadmill.core.OversizedJobException;
import com.hemju.threadmill.core.StaleJobException;
import com.hemju.threadmill.core.store.JobSearch;
import com.hemju.threadmill.core.store.NodeHeartbeat;
import com.hemju.threadmill.dashboard.api.DashboardApiException;
import com.hemju.threadmill.dashboard.api.DashboardApiService;
import com.hemju.threadmill.dashboard.api.DashboardAuditEvent;
import com.hemju.threadmill.dashboard.api.DashboardAuditSink;
import com.hemju.threadmill.dashboard.api.DashboardOptions;
import com.hemju.threadmill.dashboard.api.DashboardPayloads.ActionResponse;
import com.hemju.threadmill.dashboard.api.DashboardPayloads.JobDetail;
import com.hemju.threadmill.dashboard.api.DashboardPayloads.JobListResponse;
import com.hemju.threadmill.dashboard.api.DashboardPayloads.OverviewResponse;
import com.hemju.threadmill.dashboard.api.DashboardPayloads.PauseQueueRequest;
import com.hemju.threadmill.dashboard.api.DashboardPayloads.QueueView;
import com.hemju.threadmill.dashboard.api.DashboardPayloads.RecurringTaskView;
import com.hemju.threadmill.dashboard.api.DashboardPayloads.ReplaceJobRequest;
import com.hemju.threadmill.dashboard.api.DashboardPayloads.ScheduleRetryRequest;
import com.hemju.threadmill.dashboard.api.DashboardPayloads.SessionResponse;
import com.hemju.threadmill.dashboard.api.DashboardPayloads.UpdateRecurringRequest;
import com.hemju.threadmill.dashboard.api.DashboardPayloads.VersionedActionRequest;
import com.hemju.threadmill.dashboard.api.DashboardPermission;

/** Spring MVC endpoints for the Threadmill dashboard API. */
@RestController
@RequestMapping("${threadmill.dashboard.api.base-path:/threadmill/api}")
public final class ThreadmillDashboardApiController {

    private static final Logger LOG = LoggerFactory.getLogger(ThreadmillDashboardApiController.class);

    private final DashboardApiService service;
    private final DashboardAuthorizer authorizer;
    private final DashboardAuditSink auditSink;
    private final DashboardOptions options;

    public ThreadmillDashboardApiController(
            DashboardApiService service,
            DashboardAuthorizer authorizer,
            DashboardAuditSink auditSink,
            DashboardOptions options) {
        this.service = service;
        this.authorizer = authorizer;
        this.auditSink = auditSink;
        this.options = options;
    }

    @GetMapping("/session")
    public SessionResponse session(Authentication authentication, HttpServletRequest request) {
        var permissions = permissions(authentication);
        var csrf = (CsrfToken) request.getAttribute(CsrfToken.class.getName());
        return new SessionResponse(
                authorizer.displayName(authentication),
                permissions,
                csrf == null
                        ? null
                        : new SessionResponse.Csrf(csrf.getHeaderName(), csrf.getParameterName(), csrf.getToken()),
                sensitiveDetailsAllowed(permissions) ? "full" : "redacted");
    }

    @GetMapping("/overview")
    public OverviewResponse overview(Authentication authentication) {
        var permissions = requireRead(authentication, "overview");
        boolean includeSensitive = sensitiveDetailsAllowed(permissions);
        if (includeSensitive) {
            auditSensitiveView(authentication, "overview");
        }
        return service.overview(includeSensitive);
    }

    @GetMapping("/jobs")
    public JobListResponse jobs(
            Authentication authentication,
            @RequestParam(name = "state", required = false) JobState state,
            @RequestParam(name = "queue", required = false) String queue,
            @RequestParam(name = "handlerType", required = false) String handlerType,
            @RequestParam(name = "limit", defaultValue = "50") int limit,
            @RequestParam(name = "offset", defaultValue = "0") int offset) {
        requireRead(authentication, "jobs");
        return service.jobs(new JobSearch(state, queue, handlerType, limit, offset));
    }

    @GetMapping("/jobs/{id}")
    public JobDetail job(Authentication authentication, @PathVariable("id") String id) {
        var permissions = requireRead(authentication, "jobs/" + id);
        boolean includeSensitive = sensitiveDetailsAllowed(permissions);
        var detail = service.job(JobId.parse(id), includeSensitive);
        if (includeSensitive) {
            auditSensitiveView(authentication, id);
        }
        return detail;
    }

    private boolean sensitiveDetailsAllowed(Set<DashboardPermission> permissions) {
        return options.exposeSensitiveDetails() && has(permissions, DashboardPermission.VIEW_SENSITIVE_DETAILS);
    }

    /** Reading an unredacted payload is a security-relevant event. */
    private void auditSensitiveView(Authentication authentication, String target) {
        recordQuietly(new DashboardAuditEvent(
                Instant.now(),
                authorizer.displayName(authentication),
                DashboardPermission.VIEW_SENSITIVE_DETAILS,
                "view_sensitive_details",
                target,
                "viewed"));
    }

    /**
     * READ gate for GET endpoints that audits a denied attempt, mirroring the
     * denial audit on mutations. Mutations keep using {@link #require} via
     * {@link #action} (which audits their own denials), so there is no double
     * record.
     */
    private Set<DashboardPermission> requireRead(Authentication authentication, String target) {
        try {
            return require(authentication, DashboardPermission.READ);
        } catch (ResponseStatusException denied) {
            String actor = authentication == null ? "anonymous" : authorizer.displayName(authentication);
            recordQuietly(
                    new DashboardAuditEvent(Instant.now(), actor, DashboardPermission.READ, "read", target, "denied"));
            throw denied;
        }
    }

    /** ADMIN is a superset of every permission — including the redaction decision. */
    private static boolean has(Set<DashboardPermission> permissions, DashboardPermission permission) {
        return permissions.contains(permission) || permissions.contains(DashboardPermission.ADMIN);
    }

    @GetMapping("/queues")
    public List<QueueView> queues(Authentication authentication) {
        requireRead(authentication, "queues");
        return service.queues();
    }

    @GetMapping("/recurring")
    public List<RecurringTaskView> recurring(Authentication authentication) {
        var permissions = requireRead(authentication, "recurring");
        boolean includeSensitive = sensitiveDetailsAllowed(permissions);
        if (includeSensitive) {
            auditSensitiveView(authentication, "recurring");
        }
        return service.recurringTasks(includeSensitive);
    }

    @GetMapping("/nodes")
    public List<NodeHeartbeat> nodes(Authentication authentication) {
        requireRead(authentication, "nodes");
        return service.nodeHeartbeats();
    }

    @PostMapping("/queues/{queue}/pause")
    public ActionResponse pauseQueue(
            Authentication authentication,
            @PathVariable("queue") String queue,
            @RequestBody(required = false) PauseQueueRequest request) {
        return action(
                authentication,
                DashboardPermission.PAUSE_QUEUE,
                "pause_queue",
                queue,
                () -> service.pauseQueue(queue, request == null ? null : request.reason()));
    }

    @PostMapping("/queues/{queue}/resume")
    public ActionResponse resumeQueue(Authentication authentication, @PathVariable("queue") String queue) {
        return action(
                authentication,
                DashboardPermission.RESUME_QUEUE,
                "resume_queue",
                queue,
                () -> service.resumeQueue(queue));
    }

    @PostMapping("/jobs/{id}/requeue")
    public ActionResponse requeue(
            Authentication authentication, @PathVariable("id") String id, @RequestBody VersionedActionRequest request) {
        return action(
                authentication,
                DashboardPermission.REQUEUE_JOB,
                "requeue_job",
                id,
                () -> service.requeue(JobId.parse(id), request.expectedVersion()));
    }

    @PostMapping("/jobs/{id}/schedule-retry")
    public ActionResponse scheduleRetry(
            Authentication authentication, @PathVariable("id") String id, @RequestBody ScheduleRetryRequest request) {
        return action(
                authentication,
                DashboardPermission.REQUEUE_JOB,
                "schedule_retry",
                id,
                () -> service.scheduleRetry(JobId.parse(id), request.expectedVersion(), request.delay()));
    }

    @DeleteMapping("/jobs/{id}")
    public ActionResponse deleteJob(
            Authentication authentication,
            @PathVariable("id") String id,
            @RequestParam(name = "expectedVersion") long expectedVersion) {
        return action(
                authentication,
                DashboardPermission.DELETE_JOB,
                "delete_job",
                id,
                () -> service.deleteJob(JobId.parse(id), expectedVersion));
    }

    @PatchMapping("/jobs/{id}")
    public ActionResponse replaceJob(
            Authentication authentication, @PathVariable("id") String id, @RequestBody ReplaceJobRequest request) {
        return action(
                authentication,
                DashboardPermission.REPLACE_JOB,
                "replace_job",
                id,
                () -> service.replaceJob(JobId.parse(id), request));
    }

    @PostMapping("/recurring/{name}/trigger")
    public ActionResponse triggerRecurring(Authentication authentication, @PathVariable("name") String name) {
        return action(
                authentication,
                DashboardPermission.TRIGGER_RECURRING,
                "trigger_recurring",
                name,
                () -> service.triggerRecurring(name));
    }

    @PutMapping("/recurring/{name}")
    public ActionResponse updateRecurring(
            Authentication authentication,
            @PathVariable("name") String name,
            @RequestBody UpdateRecurringRequest request) {
        return action(
                authentication,
                DashboardPermission.UPDATE_RECURRING,
                "update_recurring",
                name,
                () -> service.updateRecurring(name, request));
    }

    @DeleteMapping("/recurring/{name}")
    public ActionResponse deleteRecurring(Authentication authentication, @PathVariable("name") String name) {
        return action(
                authentication,
                DashboardPermission.DELETE_RECURRING,
                "delete_recurring",
                name,
                () -> service.deleteRecurring(name));
    }

    private Set<DashboardPermission> require(Authentication authentication, DashboardPermission permission) {
        var permissions = permissions(authentication);
        if (!has(permissions, permission)) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "missing " + permission);
        }
        return permissions;
    }

    private Set<DashboardPermission> permissions(Authentication authentication) {
        if (authentication == null && options.allowUnsafeReadOnlyWithoutAuthentication()) {
            return Set.of(DashboardPermission.READ);
        }
        if (authentication == null)
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "authentication required");
        return authorizer.permissions(authentication);
    }

    private ActionResponse action(
            Authentication authentication,
            DashboardPermission permission,
            String action,
            String target,
            DashboardAction work) {
        String actor = authorizer.displayName(authentication);
        try {
            require(authentication, permission);
        } catch (ResponseStatusException e) {
            // Denied mutation attempts are security-relevant events.
            recordQuietly(new DashboardAuditEvent(Instant.now(), actor, permission, action, target, "denied"));
            throw e;
        }
        ActionResponse response;
        try {
            response = work.run();
        } catch (RuntimeException e) {
            recordQuietly(new DashboardAuditEvent(Instant.now(), actor, permission, action, target, "failed"));
            throw e;
        }
        // The mutation is durably applied at this point: a throwing audit
        // sink must not convert it into a false "failed" + 500.
        recordQuietly(new DashboardAuditEvent(Instant.now(), actor, permission, action, target, response.status()));
        return response;
    }

    private void recordQuietly(DashboardAuditEvent event) {
        try {
            auditSink.record(event);
        } catch (RuntimeException e) {
            LOG.warn("Dashboard audit sink failed for {} {} ({})", event.action(), event.target(), event.outcome(), e);
        }
    }

    @FunctionalInterface
    private interface DashboardAction {
        ActionResponse run();
    }

    @ExceptionHandler(StaleJobException.class)
    @ResponseStatus(HttpStatus.CONFLICT)
    ProblemDetail staleJob(StaleJobException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, "stale job version");
    }

    @ExceptionHandler(DashboardApiException.class)
    ResponseEntity<ProblemDetail> dashboardApiFailure(DashboardApiException e) {
        var status =
                switch (e.code()) {
                    case BAD_REQUEST -> HttpStatus.BAD_REQUEST;
                    case NOT_FOUND -> HttpStatus.NOT_FOUND;
                    case CONFLICT -> HttpStatus.CONFLICT;
                };
        return ResponseEntity.status(status).body(ProblemDetail.forStatusAndDetail(status, e.getMessage()));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    ProblemDetail invalidRequest(IllegalArgumentException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
    }

    @ExceptionHandler(OversizedJobException.class)
    @ResponseStatus(HttpStatus.CONTENT_TOO_LARGE)
    ProblemDetail oversizedJob(OversizedJobException e) {
        return ProblemDetail.forStatusAndDetail(
                HttpStatus.CONTENT_TOO_LARGE,
                "job serialized form is " + e.actualBytes() + " bytes, exceeds limit of " + e.limitBytes());
    }
}
