CREATE TABLE IF NOT EXISTS threadmill_cron_task_ownership (
    namespace TEXT NOT NULL,
    task_name TEXT NOT NULL REFERENCES threadmill_cron_tasks(name) ON DELETE CASCADE,
    PRIMARY KEY (namespace, task_name)
);

CREATE INDEX IF NOT EXISTS threadmill_cron_task_ownership_task_idx
    ON threadmill_cron_task_ownership (task_name);
