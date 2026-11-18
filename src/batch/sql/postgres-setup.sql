-- =============================================================================
-- PostgreSQL translation of mssql-setup.sql  (requires PostgreSQL 14+)
--
-- Notes on the translation:
--   * Identifiers keep their camelCase names via double quotes so the column
--     names returned to the application match the SQL Server version.
--   * [datetime] -> timestamp(3), [varchar](max) -> text, [bit] -> boolean,
--     IDENTITY -> GENERATED ALWAYS AS IDENTITY, GETDATE() -> LOCALTIMESTAMP.
--   * Computed columns -> STORED generated columns.
--   * (NOLOCK) hints are dropped (readers never block writers under MVCC).
--   * Stored procedures cannot return result sets in PostgreSQL, so every
--     procedure exposes its result as INOUT parameters with DEFAULT NULL.
--     "CALL pool_01.stp_xxx(<inputs>)" therefore returns exactly one row whose
--     columns are the same as the result set of the T-SQL procedure.
--     Where the T-SQL procedure would return zero rows (unknown job / task),
--     the PostgreSQL procedure raises a NO_DATA_FOUND exception instead.
--
-- USE [Grid01]  -> connect to the target database first, e.g.  \c grid01
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS pool_01;

-- -----------------------------------------------------------------------------
-- Table pool_01."GridJobTasks"
-- -----------------------------------------------------------------------------
CREATE TABLE pool_01."GridJobTasks"
(
    "jobId"           bigint         NOT NULL,
    "index"           integer        NOT NULL,
    "status"          varchar(50)    NOT NULL CONSTRAINT "DF_GridJobTasks_status" DEFAULT ('IDLE'),
    "cmd"             text           NOT NULL,
    "tag"             varchar(250)   NULL,
    "stdin"           text           NULL,
    "envJSON"         text           NULL,
    "createdDate"     timestamp(3)   NOT NULL DEFAULT (LOCALTIMESTAMP),
    "nodeId"          varchar(50)    NULL,
    "nodeName"        varchar(250)   NULL,
    "nodeSessionId"   varchar(50)    NULL,
    "startTime"       timestamp(3)   NULL,
    "finishTime"      timestamp(3)   NULL,
    "finished"        boolean        GENERATED ALWAYS AS ("finishTime" IS NOT NULL) STORED,
    -- DATEDIFF_BIG(SECOND, ...) counts second boundaries crossed, hence the date_trunc
    "durationSeconds" bigint         GENERATED ALWAYS AS (
                          (EXTRACT(EPOCH FROM (date_trunc('second', "finishTime") - date_trunc('second', "startTime"))))::bigint
                      ) STORED,
    -- NULL when retCode is NULL, otherwise retCode = 0
    "success"         boolean        GENERATED ALWAYS AS ("retCode" = 0) STORED,
    "pid"             integer        NULL,
    "retCode"         bigint         NULL,
    "stdout"          text           NULL,
    "stderr"          text           NULL,
    CONSTRAINT "PK_GridJobTasks" PRIMARY KEY ("jobId", "index") WITH (fillfactor = 90)
);

-- -----------------------------------------------------------------------------
-- Table pool_01."GridJobs"
-- -----------------------------------------------------------------------------
CREATE TABLE pool_01."GridJobs"
(
    "jobId"       bigint         GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) NOT NULL,
    "description" varchar(250)   NULL,
    "tag"         varchar(250)   NULL,
    "userId"      varchar(100)   NOT NULL,
    "userName"    varchar(200)   NULL,
    "submitTime"  timestamp(3)   NOT NULL DEFAULT (LOCALTIMESTAMP),
    "aborted"     boolean        NOT NULL DEFAULT (false),
    "abortedById" varchar(100)   NULL,
    "abortedBy"   varchar(200)   NULL,
    "abortTime"   timestamp(3)   NULL,
    CONSTRAINT "PK_GridJobs" PRIMARY KEY ("jobId") WITH (fillfactor = 90)
);

-- -----------------------------------------------------------------------------
-- Indexes
-- -----------------------------------------------------------------------------
CREATE INDEX "IX_GridJobs_description"      ON pool_01."GridJobs"     ("description" ASC);
CREATE INDEX "IX_GridJobs_tag"              ON pool_01."GridJobs"     ("tag" ASC);
CREATE INDEX "IX_GridJobTasks_jobId_status" ON pool_01."GridJobTasks" ("jobId" ASC, "status" ASC);
CREATE INDEX "IX_GridJobTasks_jobId_tag"    ON pool_01."GridJobTasks" ("jobId" ASC, "tag" ASC);

-- -----------------------------------------------------------------------------
-- View pool_01.v_pool_jobs_view
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW pool_01.v_pool_jobs_view
AS
    WITH stat AS
    (
        SELECT
         "jobId"
        ,COUNT(*)::integer                                 AS "numTasks"
        ,MIN("startTime")                                  AS "startTime"        -- could be NULL
        ,MAX("finishTime")                                 AS "maxFinishTime"    -- could be NULL
        ,(COUNT(*) FILTER (WHERE "finished"))::integer     AS "numTasksFinished" -- not NULL
        ,(COUNT(*) FILTER (WHERE "success"))::integer      AS "numTasksSuccess"  -- not NULL
        FROM pool_01."GridJobTasks"
        GROUP BY "jobId"
    )
    ,stat2 AS
    (
        SELECT
         stat.*
        ,("numTasksFinished" = "numTasks")                                         AS "finished"    -- NOT NULL
        ,"numTasksFinished"::double precision / "numTasks"::double precision * 100.0 AS "completePct" -- NOT NULL
        FROM stat
    )
    SELECT
     j."jobId"
    ,j."description"
    ,j."tag"
    ,j."userId"
    ,j."userName"
    ,j."submitTime"
    ,(CASE
        WHEN j."aborted"               THEN 'ABORTED'
        WHEN s."finished"              THEN 'FINISHED'
        WHEN s."startTime" IS NOT NULL THEN 'STARTED'
        ELSE 'SUBMITTED'
      END)::varchar(9)                                         AS "status"
    ,COALESCE(s."numTasks", 0)                                 AS "numTasks"
    ,COALESCE(s."numTasksFinished", 0)                         AS "numTasksFinished"
    ,s."startTime"
    --,s."maxFinishTime"
    ,CASE WHEN s."finished" THEN s."maxFinishTime" END         AS "finishTime"
    ,CASE WHEN s."finished" THEN
        (EXTRACT(EPOCH FROM (date_trunc('second', s."maxFinishTime") - date_trunc('second', s."startTime"))))::bigint
     END                                                       AS "durationSeconds"
    ,CASE
        WHEN j."aborted"  THEN false
        WHEN s."finished" THEN (s."numTasksSuccess" = s."numTasks")
        ELSE NULL
     END                                                       AS "success"
    ,COALESCE(s."numTasksSuccess", 0)                          AS "numTasksSuccess"
    ,s."finished"
    ,s."completePct"
    ,j."abortedById"
    ,j."abortedBy"
    ,j."abortTime"
    FROM pool_01."GridJobs" j
    LEFT JOIN stat2 s
    ON j."jobId" = s."jobId";

-- =============================================================================
-- Stored procedures
--
-- The job procedures all return one row shaped like pool_01.v_pool_jobs_view.
-- That is done through this common block of trailing INOUT parameters:
--
--   INOUT "jobId" bigint DEFAULT NULL, INOUT "description" varchar DEFAULT NULL,
--   ... (one per view column, in view column order)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- pool_01.stp_job_abort
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE pool_01.stp_job_abort
(
     p_user_id   varchar(100)
    ,p_user_name varchar(200)
    ,p_job_id    bigint
    -- result row (vPoolJobsView)
    ,INOUT "jobId"            bigint           DEFAULT NULL
    ,INOUT "description"      varchar          DEFAULT NULL
    ,INOUT "tag"              varchar          DEFAULT NULL
    ,INOUT "userId"           varchar          DEFAULT NULL
    ,INOUT "userName"         varchar          DEFAULT NULL
    ,INOUT "submitTime"       timestamp(3)     DEFAULT NULL
    ,INOUT "status"           varchar          DEFAULT NULL
    ,INOUT "numTasks"         integer          DEFAULT NULL
    ,INOUT "numTasksFinished" integer          DEFAULT NULL
    ,INOUT "startTime"        timestamp(3)     DEFAULT NULL
    ,INOUT "finishTime"       timestamp(3)     DEFAULT NULL
    ,INOUT "durationSeconds"  bigint           DEFAULT NULL
    ,INOUT "success"          boolean          DEFAULT NULL
    ,INOUT "numTasksSuccess"  integer          DEFAULT NULL
    ,INOUT "finished"         boolean          DEFAULT NULL
    ,INOUT "completePct"      double precision DEFAULT NULL
    ,INOUT "abortedById"      varchar          DEFAULT NULL
    ,INOUT "abortedBy"        varchar          DEFAULT NULL
    ,INOUT "abortTime"        timestamp(3)     DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
#variable_conflict use_column
BEGIN
    UPDATE pool_01."GridJobs"
    SET
     "aborted"     = true
    ,"abortedById" = p_user_id
    ,"abortedBy"   = p_user_name
    ,"abortTime"   = LOCALTIMESTAMP
    WHERE "jobId" = p_job_id;

    SELECT v.*
    INTO "jobId", "description", "tag", "userId", "userName", "submitTime", "status",
         "numTasks", "numTasksFinished", "startTime", "finishTime", "durationSeconds",
         "success", "numTasksSuccess", "finished", "completePct",
         "abortedById", "abortedBy", "abortTime"
    FROM pool_01.v_pool_jobs_view v
    WHERE v."jobId" = p_job_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Invalid job id (%)', p_job_id USING ERRCODE = 'no_data_found';
    END IF;
END;
$$;

-- -----------------------------------------------------------------------------
-- pool_01.stp_job_append_tasks_page
--   p_tasks: JSON array of {cmd, tag?, stdin?, envJSON?}
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE pool_01.stp_job_append_tasks_page
(
     p_job_id bigint
    ,p_tasks  text
    -- result row (vPoolJobsView)
    ,INOUT "jobId"            bigint           DEFAULT NULL
    ,INOUT "description"      varchar          DEFAULT NULL
    ,INOUT "tag"              varchar          DEFAULT NULL
    ,INOUT "userId"           varchar          DEFAULT NULL
    ,INOUT "userName"         varchar          DEFAULT NULL
    ,INOUT "submitTime"       timestamp(3)     DEFAULT NULL
    ,INOUT "status"           varchar          DEFAULT NULL
    ,INOUT "numTasks"         integer          DEFAULT NULL
    ,INOUT "numTasksFinished" integer          DEFAULT NULL
    ,INOUT "startTime"        timestamp(3)     DEFAULT NULL
    ,INOUT "finishTime"       timestamp(3)     DEFAULT NULL
    ,INOUT "durationSeconds"  bigint           DEFAULT NULL
    ,INOUT "success"          boolean          DEFAULT NULL
    ,INOUT "numTasksSuccess"  integer          DEFAULT NULL
    ,INOUT "finished"         boolean          DEFAULT NULL
    ,INOUT "completePct"      double precision DEFAULT NULL
    ,INOUT "abortedById"      varchar          DEFAULT NULL
    ,INOUT "abortedBy"        varchar          DEFAULT NULL
    ,INOUT "abortTime"        timestamp(3)     DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
#variable_conflict use_column
DECLARE
    v_num_tasks integer;
BEGIN
    SELECT v."numTasks" INTO v_num_tasks
    FROM pool_01.v_pool_jobs_view v
    WHERE v."jobId" = p_job_id;

    IF v_num_tasks IS NOT NULL THEN
        INSERT INTO pool_01."GridJobTasks"
        (
             "jobId"
            ,"index"
            ,"cmd"
            ,"tag"
            ,"stdin"
            ,"envJSON"
        )
        SELECT
         p_job_id                              AS "jobId"
        ,v_num_tasks + (t.ord - 1)::integer    AS "index"   -- OPENJSON [key] is 0-based
        ,t.elem ->> 'cmd'                      AS "cmd"
        ,t.elem ->> 'tag'                      AS "tag"
        ,t.elem ->> 'stdin'                    AS "stdin"
        ,t.elem ->> 'envJSON'                  AS "envJSON"
        FROM json_array_elements(p_tasks::json) WITH ORDINALITY AS t(elem, ord)
        ORDER BY t.ord ASC;
    END IF;

    SELECT v.*
    INTO "jobId", "description", "tag", "userId", "userName", "submitTime", "status",
         "numTasks", "numTasksFinished", "startTime", "finishTime", "durationSeconds",
         "success", "numTasksSuccess", "finished", "completePct",
         "abortedById", "abortedBy", "abortTime"
    FROM pool_01.v_pool_jobs_view v
    WHERE v."jobId" = p_job_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Invalid job id (%)', p_job_id USING ERRCODE = 'no_data_found';
    END IF;
END;
$$;

-- -----------------------------------------------------------------------------
-- pool_01.stp_job_create_new
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE pool_01.stp_job_create_new
(
     p_user_id     varchar(100)
    ,p_user_name   varchar(200)
    ,p_description varchar(250)
    ,p_tag         varchar(250)
    -- result row (vPoolJobsView)
    ,INOUT "jobId"            bigint           DEFAULT NULL
    ,INOUT "description"      varchar          DEFAULT NULL
    ,INOUT "tag"              varchar          DEFAULT NULL
    ,INOUT "userId"           varchar          DEFAULT NULL
    ,INOUT "userName"         varchar          DEFAULT NULL
    ,INOUT "submitTime"       timestamp(3)     DEFAULT NULL
    ,INOUT "status"           varchar          DEFAULT NULL
    ,INOUT "numTasks"         integer          DEFAULT NULL
    ,INOUT "numTasksFinished" integer          DEFAULT NULL
    ,INOUT "startTime"        timestamp(3)     DEFAULT NULL
    ,INOUT "finishTime"       timestamp(3)     DEFAULT NULL
    ,INOUT "durationSeconds"  bigint           DEFAULT NULL
    ,INOUT "success"          boolean          DEFAULT NULL
    ,INOUT "numTasksSuccess"  integer          DEFAULT NULL
    ,INOUT "finished"         boolean          DEFAULT NULL
    ,INOUT "completePct"      double precision DEFAULT NULL
    ,INOUT "abortedById"      varchar          DEFAULT NULL
    ,INOUT "abortedBy"        varchar          DEFAULT NULL
    ,INOUT "abortTime"        timestamp(3)     DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
#variable_conflict use_column
DECLARE
    v_job_id bigint;
BEGIN
    INSERT INTO pool_01."GridJobs" ("userId", "userName", "description", "tag")
    VALUES (p_user_id, p_user_name, p_description, p_tag)
    RETURNING "jobId" INTO v_job_id;

    SELECT v.*
    INTO "jobId", "description", "tag", "userId", "userName", "submitTime", "status",
         "numTasks", "numTasksFinished", "startTime", "finishTime", "durationSeconds",
         "success", "numTasksSuccess", "finished", "completePct",
         "abortedById", "abortedBy", "abortTime"
    FROM pool_01.v_pool_jobs_view v
    WHERE v."jobId" = v_job_id;
END;
$$;

-- -----------------------------------------------------------------------------
-- pool_01.stp_job_delete
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE pool_01.stp_job_delete
(
     p_job_id bigint
    -- result row (vPoolJobsView)
    ,INOUT "jobId"            bigint           DEFAULT NULL
    ,INOUT "description"      varchar          DEFAULT NULL
    ,INOUT "tag"              varchar          DEFAULT NULL
    ,INOUT "userId"           varchar          DEFAULT NULL
    ,INOUT "userName"         varchar          DEFAULT NULL
    ,INOUT "submitTime"       timestamp(3)     DEFAULT NULL
    ,INOUT "status"           varchar          DEFAULT NULL
    ,INOUT "numTasks"         integer          DEFAULT NULL
    ,INOUT "numTasksFinished" integer          DEFAULT NULL
    ,INOUT "startTime"        timestamp(3)     DEFAULT NULL
    ,INOUT "finishTime"       timestamp(3)     DEFAULT NULL
    ,INOUT "durationSeconds"  bigint           DEFAULT NULL
    ,INOUT "success"          boolean          DEFAULT NULL
    ,INOUT "numTasksSuccess"  integer          DEFAULT NULL
    ,INOUT "finished"         boolean          DEFAULT NULL
    ,INOUT "completePct"      double precision DEFAULT NULL
    ,INOUT "abortedById"      varchar          DEFAULT NULL
    ,INOUT "abortedBy"        varchar          DEFAULT NULL
    ,INOUT "abortTime"        timestamp(3)     DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
#variable_conflict use_column
BEGIN
    SELECT v.*
    INTO "jobId", "description", "tag", "userId", "userName", "submitTime", "status",
         "numTasks", "numTasksFinished", "startTime", "finishTime", "durationSeconds",
         "success", "numTasksSuccess", "finished", "completePct",
         "abortedById", "abortedBy", "abortTime"
    FROM pool_01.v_pool_jobs_view v
    WHERE v."jobId" = p_job_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Invalid job id (%)', p_job_id USING ERRCODE = 'no_data_found';
    END IF;

    DELETE FROM pool_01."GridJobs"     WHERE "jobId" = p_job_id;
    DELETE FROM pool_01."GridJobTasks" WHERE "jobId" = p_job_id;
END;
$$;

-- -----------------------------------------------------------------------------
-- pool_01.stp_job_get
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE pool_01.stp_job_get
(
     p_job_id bigint
    -- result row (vPoolJobsView)
    ,INOUT "jobId"            bigint           DEFAULT NULL
    ,INOUT "description"      varchar          DEFAULT NULL
    ,INOUT "tag"              varchar          DEFAULT NULL
    ,INOUT "userId"           varchar          DEFAULT NULL
    ,INOUT "userName"         varchar          DEFAULT NULL
    ,INOUT "submitTime"       timestamp(3)     DEFAULT NULL
    ,INOUT "status"           varchar          DEFAULT NULL
    ,INOUT "numTasks"         integer          DEFAULT NULL
    ,INOUT "numTasksFinished" integer          DEFAULT NULL
    ,INOUT "startTime"        timestamp(3)     DEFAULT NULL
    ,INOUT "finishTime"       timestamp(3)     DEFAULT NULL
    ,INOUT "durationSeconds"  bigint           DEFAULT NULL
    ,INOUT "success"          boolean          DEFAULT NULL
    ,INOUT "numTasksSuccess"  integer          DEFAULT NULL
    ,INOUT "finished"         boolean          DEFAULT NULL
    ,INOUT "completePct"      double precision DEFAULT NULL
    ,INOUT "abortedById"      varchar          DEFAULT NULL
    ,INOUT "abortedBy"        varchar          DEFAULT NULL
    ,INOUT "abortTime"        timestamp(3)     DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
#variable_conflict use_column
BEGIN
    SELECT v.*
    INTO "jobId", "description", "tag", "userId", "userName", "submitTime", "status",
         "numTasks", "numTasksFinished", "startTime", "finishTime", "durationSeconds",
         "success", "numTasksSuccess", "finished", "completePct",
         "abortedById", "abortedBy", "abortTime"
    FROM pool_01.v_pool_jobs_view v
    WHERE v."jobId" = p_job_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Invalid job id (%)', p_job_id USING ERRCODE = 'no_data_found';
    END IF;
END;
$$;

-- -----------------------------------------------------------------------------
-- pool_01.stp_job_task_exec_ack
--   returns: "jobAborted", "cmd", "envJSON", "stdin"
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE pool_01.stp_job_task_exec_ack
(
     p_j               bigint
    ,p_t               integer
    ,p_node_id         varchar(50)
    ,p_hostname        varchar(250)
    ,p_node_session_id varchar(50)
    -- result row
    ,INOUT "jobAborted" boolean DEFAULT NULL
    ,INOUT "cmd"        text    DEFAULT NULL
    ,INOUT "envJSON"    text    DEFAULT NULL
    ,INOUT "stdin"      text    DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
#variable_conflict use_column
DECLARE
    v_job_aborted boolean := false;
BEGIN
    SELECT j."aborted" INTO v_job_aborted
    FROM pool_01."GridJobs" j
    WHERE j."jobId" = p_j;

    v_job_aborted := COALESCE(v_job_aborted, false);

    IF NOT v_job_aborted THEN
        -- change the job task status from IDLE to STARTED
        UPDATE pool_01."GridJobTasks"
        SET
         "nodeId"        = p_node_id
        ,"nodeName"      = p_hostname
        ,"nodeSessionId" = p_node_session_id
        ,"status"        = 'STARTED'
        ,"startTime"     = LOCALTIMESTAMP
        WHERE
            "jobId"  = p_j
        AND "index"  = p_t
        AND "status" = 'IDLE';
    END IF;

    SELECT
     v_job_aborted
    ,t."cmd"
    ,t."envJSON"
    ,t."stdin"
    INTO "jobAborted", "cmd", "envJSON", "stdin"
    FROM pool_01."GridJobTasks" t
    WHERE
        t."jobId" = p_j
    AND t."index" = p_t;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Task not found (job %, index %)', p_j, p_t USING ERRCODE = 'no_data_found';
    END IF;
END;
$$;

-- -----------------------------------------------------------------------------
-- pool_01.stp_job_task_exec_finish
--   returns: "code"
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE pool_01.stp_job_task_exec_finish
(
     p_j      bigint
    ,p_t      integer
    ,p_pid    integer
    ,p_code   bigint
    ,p_stdout text
    ,p_stderr text
    -- result row
    ,INOUT "code" bigint DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
#variable_conflict use_column
BEGIN
    -- change the job task status from STARTED to FINISHED
    -- (LEN() in T-SQL ignores trailing spaces, hence the rtrim)
    UPDATE pool_01."GridJobTasks"
    SET
     "status"     = 'FINISHED'
    ,"finishTime" = LOCALTIMESTAMP
    ,"pid"        = p_pid
    ,"retCode"    = p_code
    ,"stdout"     = CASE WHEN length(rtrim(p_stdout, ' ')) > 0 THEN p_stdout END
    ,"stderr"     = CASE WHEN length(rtrim(p_stderr, ' ')) > 0 THEN p_stderr END
    WHERE
        "jobId"  = p_j
    AND "index"  = p_t
    AND "status" = 'STARTED';

    -- return code logged
    SELECT t."retCode"
    INTO "code"
    FROM pool_01."GridJobTasks" t
    WHERE
        t."jobId" = p_j
    AND t."index" = p_t;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Task not found (job %, index %)', p_j, p_t USING ERRCODE = 'no_data_found';
    END IF;
END;
$$;

-- -----------------------------------------------------------------------------
-- pool_01.stp_job_replicate
--   Creates a new job (owned by p_user_id) as a copy of p_job_id, copying either
--   all of its tasks or, when p_failed_tasks_only is true, only the tasks that
--   are unfinished or failed. Task indexes are renumbered from 0.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE pool_01.stp_job_replicate
(
     p_user_id           varchar(100)
    ,p_user_name         varchar(200)
    ,p_job_id            bigint
    ,p_failed_tasks_only boolean
    -- result row (vPoolJobsView)
    ,INOUT "jobId"            bigint           DEFAULT NULL
    ,INOUT "description"      varchar          DEFAULT NULL
    ,INOUT "tag"              varchar          DEFAULT NULL
    ,INOUT "userId"           varchar          DEFAULT NULL
    ,INOUT "userName"         varchar          DEFAULT NULL
    ,INOUT "submitTime"       timestamp(3)     DEFAULT NULL
    ,INOUT "status"           varchar          DEFAULT NULL
    ,INOUT "numTasks"         integer          DEFAULT NULL
    ,INOUT "numTasksFinished" integer          DEFAULT NULL
    ,INOUT "startTime"        timestamp(3)     DEFAULT NULL
    ,INOUT "finishTime"       timestamp(3)     DEFAULT NULL
    ,INOUT "durationSeconds"  bigint           DEFAULT NULL
    ,INOUT "success"          boolean          DEFAULT NULL
    ,INOUT "numTasksSuccess"  integer          DEFAULT NULL
    ,INOUT "finished"         boolean          DEFAULT NULL
    ,INOUT "completePct"      double precision DEFAULT NULL
    ,INOUT "abortedById"      varchar          DEFAULT NULL
    ,INOUT "abortedBy"        varchar          DEFAULT NULL
    ,INOUT "abortTime"        timestamp(3)     DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
#variable_conflict use_column
DECLARE
    v_new_job_id bigint;
BEGIN
    INSERT INTO pool_01."GridJobs" ("userId", "userName", "description", "tag")
    SELECT
     p_user_id   AS "userId"
    ,p_user_name AS "userName"
    ,j."description"
    ,j."tag"
    FROM pool_01."GridJobs" j
    WHERE j."jobId" = p_job_id
    RETURNING "jobId" INTO v_new_job_id;

    IF v_new_job_id IS NULL THEN
        -- source job does not exist (T-SQL version returns an empty result set)
        RAISE EXCEPTION 'Invalid job id (%)', p_job_id USING ERRCODE = 'no_data_found';
    END IF;

    INSERT INTO pool_01."GridJobTasks"
    (
         "jobId"
        ,"index"
        ,"cmd"
        ,"tag"
        ,"stdin"
        ,"envJSON"
    )
    SELECT
     v_new_job_id                                          AS "jobId"
    ,(ROW_NUMBER() OVER (ORDER BY t."index") - 1)::integer AS "index"   -- 0-based, like IDENTITY(0,1)
    ,t."cmd"
    ,t."tag"
    ,t."stdin"
    ,t."envJSON"
    FROM pool_01."GridJobTasks" t
    WHERE
        t."jobId" = p_job_id
    AND (NOT COALESCE(p_failed_tasks_only, false) OR NOT t."finished" OR t."success" = false);

    SELECT v.*
    INTO "jobId", "description", "tag", "userId", "userName", "submitTime", "status",
         "numTasks", "numTasksFinished", "startTime", "finishTime", "durationSeconds",
         "success", "numTasksSuccess", "finished", "completePct",
         "abortedById", "abortedBy", "abortTime"
    FROM pool_01.v_pool_jobs_view v
    WHERE v."jobId" = v_new_job_id;
END;
$$;
