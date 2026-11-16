USE [Grid01]
GO
/****** Object:  Table [pool_01].[GridJobTasks]    Script Date: 11/13/2026 2:12:37 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [pool_01].[GridJobTasks](
	[jobId] [bigint] NOT NULL,
	[index] [int] NOT NULL,
	[status] [varchar](50) NOT NULL,
	[cmd] [varchar](max) NOT NULL,
	[tag] [varchar](250) NULL,
	[stdin] [varchar](max) NULL,
	[envJSON] [varchar](max) NULL,
	[createdDate] [datetime] NOT NULL,
	[nodeId] [varchar](50) NULL,
	[nodeName] [varchar](250) NULL,
	[nodeSessionId] [varchar](50) NULL,
	[startTime] [datetime] NULL,
	[finishTime] [datetime] NULL,
	[finished]  AS (CONVERT([bit],case when [finishTime] IS NOT NULL then (1) else (0) end)),
	[durationSeconds]  AS (datediff_big(second,[startTime],[finishTime])),
	[success]  AS (CONVERT([bit],case when [retCode] IS NULL then NULL else case when [retCode]=(0) then (1) else (0) end end)),
	[pid] [int] NULL,
	[retCode] [bigint] NULL,
	[stdout] [varchar](max) NULL,
	[stderr] [varchar](max) NULL,
 CONSTRAINT [PK_GridJobTasks] PRIMARY KEY CLUSTERED 
(
	[jobId] DESC,
	[index] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 90, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [pool_01].[GridJobs]    Script Date: 11/13/2026 2:12:37 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [pool_01].[GridJobs](
	[jobId] [bigint] IDENTITY(1,1) NOT NULL,
	[description] [varchar](250) NULL,
	[tag] [varchar](250) NULL,
	[userId] [varchar](100) NOT NULL,
	[userName] [varchar](200) NULL,
	[submitTime] [datetime] NOT NULL,
	[aborted] [bit] NOT NULL,
	[abortedById] [varchar](100) NULL,
	[abortedBy] [varchar](200) NULL,
	[abortTime] [datetime] NULL,
 CONSTRAINT [PK_GridJobs] PRIMARY KEY CLUSTERED 
(
	[jobId] DESC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 90, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  View [pool_01].[vPoolJobsView]    Script Date: 11/13/2026 2:12:37 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [pool_01].[vPoolJobsView]
AS
	WITH stat AS
	(
		SELECT
		[jobId]
		,COUNT(*) AS [numTasks]
		,MIN([startTime]) AS [startTime] -- could be NULL
		,MAX([finishTime]) AS [maxFinishTime] -- could be NULL
		,CONVERT(INT, SUM(IIF([finished]=1, 1, 0))) AS [numTasksFinished] -- not NULL 
		,CONVERT(INT, SUM(IIF([success] IS NULL, 0, [success]))) AS [numTasksSuccess] -- not NULL 
		FROM [pool_01].[GridJobTasks] (NOLOCK)
		GROUP BY [jobId]
	)
	,stat2 as
	(
		SELECT
		stat.*
		,CONVERT(BIT, IIF([numTasksFinished]=[numTasks], 1, 0)) AS [finished] -- NOT NULL
		,[completePct]=CONVERT(FLOAT, [numTasksFinished])/CONVERT(FLOAT, [numTasks]) * 100.0 -- NOT NULL
		FROM stat
	)
	SELECT
	j.[jobId]
	,j.[description]
	,j.[tag]
	,j.[userId]
	,j.[userName]
	,j.[submitTime]
	,IIF(j.[aborted]=1,'ABORTED',IIF(s.[finished]=1,'FINISHED',IIF(s.[startTime] IS NOT NULL,'STARTED','SUBMITTED'))) As [status]
	,ISNULL(s.[numTasks], 0) AS [numTasks]
	,ISNULL(s.[numTasksFinished], 0) AS [numTasksFinished]
	,s.[startTime]
	--,s.[maxFinishTime]
	,IIF(s.[finished]=1, [maxFinishTime], NULL) AS [finishTime]
	,IIF(s.[finished]=1, DATEDIFF_BIG(SECOND, s.[startTime], s.[maxFinishTime]), NULL) AS [durationSeconds]
	,CONVERT(BIT, IIF(j.[aborted]=1, 0, IIF(s.[finished]=1, IIF(s.[numTasksSuccess]=s.[numTasks], 1, 0), NULL))) AS [success]
	,ISNULL(s.[numTasksSuccess], 0) AS [numTasksSuccess]
	,s.[finished]
	,s.[completePct]
	,j.[abortedById]
	,j.[abortedBy]
	,j.[abortTime]
	FROM [pool_01].[GridJobs] j (NOLOCK)
	LEFT JOIN stat2 s
	ON j.[jobId]=s.[jobId]
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [IX_GridJobs_description]    Script Date: 11/13/2026 2:12:37 PM ******/
CREATE NONCLUSTERED INDEX [IX_GridJobs_description] ON [pool_01].[GridJobs]
(
	[description] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [IX_GridJobs_tag]    Script Date: 11/13/2026 2:12:37 PM ******/
CREATE NONCLUSTERED INDEX [IX_GridJobs_tag] ON [pool_01].[GridJobs]
(
	[tag] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [IX_GridJobTasks_jobId_status]    Script Date: 11/13/2026 2:12:37 PM ******/
CREATE NONCLUSTERED INDEX [IX_GridJobTasks_jobId_status] ON [pool_01].[GridJobTasks]
(
	[jobId] ASC,
	[status] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [IX_GridJobTasks_jobId_tag]    Script Date: 11/13/2026 2:12:37 PM ******/
CREATE NONCLUSTERED INDEX [IX_GridJobTasks_jobId_tag] ON [pool_01].[GridJobTasks]
(
	[jobId] ASC,
	[tag] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
ALTER TABLE [pool_01].[GridJobs] ADD  CONSTRAINT [DF_GridJobs_submitTime]  DEFAULT (getdate()) FOR [submitTime]
GO
ALTER TABLE [pool_01].[GridJobs] ADD  CONSTRAINT [DF_GridJobs_aborted]  DEFAULT ((0)) FOR [aborted]
GO
ALTER TABLE [pool_01].[GridJobTasks] ADD  CONSTRAINT [DF_GridJobTasks_status]  DEFAULT ('IDLE') FOR [status]
GO
ALTER TABLE [pool_01].[GridJobTasks] ADD  CONSTRAINT [DF_GridJobTasks_createdDate]  DEFAULT (getdate()) FOR [createdDate]
GO
/****** Object:  StoredProcedure [pool_01].[stp_job_abort]    Script Date: 11/13/2026 2:12:37 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [pool_01].[stp_job_abort]
	@userId VARCHAR(100)
	,@userName VARCHAR(200)
	,@jobId BIGINT
AS
BEGIN
	SET NOCOUNT ON;
	UPDATE [pool_01].[GridJobs]
	SET
	[aborted]=1
	,[abortedById]=@userId
	,[abortedBy]=@userName
	,[abortTime]=GETDATE()
	WHERE [jobId]=@jobId

	SELECT * FROM [pool_01].[vPoolJobsView] (NOLOCK) WHERE [jobId]=@jobId
END
GO
/****** Object:  StoredProcedure [pool_01].[stp_job_append_tasks_page]    Script Date: 11/13/2026 2:12:37 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [pool_01].[stp_job_append_tasks_page]
	@jobId BIGINT
	,@tasks VARCHAR(MAX)
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @numTasks INT
	SELECT @numTasks=[numTasks] FROM [pool_01].[vPoolJobsView] (NOLOCK) WHERE [jobId]=@jobId
	IF @numTasks IS NOT NULL
	BEGIN
		;WITH base AS
		(
			SELECT
			CONVERT(INT, [key]) AS [idx]
			,JSON_VALUE([value], '$.cmd') AS [cmd]
			,JSON_VALUE([value], '$.tag') AS [tag]
			,JSON_VALUE([value], '$.stdin') AS [stdin]
			,JSON_VALUE([value], '$.envJSON') AS [envJSON]
			FROM OPENJSON(@tasks)
		)
		INSERT INTO [pool_01].[GridJobTasks]
		(
			[jobId]
			,[index]
			,[cmd]
			,[tag]
			,[stdin]
			,[envJSON]
		)
		SELECT
		@jobId AS [jobId]
		,@numTasks + [idx] AS [index]
		,[cmd]
		,[tag]
		,[stdin]
		,[envJSON]
		FROM base
		ORDER BY [idx] ASC
	END

	SELECT * FROM [pool_01].[vPoolJobsView] (NOLOCK) WHERE [jobId]=@jobId
END
GO
/****** Object:  StoredProcedure [pool_01].[stp_job_create_new]    Script Date: 11/13/2026 2:12:37 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [pool_01].[stp_job_create_new]
	@userId VARCHAR(100)
	,@userName VARCHAR(200)
	,@description VARCHAR(250)
	,@tag VARCHAR(250)
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @jobId BIGINT
    INSERT INTO [pool_01].[GridJobs] ([userId],[userName],[description],[tag]) VALUES (@userId,@userName,@description,@tag)
	SELECT @jobId=SCOPE_IDENTITY()
	SELECT * FROM [pool_01].[vPoolJobsView] (NOLOCK) WHERE [jobId]=@jobId
END
GO
/****** Object:  StoredProcedure [pool_01].[stp_job_delete]    Script Date: 11/13/2026 2:12:37 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [pool_01].[stp_job_delete]
	@jobId BIGINT
AS
BEGIN
	SET NOCOUNT ON;
	SELECT * FROM [pool_01].[vPoolJobsView] (NOLOCK) WHERE [jobId]=@jobId
	DELETE FROM [pool_01].[GridJobs] WHERE [jobId]=@jobId
	DELETE FROM [pool_01].[GridJobTasks] WHERE [jobId]=@jobId
END
GO
/****** Object:  StoredProcedure [pool_01].[stp_job_get]    Script Date: 11/13/2026 2:12:37 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [pool_01].[stp_job_get]
	@jobId BIGINT
AS
BEGIN
	SET NOCOUNT ON;
	SELECT * FROM [pool_01].[vPoolJobsView] (NOLOCK) WHERE [jobId]=@jobId
END
GO
/****** Object:  StoredProcedure [pool_01].[stp_job_task_exec_ack]    Script Date: 11/13/2026 2:12:37 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [pool_01].[stp_job_task_exec_ack]
	@j BIGINT
	,@t INT
	,@nodeId VARCHAR(50)
	,@hostname VARCHAR(250)
	,@nodeSessionId VARCHAR(50)
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @jobAborted BIT = 0
	SELECT @jobAborted=[aborted] FROM [pool_01].[GridJobs] (NOLOCK) WHERE [jobId]=@j

	IF @jobAborted = 0
	BEGIN
		-- change the job task status from IDLE to STARTED
		UPDATE [pool_01].[GridJobTasks]
		SET
		[nodeId]=@nodeId
		,[nodeName]=@hostname
		,[nodeSessionId]=@nodeSessionId
		,[status]='STARTED'
		,[startTime]=GETDATE()
		WHERE
		[jobId]=@j
		AND [index]=@t
		AND [status]='IDLE'
	END

	SELECT
	@jobAborted AS [jobAborted]
	,[cmd]
	,[envJSON]
	,[stdin]
	FROM [pool_01].[GridJobTasks] (NOLOCK)
	WHERE
	[jobId]=@j
	AND [index]=@t

END
GO
/****** Object:  StoredProcedure [pool_01].[stp_job_task_exec_finish]    Script Date: 11/13/2026 2:12:37 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [pool_01].[stp_job_task_exec_finish]
	@j BIGINT
	,@t INT
	,@pid INT
	,@code BIGINT
	,@stdout VARCHAR(MAX)
	,@stderr VARCHAR(MAX)
AS
BEGIN
	SET NOCOUNT ON;

	-- change the job task status from STARTED to FINISHED
	UPDATE [pool_01].[GridJobTasks]
	SET
	[status]='FINISHED'
	,[finishTime]=GETDATE()
	,[pid]=@pid
	,[retCode]=@code
	,[stdout]=IIF(@stdout IS NULL, NULL, IIF(LEN(@stdout) > 0, @stdout, NULL))
	,[stderr]=IIF(@stderr IS NULL, NULL, IIF(LEN(@stderr) > 0, @stderr, NULL))
	WHERE
	[jobId]=@j
	AND [index]=@t
	AND [status]='STARTED'

    -- return code logged
	SELECT
	[retCode] AS [code]
	FROM [pool_01].[GridJobTasks] (NOLOCK)
	WHERE
	[jobId]=@j
	AND [index]=@t
END
GO
/****** Object:  StoredProcedure [pool_01].[stp_job_replicate]    Script Date: 11/13/2026 4:57:28 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [pool_01].[stp_job_replicate]
	@userId VARCHAR(100)
	,@userName VARCHAR(200)
	,@jobId BIGINT
	,@failedTasksOnly BIT
AS
BEGIN
	SET NOCOUNT ON;

	IF EXISTS (SELECT [jobId] FROM [pool_01].[GridJobs] (NOLOCK) WHERE [jobId]=@jobId)
	BEGIN
		DECLARE @newJobId BIGINT
		INSERT INTO [pool_01].[GridJobs]
		([userId],[userName],[description],[tag])
		SELECT
		@userId AS [userId]
		,@userName AS [userName]
		,[description]
		,[tag]
		FROM [pool_01].[GridJobs] (NOLOCK)
		WHERE [jobId]=@jobId
		SELECT @newJobId=SCOPE_IDENTITY()

		DECLARE @tmp TABLE
		(
			[index] INT IDENTITY(0,1)
			,[cmd] VARCHAR(MAX)
			,[tag] VARCHAR(250)
			,[stdin] VARCHAR(MAX)
			,[envJSON] VARCHAR(MAX)
		)
		INSERT INTO @tmp
		(
			[cmd]
			,[tag]
			,[stdin]
			,[envJSON]
		)
		SELECT
		[cmd]
		,[tag]
		,[stdin]
		,[envJSON]
		FROM [pool_01].[GridJobTasks] (NOLOCK)
		WHERE
		[jobId]=@jobId
		AND IIF(@failedTasksOnly=1, IIF([finished]=0 OR [success]=0, 1, 0), 1)=1

		INSERT INTO [pool_01].[GridJobTasks]
		(
			[jobId]
			,[index]
			,[cmd]
			,[tag]
			,[stdin]
			,[envJSON]
		)
		SELECT
		@newJobId AS [jobId]
		,[index]
		,[cmd]
		,[tag]
		,[stdin]
		,[envJSON]
		FROM @tmp
		ORDER BY [index] ASC

		SELECT * FROM [pool_01].[vPoolJobsView] (NOLOCK) WHERE [jobId]=@newJobId
	END
	ELSE
	BEGIN
		SELECT * FROM [pool_01].[vPoolJobsView] (NOLOCK) WHERE 1=0
	END

END
GO