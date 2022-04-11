def emrr():
        sql = ('''
WITH emr as (
    SELECT 
    fc.*
    FROM [DW_Operations].[dbo].[FactChart]              fc
    LEFT JOIN [DW_Operations].[dbo].[DimRetrievalMethod]    rm
        ON fc.RetrievalMethodId = rm.RetrievalMethodId
    LEFT JOIN [DW_Operations].[dbo].[DimProjectStatus]      ps
        ON fc.ProjectStatusId = ps.ProjectStatusId
    WHERE   rm.RetrievalGroup = 'EMR Remote'
    AND     ps.ProjectStatusId IN (1, 3)
)
, msi AS (
    SELECT 
         msi.Id      [MasterSiteId]
        ,msi.Name   [MasterSiteName]
        ,msic.Phone
        ,msic.Fax
    FROM [MasterSiteId].[dbo].[MSI]                         msi
    LEFT JOIN [MasterSiteId].[dbo].[MSIContact]               msic 
        ON msic.MSId = msi.Id
    WHERE msic.[Primary] = 1 
)
,RetrievalMethod as (
    SELECT 
        OutreachId
        ,STRING_AGG(RetrievalMethodDescription, ' | ') 'Sub RetrievalMethods'
    FROM (
        SELECT DISTINCT
             OutreachId
            ,rm.RetrievalMethodDescription
        FROM emr                                                fc
        LEFT JOIN [DW_Operations].[dbo].[DimRetrievalMethod]    rm
            ON fc.RetrievalMethodId = rm.RetrievalMethodId
        ) AS dist
    GROUP BY OutreachId
)
,charts as (
    SELECT
         OutreachId
        ,ChartStatusId
        ,count(ChartStatusId) AS chart_count

    FROM emr
    GROUP BY OutreachId, ChartStatusId
)
,chart_counts AS (
    SELECT
         OutreachId
        ,SUM(chart_count)                                                                   AS 'TotalCharts'
        ,SUM(CASE WHEN ChartStatusId IN (1, 2, 11, 22)      THEN chart_count ELSE 0 END)    AS 'ToGoCharts'
        ,SUM(CASE WHEN ChartStatusId IN (8, 9, 13, 29, 18)  THEN chart_count ELSE 0 END)    AS 'QA+Charts'
    FROM charts
    GROUP BY OutreachId
)
,final AS (
    SELECT DISTINCT
        fc.OutreachID              AS [OutreachID]
        ,adt.AuditTypeDescription  AS [Audit_Type]
        ,pt.ProjectTypeDescription AS [Project_Type]
        ,os.OutreachStatusDescription Outreach_Status
        ,dmo.MasterSiteID
        ,ISNULL(msi.Phone, dmo.Phone1) AS 'PhoneNumber'
        ,rm_agg.[Sub RetrievalMethods]
        ,rm.RetrievalGroup
        ,ec.[Remote Sites Name]
        ,dmo.Address1
        ,dmo.Address2
        ,dmo.City
        ,dmo.State
        ,dmo.ZIP
        ,cc.TotalCharts
        ,cc.ToGoCharts
        ,cc.[QA+Charts]
        ,ISNULL(dmo.CallCount, 0)       AS 'CallCount'
        ,dmo.ScheduledRetrievalDate     AS 'ScheduleDate'
        ,ISNULL(o.LastCallDateId, '')   AS 'Last Call'
        ,ISNULL(o.InsertDate, '')       AS 'InsertDate'
        ,CONVERT(DATE, DATEADD(WEEKDAY, 9, CONVERT(VARCHAR(10), GETDATE(), 23))) AS 'Recommended Schedule Date'
        ,ISNULL(dp.ProjectDueOnDateId,'')      AS 'DueDate'

    FROM [emr]                          					fc
    LEFT JOIN [DW_Operations].[dbo].[DimRetrievalMethod]    rm
        ON fc.RetrievalMethodId = rm.RetrievalMethodId
    LEFT JOIN [DW_Operations].[dbo].[DimOutreachStatus]     os
        ON fc.OutreachStatusId = os.OutreachStatusId
    LEFT JOIN [DW_Operations].[dbo].[DIM_Master_OrgdataEnhance] dmo
        ON fc.OutreachId = dmo.OutreachId
    LEFT JOIN [DW_Operations].[dbo].[DimOutreach]           o
        ON fc.OutreachID = o.OutreachId

    LEFT JOIN [DW_Operations].[dbo].[DimProject]            dp
        ON fc.ProjectId = dp.ProjectId
    LEFT JOIN [DW_Operations].[dbo].[DimProjectType]        pt
        ON dp.ProjectTypeId = pt.ProjectTypeId
    LEFT JOIN [DW_Operations].[dbo].[DimAuditType]          adt
        ON dp.AuditTypeId = adt.AuditTypeId

    LEFT JOIN [DWWorking].[Prod].[EMR_Credentials]          ec
                ON fc.OutreachId = ec.[Outreach ID]

    LEFT JOIN chart_counts                                  cc
        ON fc.OutreachId = cc.OutreachId
    LEFT JOIN RetrievalMethod                               rm_agg
        ON fc.OutreachId = rm_agg.OutreachId
    LEFT JOIN msi
        ON dmo.MasterSiteID = msi.MasterSiteId
    WHERE   os.OutreachStatusDescription IN ('Unscheduled', 'PNP Released','Escalated','Acct Mgmt Research ')
    AND     cc.ToGoCharts > 0
)
SELECT 
* 
FROM final

                        ''')
        return sql