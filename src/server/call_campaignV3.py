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
    AND     fc.OutreachId <> -1
)
,CleanSiteId AS (
    SELECT 
        OutreachId
        ,STRING_AGG(SiteCleanId, ' | ') 'SiteCleanId'
    FROM (
        SELECT DISTINCT
             fc.OutreachId
            ,dmo.SiteCleanId
        FROM emr fc
        LEFT JOIN [DW_Operations].[dbo].[DIM_Master_OrgdataEnhance] dmo
            ON fc.OutreachId = dmo.OutreachId
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
        fc.OutreachId
        -- ,adt.AuditTypeDescription  AS [Audit_Type]
        -- ,pt.ProjectTypeDescription AS [Project_Type]
        ,os.OutreachStatusDescription Outreach_Status
        ,dmo.MasterSiteID
        -- ,dmo.Phone1 'PhonNumber'
        ,rm.RetrievalMethodDescription
        ,rm.RetrievalGroup
        ,ec.[Remote Sites Name]
        ,dmo.Address1
        ,dmo.Address2
        ,dmo.City
        ,dmo.State
        ,dmo.ZIP
        ,csi.SiteCleanId
        ,cc.TotalCharts
        ,cc.ToGoCharts
        ,cc.[QA+Charts]
        ,ISNULL(dmo.CallCount, 0) AS 'CallCount'
        ,dmo.ScheduledRetrievalDate AS 'ScheduleDate'
        ,ISNULL(CONVERT(VARCHAR(10), o.LastCallDateId, 1), '') AS 'Last Call'
        ,ISNULL(CONVERT(VARCHAR(10), o.InsertDate, 1), '')  AS 'InserDate'
        ,CONVERT(VARCHAR(10), DATEADD(WEEKDAY, 9, CONVERT(VARCHAR(10), GETDATE(), 1)),1 ) AS 'Recommended Schedule Date'
--         ,CAST(p.Duedate AS DATE) AS 'DueDate'

    FROM [emr]                          					fc
    LEFT JOIN [DW_Operations].[dbo].[DimRetrievalMethod]    rm
        ON fc.RetrievalMethodId = rm.RetrievalMethodId
    LEFT JOIN [DW_Operations].[dbo].[DimOutreachStatus]     os
        ON fc.ChartStatusId = os.OutreachStatusId
    LEFT JOIN [DW_Operations].[dbo].[DIM_Master_OrgdataEnhance] dmo ----- site clean ID
        ON fc.OutreachId = dmo.OutreachId
    -- LEFT JOIN [DW_Operations].[dbo].[DimRegion]             dr
    --     ON fc.Zip = dr.Zip
    --     AND fc.City = fc.City
    LEFT JOIN [DW_Operations].[dbo].[DimOutreach]           o
        ON fc.OutreachID = o.OutreachId
    LEFT JOIN [DWWorking].[Prod].[EMR_Credentials]          ec
                ON fc.OutreachId = ec.[Outreach ID]
    LEFT JOIN chart_counts                                  cc
        ON fc.OutreachId = cc.OutreachId
    LEFT JOIN CleanSiteId                                   csi
        ON fc.OutreachId = csi.OutreachId
        
    WHERE   os.OutreachStatusId IN (4, 5, 6, 7, 8, 9, 11, 12, 14, 15, 16, 17, 18)
)

SELECT 
*
FROM final
                        ''')
        return sql