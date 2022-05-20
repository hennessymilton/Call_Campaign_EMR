def emrr():
        sql = ('''
WITH emr_chart as (
    SELECT DISTINCT
		fc.OutreachId
		,fc.ChartStatusId
		,fc.SiteId
		,rm.RetrievalMethodDescription
		,os.OutreachStatusDescription
		,fc.ProjectId
    FROM [DW_Operations].[dbo].[FactChart]              fc
    LEFT JOIN [DW_Operations].[dbo].[DimRetrievalMethod]    rm
        ON fc.RetrievalMethodId = rm.RetrievalMethodId
	LEFT JOIN [DW_Operations].[dbo].[DimOutreachStatus]     os
        ON fc.OutreachStatusId = os.OutreachStatusId
    LEFT JOIN [DW_Operations].[dbo].[DimProjectStatus]      ps
        ON fc.ProjectStatusId = ps.ProjectStatusId
    WHERE   rm.RetrievalGroup = 'EMR Remote'
    AND     ps.ProjectStatusId IN (1, 3)
)
, emr_org AS (
	SELECT DISTINCT
		 OutreachId
		,SiteId
		,RetrievalMethodDescription
		,OutreachStatusDescription
		,ProjectId
	FROM emr_chart	
	WHERE OutreachStatusDescription IN ('Unscheduled', 'PNP Released','Escalated','Acct Mgmt Research ')
)
, cf_phone AS (
    SELECT DISTINCT
		fc.OutreachId
		,psc.PhoneNum AS [PhoneNumber]
	FROM emr_org										fc
	LEFT JOIN [DW_Operations].[dbo].[DimProjectSite]    ps
		ON fc.SiteId = ps.SiteId
		AND fc.OutreachId = ps.OutreachId
	LEFT JOIN [DW_Operations].[dbo].[DimPrimarySiteContact] psc
		ON ps.PrimaryContactId = psc.ContactId
)
,RetrievalMethod as (
    SELECT 
        OutreachId
        ,STRING_AGG(RetrievalMethodDescription, ' | ') 'Sub RetrievalMethods'
    FROM (
        SELECT DISTINCT
             OutreachId
            ,RetrievalMethodDescription
        FROM emr_org                                                fc
        ) AS dist
    GROUP BY OutreachId
)
,charts as (
    SELECT
         OutreachId
        ,ChartStatusId
        ,count(ChartStatusId) AS chart_count

    FROM emr_chart
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
	HAVING SUM(CASE WHEN ChartStatusId IN (1, 2, 11, 22)      THEN chart_count ELSE 0 END) > 0
)
,final AS (
    SELECT DISTINCT
        fc.OutreachID              AS [OutreachID]
		,ROW_NUMBER() OVER( PARTITION BY fc.OutreachID
                            ORDER BY cf_phone.PhoneNumber) top_org
        ,adt.AuditTypeDescription  AS [Audit_Type]
        ,pt.ProjectTypeDescription AS [Project_Type]
        ,fc.OutreachStatusDescription Outreach_Status
        ,dmo.MasterSiteID
        ,cf_phone.PhoneNumber
        ,rm_agg.[Sub RetrievalMethods]
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
        ,ISNULL(
            MAX(dp.ProjectDueOnDateId) OVER (PARTITION BY fc.OutreachID),
            '')      AS 'DueDate'

    FROM emr_org                         					fc
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

    JOIN chart_counts										cc
        ON fc.OutreachId = cc.OutreachId
    LEFT JOIN RetrievalMethod                               rm_agg
        ON fc.OutreachId = rm_agg.OutreachId
    LEFT JOIN cf_phone
        ON fc.OutreachId = cf_phone.OutreachId

    WHERE	cc.ToGoCharts > 0
)

SELECT * 
FROM final
WHERE top_org = 1

                        ''')
        return sql