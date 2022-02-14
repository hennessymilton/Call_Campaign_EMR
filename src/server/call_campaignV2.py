def emrr():
        sql = ('''
WITH base as (
    SELECT
         OM.OutreachID
        ,ROW_NUMBER() OVER( PARTITION BY OM.OutreachID
                            ORDER BY OM.OutreachID) top_org
        ,PT.Name AS 'Project Type'
        ,A.Name AS 'Audit Type'
        ,(
            CASE
                WHEN OM.OutreachStatus = 'Scheduled' AND
                    CONVERT(VARCHAR(10), OM.ScheduleDate, 1) < CONVERT(VARCHAR(10), GETDATE(), 1) THEN 'Past Due'
                ELSE OM.OutreachStatus
            END) AS 'Outreach Status'
        ,msi.MSId
        ,msic.Phone AS [MSI_PhoneNumber]
        ,SC.PHONENUM AS [PhoneNumber]
        ,ISNULL(RM.Name, 'Off Site Pending') AS 'Retrieval Method'
        ,mrt.[Sub Retrieval Method]
        ,ec.[Remote Sites Name]
        ,CONCAT(S.Address1, ' ', S.Address2) AS 'Address'
        ,S.City
        ,S.State
        ,S.Zip
        ,ISNULL(S.SiteCleanId, '') AS 'Site Clean ID'
        ,ISNULL(pnp.Name, '') AS 'PNP Code'

        ,OC.ToGoCharts
        ,OC.TotalCharts
        ,OC.RetrievedCharts
        ,ISNULL(OC.CallCount, 0) AS 'CallCount'
        ,ISNULL(CONVERT(VARCHAR(10), OM.ScheduleDate, 1), '') AS 'ScheduleDate'
        ,ISNULL(CONVERT(VARCHAR(10), OD.LastCallDate, 1), '') AS 'Last Call'
        ,ISNULL(CONVERT(VARCHAR(10), OM.InsertDate, 1), '') AS InsertDate
        ,CONVERT(VARCHAR(10), DATEADD(WEEKDAY, 9, CONVERT(VARCHAR(10), GETDATE(), 1)),1 ) AS 'Recommended Schedule Date'
        ,CAST(p.Duedate AS DATE) AS 'DueDate'
  
    FROM ChartFinder.dbo.OutreachMaster OM 
    JOIN ChartFinder.dbo.ProjectSite PS 
        ON OM.PrimarySiteId = PS.SiteId
            AND PS.OutreachID = OM.OutreachID
    JOIN ChartFinder.dbo.SiteContact sc 
        ON sc.id = PS.PrimaryContactId
    JOIN ChartFinder.dbo.OutreachCount OC 
        ON OM.OutreachID = OC.OutreachID
    LEFT JOIN ChartFinder.dbo.OutreachDates OD 
        ON OM.OutreachID = OD.OutreachID
    JOIN ChartFinder.dbo.Project P 
        ON PS.ProjectId = P.ProjectId
    JOIN ChartFinder.dbo.[Site] S 
        ON OM.PrimarySiteId = S.id
    JOIN ChartFinder.dbo.List PT 
        ON P.ProjectType = PT.Value
            AND PT.ListType = 'ProjectType'
    LEFT JOIN ChartFinder.dbo.List RM 
        ON OM.RetrievalMethod = RM.Value
            AND RM.ListType = 'RetrievalMethod'
    LEFT JOIN ChartFinder.dbo.List A 
        ON P.AuditType = A.Value
            AND A.ListType = 'AuditType'
    LEFT JOIN ChartFinder.dbo.List pnp 
        ON OM.PNPCode = pnp.Value
            AND pnp.ListType = 'PNPCode'
    LEFT JOIN MasterSiteID.dbo.MSIOutreach msi
        ON OM.OutreachId = msi.OutreachId
    LEFT JOIN MasterSiteID.dbo.MSIContact msic
        ON msi.MSId = msic.MSId
    LEFT JOIN [DWWorking].[Prod].[EMR_Credentials]                                AS ec
                ON om.OutreachId = ec.[Outreach ID]
    LEFT JOIN [DWWorking].[Prod].[Master_Reporting_Table] mrt
        ON om.OutreachId = mrt.[Outreach ID]
    WHERE P.Status = '3'
    -- AND sc.PrimartFlag = 1
    AND OC.ToGoCharts != '0'
    AND PT.CustomField6 <> 'Y'
    AND P.DueDate > getdate()
    AND RM.NAME IN ('EMR - Remote') -- 'Retrieval Method'
    AND SC.PrimaryFlag = 1
    AND SC.ActiveFlag = 1
    AND msic.[Primary] = 1
)

SELECT DISTINCT 
*
FROM base b
WHERE top_org = 1
                        ''')
        return sql