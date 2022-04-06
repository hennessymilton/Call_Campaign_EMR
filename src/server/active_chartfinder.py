def sql():
    return """
            SELECT 
            OM.[OutreachId] [OutreachID]
            
            FROM [ChartFinder].[dbo].[OutreachMaster] OM
            JOIN ChartFinder.dbo.ProjectSite PS 
                    ON OM.PrimarySiteId = PS.SiteId
                        AND PS.OutreachID = OM.OutreachID
            JOIN ChartFinder.dbo.Project P
                ON PS.ProjectId = P.ProjectId
            WHERE OM.RetrievalMethod in (8, 23) --'EMR - Remote','EMR - Remote Queued'
            AND OM.OutreachStatus in ('Unscheduled', 'PNP Released','Escalated','Acct Mgmt Research ')
            AND P.Status in ('3', '1')
            AND P.DueDate > getdate()
        """