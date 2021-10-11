import pandas as pd
import time
from datetime import date, timedelta, datetime

from dbo_Query import Query
# from Bus_day_calc import newPath
startTime_1 = time.time()

today = date.today()

def EMR_output():
      query = ('''
            SELECT Distinct
                  om.OutreachID											AS	[OutreachID]
                  , os.OutreachStatusDescription							AS	[Outreach Status]
                  , om.OutreachTeam										AS 	[Outreach Team]
                  -- Agent ID?
                  , (lne.FirstName+' '+lne.LastName)						AS 	[AgentName]
                  , rm.InternalCategoryName							    AS	[Retrieval Method]
                  , pt.ProjectTypeDescription								AS 	[Project Type]
                  , atd.AuditTypeId                                       AS 	[Audit Type]
                  , lcd.Date										        As  [Last Call Date]    --- switch
                  , om.CallCount											As  [Call Count]
                  , s.Phone1                                              AS  [Phone Number]
                  , csid.SiteCleanId                                      AS  [Site Clean ID]
                  , CONCAT(S.Address1, ' ', S.Address2)                   AS  [Address]
                  , S.City												AS	[City]
                  , S.State												AS  [State]
                  , S.Zip	
                  , m.Market												AS  [Zip]
                  , icd.insertdate										AS 	[InsertDate]
                  , region.Region											AS 	[Region]
                  , SUM (CASE WHEN fc.ChartStatusID IN (1,2,3,4,5,11,13,22,23,29,30,31)
                              THEN 1
                              ELSE 0
                              END)										AS	[Net Charts]
                  , SUM (CASE WHEN fc.ChartStatusID IN (1,2,11,22)
                              THEN 1
                              ELSE 0
                              END)										AS	[ToGo Charts]
                  , lne.FirstName + ' '+lne.LastName as 'Coordinator Name'
                  , lne.LoginName as 'CF Username'
                  , Prjdue.[Project Due Date]
                  , last_call.[NoteDate]
                  , last_call.Note

            FROM DW_Operations.dbo.FactChart					AS	fc
            JOIN DW_Operations.dbo.DimOutreach					AS	om
                  ON fc.OutreachId = om.OutreachId
            JOIN DW_Operations.dbo.DimDate						AS lcd
                  ON om.LastCallDateId = lcd.DateKey
            JOIN DW_Operations.dbo.DimRetrievalMethod			AS	rm
                  ON fc.DerivedRetrievalMethod = rm.RetrievalMethodId
            JOIN DW_Operations.dbo.DimOutreachStatus			AS	os
                  ON fc.OutreachStatusId = os.OutreachStatusId
            JOIN DW_Operations.dbo.DimProject					AS	p
                  ON fc.ProjectId = p.ProjectId
            JOIN DW_Operations.dbo.DimProjectType				AS	pt
                  ON p.ProjectTypeId = pt.ProjectTypeId
            JOIN DW_Operations.dbo.DimAuditType					AS	atd
                  ON p.AuditTypeId = atd.AuditTypeId
            JOIN ChartFinder_Snap.dbo.OutreachMaster			AS	icd
                  on om.OutreachId = icd.OutreachId
            JOIN [ChartFinder_Snap].[dbo].[Site]                AS	csid
                  ON om.PrimarySiteId = csid.ID
            JOIN DW_Operations.dbo.DimSite					    AS	s
                  ON om.PrimarySiteId=s.SiteId
            JOIN DW_Operations.dbo.DimState						AS	st
                  ON fc.StateID = st.StateId
            Left Join DWWorking.prod.Optum_State_Lookup Region 
                  ON st.StateAbbreviation=Region.State
            LEFT JOIN --- Project Due date ---
                  (
                  SELECT DISTINCT
                        a.[Outreach ID]
                        ,a.[Project Due Date]
                  FROM [DWWorking].[Prod].[Master_Reporting_Table]  a
                  WHERE a.[Project Due Date]=(select MAX([Project Due Date]) from [DWWorking].[Prod].[Master_Reporting_Table] b where b.[Outreach ID]=a.[Outreach ID])
                  AND a.[Project Status] in ('New', 'In Process')
                  )												AS Prjdue
                  ON Prjdue.[Outreach ID] = OM.OutreachId 
            LEFT JOIN --- Note / and last date ---
                  (  
                  SELECT DISTINCT
                        a.[OutreachId]
                        ,CAST(a.[NoteDate] AS DATE) [NoteDate]
                        ,a.[Note]
                        ,a.UserId
                  FROM [chartfinder].[dbo].[MasterNotes]  a
                  WHERE a.NoteDate=(select MAX(NoteDate) from [chartfinder].[dbo].[MasterNotes] b where b.OutreachId=a.OutreachId)
                  AND a.NoteType = '120'
                  )												AS last_call
                  ON last_call.Outreachid = OM.OutreachId 
                  ---  total charts, togo charts --- not even apart of top quier, Togo??
            LEFT JOIN [DW_Operations].[dbo].[DimLoginName]		AS lne 
                  ON lne.LoginNameId = last_call.UserId
            LEFT JOIN (
                        SELECT DISTINCT
                         [State]
                        ,[Region] AS Market
                        FROM [SiteScheduler].[dbo].[Market] AS m
                        WHERE m.UpdateDate=(select MAX(UpdateDate) from SiteScheduler.dbo.Market b where b.UpdateDate = m.UpdateDate) 
                  ) as m
                  on m.State = S.State
            WHERE 
                  p.ProjectStatusID < 4
                  AND Prjdue.[Project Due Date] >= CAST(GETDATE() AS Date)
                  AND rm.RetrievalGroup = 'EMR Remote'
                  --------------------------------------------------Alter Call Queue
                  AND om.OutreachTeam in ('Sub 15','60 to 299','15 to 59')
                  --and togo.ToGoCharts > 0
                  -- ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- --  
                  --## Removing Self Retrievals and Attestations ##--					
                  AND atd.AuditTypeDescription NOT IN ( 'Attestations', 'PAF' )																											--#
                  --Removing the Self Retrievals																																									--#
                  AND rm.RetrievalMethodDescription NOT IN ( 'Self Retrieval', 'UHC Onsite' ) --Take out Self Retrieval Methods															--#
                  AND
                  (	om.RetrievalTeam = 'Unknown' 
                        OR om.RetrievalTeam = 'CIOX' 
                        OR om.RetrievalTeam = 'Genpact' 
                        OR om.RetrievalTeam = 'Genpact Offshore' )	--REMOVE SELF RETRIEVAL TEAMS																							--#
                  AND
                  (   
                        ( fc.PNPCodeId <> 51 AND fc.PNPCodeId <> 52 AND fc.PNPCodeId <> 53 )																																			--#
                        OR fc.PNPCodeId IS NULL
                  )					--REMOVE SELF RETRIEVAL CODES																							--#
                  AND pt.ProjectTypeDescription NOT LIKE 'PAF%'						--Not part of Chart Review																			--#
                  AND pt.ProjectTypeDescription NOT LIKE 'SOPAF%'						--Not part of Chart Review																			--#
                  AND pt.ProjectTypeDescription NOT LIKE 'PSC%'																															--#
                  AND LOWER(pt.ProjectTypeDescription) NOT LIKE '%sr'					--Self Retrieval Project Types																		--#
                  AND LOWER(pt.ProjectTypeDescription) NOT LIKE '%mailrm%'			--Mailroom projects are for third party intake charts - not actual retrievals by Ciox				--#
                  AND LOWER(pt.ProjectTypeDescription) NOT LIKE '%attestations%'		--not part of chart review - processed differently													--#
                  AND LOWER(pt.ProjectTypeDescription) NOT LIKE '%preschedule%'		--For projects that were scheduled before the file loaded. Does not happen any more, but take it out.--#
                  AND pt.ProjectTypeDescription <> 'Optum Loaded'						--Closed way back(probably 2016), take it out.														--#
                  AND pt.ProjectTypeDescription <> 'Optum CIOX Coding'				--Coding only project. Take out.																	--#
                  AND pt.ProjectTypeDescription <> 'HEDIS Self Retrieval'				--Self Retrieval Project																			--#
                  AND p.ProjectId <> 'BSCA_HEDIS_SelfRetrieval'																															--#	
                  AND pt.ProjectTypeDescription <> 'Cigna HEDIS Intake Services Only'	--Self Retrieval Project	
                  AND atd.AuditTypeDescription <> '[]Specialty'--						-- ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
            GROUP BY
                  om.OutreachID
                  ,csid.SiteCleanId 
                  ,om.PrimarySiteId
                  , os.OutreachStatusDescription							
                  , om.OutreachTeam
                  , (lne.FirstName+' '+lne.LastName)						
                  ,rm.InternalCategoryName									
                  , pt.ProjectTypeDescription
                  , om.CallCount																	
                  ,atd.AuditTypeId
                  ,lcd.date
                  ,s.Phone1
                  ,CONCAT(S.Address1, ' ', S.Address2)
                  ,S.City
                  ,S.State
                  ,S.Zip
                  ,m.Market
                  ,icd.insertdate	
                  ,Region.Region
                  ,last_call.[NoteDate]
                  ,last_call.Note
                  ,lne.FirstName 
                  ,lne.LastName
                  ,lne.LoginName
                  ,Prjdue.[Project Due Date]

            HAVING
            SUM (CASE WHEN fc.ChartStatusID IN (1,2,11,22)
                              THEN 1
                              ELSE 0
                              END) > 0 
                  ''')
      df = Query('DWWorking', query, 'Base Table')
      return df

#df = EMR_output()/
#print(df['Market)

# executionTime_1 = (time.time() - startTime_1)
# print("-----------------------------------------------")
# print('Time: ' + str(executionTime_1))
# print("-----------------------------------------------")