def ptr():
  sql = """
      SELECT 
            A.[Project Year]
          , A.[Audit Type]
          , A.[Project Type]
          , A.[Client Project]
          , A.[Project ID]
          , A.[Today's Targeted charts]
          , A.[Targeted charts]
          , A.[QA Completed]
          , A.[SB.EMR Remote]
          -- , A.[AV.EMR Remote]
      FROM DWWorking.Prod.Project_Tracking_Report_V2 AS A
      WHERE A.[Insert Date] = CAST(GETDATE() AS DATE)
              AND [Project Status] IN ('New', 'In Process')
              AND A.[Project Due Date] >= CAST(GETDATE() AS DATE)
              and A.[Net Charts] > 0
              """
  return sql