import pandas as pd

from pipeline.clean import clean
from pipeline.etc import time_check, table_drops, daily_piv, Business_Days
from pipeline.extract import update_table
from pipeline.outreach_status import outreach_status
from pipeline.score import stack_inventory
from pipeline.skills import skills
from log.log import df_len

busday = Business_Days

def main(force='n'):
    ### Get tables ###
    try:
        if force == 'y': update_table()
        table = pd.read_csv(f'data/extract/{busday.today_str}.csv')
    except:
        table = update_table()

    time_check(busday.now, 'EMR_output')
    # save table
    table_drops("push",'extract', table, f"{busday.today_str}.csv")

    # remove dubs
    def collect_dups(df):
        dubs = df[df.duplicated(subset = 'OutreachID')].sort_values('OutreachID')
        table_drops("push", "table_drop", dubs, "check_dubs_sql_extract.csv")
        return df.drop_duplicates(subset='OutreachID')

    # get updated status_log from jira
    status_log_raw = pd.read_csv(f'data/table_drop/status_log.csv')

    # run full pipeline
    scored = ( table.pipe(df_len)
                    .pipe(collect_dups)
                    .pipe(df_len)
                    .pipe(outreach_status, status_log_raw)
                    .pipe(clean, busday.today)
                    .pipe(skills)
                    .pipe(stack_inventory, 'PhoneNumber')
                    .pipe(df_len)
    )

    # Save final product
    table_drops('push','load',scored, f"{busday.today_str}.csv")
    table_drops('push','load', daily_piv(scored),'Coordinator_Pivot.csv')
    print(daily_piv(scored))

### Upload files
if __name__ == '__main__':
    main()
    