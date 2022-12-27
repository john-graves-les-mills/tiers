import datetime
import streamlit as st
import numpy as np
import pandas as pd
import sidetable as stb
import snowflake.connector
import plotly.express as px

today = datetime.datetime.today().date()

def find_percentage_total(df, start_col=1):
    """Find total and percent of total for columns of Pandas dataframe

    @start_col Index of starting column (typically 1 as first column -- column 0 -- is a date or label)
    """
    # Get values for col1,col2 and col3
    total = pd.Series(data=np.zeros(len(df)))
    col_count = len(df.columns)
    for i in range(start_col, col_count):
        total += df.iloc[:,i]
    df.insert(len(df.columns), 'total', total)
    for i in range(start_col, col_count + 1):        
        pct_of_total = round((df.iloc[:,i]/total)*100, 2)

        # Create Pandas DF with new column of pct_of_total
        df.insert(len(df.columns),f"{df.columns[i]} %", pct_of_total)
    
    # Pull original dataframe to show total and %
    return df


def run_query(query):
    query_pandas = snowflake_cursor.execute(query).fetch_pandas_all()
    return query_pandas


def t(title_string, no_year=False, silent=False):
    """Add "as at {today}" to title. Usage: t(title_sting)

    @title_string text to preceed the "as at" part
    """
    if no_year == False:
        today = datetime.datetime.today().strftime('%d %b %Y')
        title = f"{title_string} as at {today}"
    else:
        today = datetime.datetime.today().strftime('%d %b')
        title = f"{title_string} - {today}"


conn_sflake = snowflake.connector.connect(**st.secrets["snowflake"], client_session_keep_alive=True)
snowflake_cursor = conn_sflake.cursor()   

st.header("Acquisitions by Tier Since 19 December 2022")
df = run_query(f"""SELECT * FROM LMI_TEST.APPFIGURES.BASE_TIER_ACQUISITIONS_20221221""")
days_in_data = (max(df.MAX_CREATED_DATE.values) - min(df.MIN_CREATED_DATE.values)).days + 1
st.subheader(f"New Signups by Tier for {days_in_data} Days to {str(max(df.MAX_CREATED_DATE.values))}")

df1 = df.pivot(index='SUBSCRIPTION_STORE' , columns='TIER' , values='COUNT')
df2 = df1.sort_values(by='TIER#base', ascending=False).head(5).copy()
df2.reset_index(inplace=True)
df3 = find_percentage_total(df2)
for col in df3.columns[1:]:
    df3[col] = df3[col].astype(int)
col_map  = {'SUBSCRIPTION_STORE':'Store - Top 5',
            'TIER#base':'Base',
            'TIER#premium':'Premium',
            'total':'Total',
            'TIER#base %':'Base %',
            'TIER#premium %':'Premium %',
            'total %':'Total %'}    
df3.columns = [col_map[item] for item in list(df3.columns)]
df3 = df3[['Store - Top 5', 'Base', 'Premium', 'Total', 'Base %', 'Premium %', 'Total %']]    

fig = px.bar(df3, x=df3.columns[0], y=df3.columns[1:3], width=940)
fig.update_layout(xaxis_title='Top Stores',
                 yaxis_title='Count of New Subscriptions',
                  legend_title_text='Tier',
                 barmode='group')
st.plotly_chart(fig,  use_container_width=True)
# CSS to inject contained in a string
hide_table_row_index = """
            <style>
            thead tr th:first-child {display:none}
            tbody th {display:none}
            </style>
            """
st.markdown(hide_table_row_index, unsafe_allow_html=True)
st.table(df3)

# 20221228 0906 