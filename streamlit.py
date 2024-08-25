import psycopg2
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

# Database connection parameters
db_params = {
    'dbname': 'd5pt3225ki095v',
    'user': 'uchk5knobsqvs7',
    'password': 'pb82e547f1beee9040983d54a568e419b3d91a76ea16d6aaedd49b5fb41f1bcfe',
    'host': 'ec2-23-20-93-193.compute-1.amazonaws.com',
    'port': '5432'
}

# SQL query to fetch the required records with NORMAL CLIENT classification
fetch_records_query = """
WITH StageHistory AS (
    SELECT 
        csp.client_id,
        c.fullname AS client_name,
        e.fullname AS employee_name,
        csp.current_stage,
        csp.created_on AS time_entered_stage,
        csp.stage_name,  -- Directly fetching the stage_name from the table
        ROW_NUMBER() OVER (PARTITION BY csp.client_id ORDER BY csp.created_on ASC) AS stage_order
    FROM 
        public.client_stage_progression csp
    JOIN 
        public.client c ON csp.client_id = c.id
    JOIN 
        public.employee e ON c.assigned_employee = e.id
),
ClientTimeDiff AS (
    SELECT 
        client_id,
        MIN(time_entered_stage) AS first_stage_time,
        MAX(time_entered_stage) AS last_stage_time,
        EXTRACT(EPOCH FROM (MAX(time_entered_stage) - MIN(time_entered_stage))) / 3600 AS time_diff_hours,
        MAX(current_stage) AS max_stage_reached
    FROM 
        StageHistory
    GROUP BY 
        client_id
),
DynamicStages AS (
    SELECT
        client_id,
        stage_name,
        time_entered_stage,
        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY time_entered_stage) AS stage_number
    FROM
        StageHistory
)
SELECT 
    sh.client_id,
    CONCAT('https://services.followupboss.com/2/people/view/', sh.client_id) AS followup_boss_link,
    sh.client_name,
    sh.employee_name,
    CASE 
        WHEN ctd.max_stage_reached = 8 AND ctd.time_diff_hours <= 35 THEN 'NORMAL CLIENT'
        ELSE 'NOT NORMAL CLIENT'
    END AS client_status,
    MAX(CASE WHEN ds.stage_number = 1 THEN ds.stage_name END) AS "STAGE_1_NAME",
    MAX(CASE WHEN ds.stage_number = 1 THEN ds.time_entered_stage END) AS "TIME_ENTERED_STAGE_1",
    MAX(CASE WHEN ds.stage_number = 2 THEN ds.stage_name END) AS "STAGE_2_NAME",
    MAX(CASE WHEN ds.stage_number = 2 THEN ds.time_entered_stage END) AS "TIME_ENTERED_STAGE_2",
    MAX(CASE WHEN ds.stage_number = 3 THEN ds.stage_name END) AS "STAGE_3_NAME",
    MAX(CASE WHEN ds.stage_number = 3 THEN ds.time_entered_stage END) AS "TIME_ENTERED_STAGE_3",
    MAX(CASE WHEN ds.stage_number = 4 THEN ds.stage_name END) AS "STAGE_4_NAME",
    MAX(CASE WHEN ds.stage_number = 4 THEN ds.time_entered_stage END) AS "TIME_ENTERED_STAGE_4",
    MAX(CASE WHEN ds.stage_number = 5 THEN ds.stage_name END) AS "STAGE_5_NAME",
    MAX(CASE WHEN ds.stage_number = 5 THEN ds.time_entered_stage END) AS "TIME_ENTERED_STAGE_5",
    MAX(CASE WHEN ds.stage_number = 6 THEN ds.stage_name END) AS "STAGE_6_NAME",
    MAX(CASE WHEN ds.stage_number = 6 THEN ds.time_entered_stage END) AS "TIME_ENTERED_STAGE_6",
    MAX(CASE WHEN ds.stage_number = 7 THEN ds.stage_name END) AS "STAGE_7_NAME",
    MAX(CASE WHEN ds.stage_number = 7 THEN ds.time_entered_stage END) AS "TIME_ENTERED_STAGE_7",
    MAX(CASE WHEN ds.stage_number = 8 THEN ds.stage_name END) AS "STAGE_8_NAME",
    MAX(CASE WHEN ds.stage_number = 8 THEN ds.time_entered_stage END) AS "TIME_ENTERED_STAGE_8",
    MAX(CASE WHEN ds.stage_number = 9 THEN ds.stage_name END) AS "STAGE_9_NAME",
    MAX(CASE WHEN ds.stage_number = 9 THEN ds.time_entered_stage END) AS "TIME_ENTERED_STAGE_9"
FROM 
    StageHistory sh
JOIN 
    ClientTimeDiff ctd ON sh.client_id = ctd.client_id
LEFT JOIN 
    DynamicStages ds ON sh.client_id = ds.client_id
GROUP BY 
    sh.client_id, sh.client_name, sh.employee_name, ctd.time_diff_hours, ctd.max_stage_reached
ORDER BY 
    sh.client_id;
"""

# SQL query to fetch the latest stage each client is in (without Follow-Up Boss link)
fetch_latest_stage_query = """
SELECT 
    csp.client_id,
    c.fullname AS client_name,
    e.fullname AS employee_name,
    CASE 
        WHEN csp.current_stage = 2 THEN 'Stage 2: Initial Contact'
        WHEN csp.current_stage = 3 THEN 'Stage 3: Requirement Collection'
        WHEN csp.current_stage = 4 THEN 'Stage 4: Property Touring'
        WHEN csp.current_stage = 5 THEN 'Stage 5: Property Tour and Feedback'
        WHEN csp.current_stage = 6 THEN 'Stage 6: Application and Approval'
        WHEN csp.current_stage = 7 THEN 'Stage 7: Post-Approval and Follow-Up'
        WHEN csp.current_stage = 8 THEN 'Stage 8: Commission Collection'
        WHEN csp.current_stage = 1 THEN 'Stage 1: Not Interested'
        WHEN csp.current_stage = 9 THEN 'Stage 9: Dead Stage'
        ELSE 'Unknown Stage'
    END AS latest_stage_name
FROM 
    public.client_stage_progression csp
JOIN 
    public.client c ON csp.client_id = c.id
JOIN 
    public.employee e ON c.assigned_employee = e.id
WHERE 
    (csp.client_id, csp.created_on) IN (
        SELECT client_id, MAX(created_on)
        FROM public.client_stage_progression
        GROUP BY client_id
    )
ORDER BY 
    csp.client_id;
"""


# SQL query to fetch employee-wise client stage information
fetch_employee_stage_query = """
SELECT 
    csp.client_id,
    CONCAT('https://services.followupboss.com/2/people/view/', csp.client_id) AS followup_boss_link,
    e.fullname AS employee_name,
    c.fullname AS client_name,
    csp.stage_name AS current_stage_name
FROM 
    public.client_stage_progression csp
JOIN 
    public.client c ON csp.client_id = c.id
JOIN 
    public.employee e ON c.assigned_employee = e.id
WHERE 
    (csp.client_id, csp.created_on) IN (
        SELECT client_id, MAX(created_on)
        FROM public.client_stage_progression
        GROUP BY client_id
    )
ORDER BY 
    e.fullname, c.fullname;
"""

def fetch_data(query):
    connection = None
    cursor = None
    try:
        # Connect to the database
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()
        
        # Execute the query to fetch records
        cursor.execute(query)
        
        # Fetch all records
        records = cursor.fetchall()
        
        # Get column names from cursor
        column_names = [desc[0] for desc in cursor.description]
        
        # Create a pandas DataFrame from the records
        df = pd.DataFrame(records, columns=column_names)
        
        return df
        
    except Exception as error:
        st.error(f"Error fetching records: {error}")
    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()

# Streamlit application
st.title("Client Stage Progression Report")

# Add a refresh button
if st.button('Refresh Data'):
    # Fetch data for the client stage progression report
    data = fetch_data(fetch_records_query)

    # Rename columns to "First_Stage_Recorded", "Second_Stage_Recorded", etc.
    rename_columns = {
        'STAGE_1_NAME': 'First_Recorded',
        'TIME_ENTERED_STAGE_1': 'Time_Entered_First_Recorded',
        'STAGE_2_NAME': 'Second_Recorded',
        'TIME_ENTERED_STAGE_2': 'Time_Entered_Second_Recorded',
        'STAGE_3_NAME': 'Third_Recorded',
        'TIME_ENTERED_STAGE_3': 'Time_Entered_Third_Recorded',
        'STAGE_4_NAME': 'Fourth_Recorded',
        'TIME_ENTERED_STAGE_4': 'Time_Entered_Fourth_Recorded',
        'STAGE_5_NAME': 'Fifth_Recorded',
        'TIME_ENTERED_STAGE_5': 'Time_Entered_Fifth_Recorded',
        'STAGE_6_NAME': 'Sixth_Recorded',
        'TIME_ENTERED_STAGE_6': 'Time_Entered_Sixth_Recorded',
        'STAGE_7_NAME': 'Seventh_Recorded',
        'TIME_ENTERED_STAGE_7': 'Time_Entered_Seventh_Recorded',
        'STAGE_8_NAME': 'Eighth_Recorded',
        'TIME_ENTERED_STAGE_8': 'Time_Entered_Eighth_Recorded',
        'STAGE_9_NAME': 'Ninth_Recorded',
        'TIME_ENTERED_STAGE_9': 'Time_Entered_Ninth_Recorded'
    }

    # Apply the renaming to the DataFrame
    if data is not None:
        data.rename(columns=rename_columns, inplace=True)

    # Display the data in a Streamlit table
    if data is not None:
        st.dataframe(data)
        st.write(f"Total records fetched: {len(data)}")

    # Fetch the latest stage each client is in for the summary
    latest_stage_data = fetch_data(fetch_latest_stage_query)

    # Display the summarized data in a table
    if latest_stage_data is not None:
        stage_summary = latest_stage_data.groupby('latest_stage_name').size().reset_index(name='Number of Clients')
        st.subheader("Summary of Clients in Latest Stage")
        st.table(stage_summary)
        
        # Create a bar chart to visualize the summary
        st.subheader("Bar Chart of Clients in Latest Stage")
        fig, ax = plt.subplots()
        ax.bar(stage_summary['latest_stage_name'], stage_summary['Number of Clients'])
        ax.set_xlabel('Stage')
        ax.set_ylabel('Number of Clients')
        ax.set_title('Clients in Latest Stage')
        plt.xticks(rotation=45, ha='right')
        st.pyplot(fig)
    
    # Fetch employee-wise client stage information
    employee_stage_data = fetch_data(fetch_employee_stage_query)

    if employee_stage_data is not None:
        st.subheader("Client Stages by Employee")
        
        # Display the data in a tabular form
        st.dataframe(employee_stage_data)

    # Create a bar chart to visualize the number of clients per employee in different stages
    st.subheader("Bar Chart of Client Stages by Employee")
    fig, ax = plt.subplots(figsize=(14, 8))  # Increase the figure size
    employee_stage_summary = employee_stage_data.groupby(['employee_name', 'current_stage_name']).size().unstack().fillna(0)
    employee_stage_summary.plot(kind='bar', stacked=True, ax=ax)
    ax.set_xlabel('Employee', fontsize=12)
    ax.set_ylabel('Number of Clients', fontsize=12)
    ax.set_title('Client Stages by Employee', fontsize=16)
    plt.xticks(rotation=45, ha='right', fontsize=10)  # Adjust the rotation and font size for x-axis labels
    plt.yticks(fontsize=10)  # Adjust the font size for y-axis labels
    st.pyplot(fig)

