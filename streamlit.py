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

# SQL query to fetch the required records (excluding the client_status logic)
fetch_max_stages_query = """
WITH StageHistory AS (
    SELECT 
        csp.client_id,
        ROW_NUMBER() OVER (PARTITION BY csp.client_id ORDER BY csp.created_on ASC) AS stage_order
    FROM 
        public.client_stage_progression csp
)
SELECT MAX(stage_order) AS max_stage
FROM StageHistory;
"""

# Step 2: Adjust the Data Fetch Query
def fetch_dynamic_stages_query(max_stage):
    stages_select = ",\n".join(
        [f"MAX(CASE WHEN ds.stage_number = {i} THEN ds.stage_name END) AS Data_{i}_recorded," +
         f"MAX(CASE WHEN ds.stage_number = {i} THEN ds.time_entered_stage END) AS Time_for_data{i}_recorded"
         for i in range(1, max_stage + 1)]
    )

    return f"""
    WITH StageHistory AS (
        SELECT 
            csp.client_id,
            c.fullname AS client_name,
            e.fullname AS employee_name,
            csp.current_stage,
            csp.created_on AS time_entered_stage,
            csp.stage_name,
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
        {stages_select}
    FROM 
        StageHistory sh
    LEFT JOIN 
        DynamicStages ds ON sh.client_id = ds.client_id
    GROUP BY 
        sh.client_id, sh.client_name, sh.employee_name
    ORDER BY 
        sh.client_id;
    """

# SQL query to calculate the average time difference for clients whose current_stage=8
calculate_average_time_diff_query = """
WITH StageHistory AS (
    SELECT 
        csp.client_id,
        csp.created_on AS time_entered_stage,
        ROW_NUMBER() OVER (PARTITION BY csp.client_id ORDER BY csp.created_on ASC) AS stage_order
    FROM 
        public.client_stage_progression csp
    WHERE 
        csp.current_stage = 8
),
ClientTimeDiff AS (
    SELECT 
        client_id,
        MIN(time_entered_stage) AS first_stage_time,
        MAX(time_entered_stage) AS last_stage_time,
        EXTRACT(EPOCH FROM (MAX(time_entered_stage) - MIN(time_entered_stage))) / 3600 AS time_diff_hours
    FROM 
        StageHistory
    GROUP BY 
        client_id
)
SELECT 
    AVG(time_diff_hours) AS avg_time_diff_hours
FROM 
    ClientTimeDiff;
"""

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

# SQL query to classify clients based on the calculated average time difference
calculate_average_time_diff_query = """
WITH StageHistory AS (
    SELECT 
        csp.client_id,
        csp.current_stage,
        csp.created_on AS time_entered_stage,
        ROW_NUMBER() OVER (PARTITION BY csp.client_id ORDER BY csp.created_on DESC) AS row_num -- Ordering DESC to get the last row
    FROM 
        public.client_stage_progression csp
),
ClientTimeDiff AS (
    SELECT 
        client_id,
        MIN(time_entered_stage) AS first_stage_time,
        MAX(time_entered_stage) AS last_stage_time,
        current_stage,
        EXTRACT(EPOCH FROM (MAX(time_entered_stage) - MIN(time_entered_stage))) / 3600 AS time_diff_hours
    FROM 
        StageHistory
    WHERE 
        row_num = 1 -- Selecting only the last row for each client
    GROUP BY 
        client_id, current_stage
    HAVING
        current_stage = 8 -- Ensure that the last stage is 8
)
SELECT 
    AVG(time_diff_hours) AS avg_time_diff_hours
FROM 
    ClientTimeDiff;
"""

# SQL query to classify clients based on the calculated average time difference
classify_clients_query_template = """
WITH StageHistory AS (
    SELECT 
        csp.client_id,
        csp.current_stage,
        csp.created_on AS time_entered_stage,
        ROW_NUMBER() OVER (PARTITION BY csp.client_id ORDER BY csp.created_on DESC) AS row_num -- Ordering DESC to get the last row
    FROM 
        public.client_stage_progression csp
),
ClientTimeDiff AS (
    SELECT 
        client_id,
        MIN(time_entered_stage) AS first_stage_time,
        MAX(time_entered_stage) AS last_stage_time,
        current_stage,
        EXTRACT(EPOCH FROM (MAX(time_entered_stage) - MIN(time_entered_stage))) / 3600 AS time_diff_hours
    FROM 
        StageHistory
    WHERE 
        row_num = 1 -- Selecting only the last row for each client
    GROUP BY 
        client_id, current_stage
)
SELECT 
    ctd.client_id,
    c.fullname AS client_name,
    e.fullname AS employee_name,
    CASE 
        WHEN ctd.current_stage = 8 AND ctd.time_diff_hours <= {avg_time_diff_hours} THEN 'NORMAL CLIENT'
        ELSE 'NOT NORMAL CLIENT'
    END AS client_status
FROM 
    ClientTimeDiff ctd
JOIN 
    public.client c ON ctd.client_id = c.id
JOIN 
    public.employee e ON c.assigned_employee = e.id
ORDER BY 
    ctd.client_id;
"""
def fetch_max_stage():
    connection = None
    cursor = None
    try:
        # Connect to the database
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()
        
        # Execute the query to fetch the maximum number of stages
        cursor.execute(fetch_max_stages_query)
        
        # Fetch the result (maximum stage number)
        max_stage = cursor.fetchone()[0]
        
        return max_stage
        
    except Exception as error:
        st.error(f"Error fetching maximum stage: {error}")
    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()

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

def fetch_average_time_diff():
    connection = None
    cursor = None
    try:
        # Connect to the database
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()
        
        # Execute the query to calculate average time difference
        cursor.execute(calculate_average_time_diff_query)
        
        # Fetch the result (average time difference)
        avg_time_diff = cursor.fetchone()[0]
        
        return avg_time_diff
        
    except Exception as error:
        st.error(f"Error calculating average time difference: {error}")
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
    # Fetch the average time difference for clients in stage 8
    avg_time_diff_hours = fetch_average_time_diff()

    max_stage = fetch_max_stage()    # Fetch data for the client stage progression report
    dynamic_query = fetch_dynamic_stages_query(max_stage)
    data = fetch_data(dynamic_query)

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
    
    # Classify clients as NORMAL or NOT NORMAL based on the calculated average time difference
    classify_clients_query = classify_clients_query_template.format(avg_time_diff_hours=avg_time_diff_hours)
    classified_clients_data = fetch_data(classify_clients_query)

    if classified_clients_data is not None:
        st.subheader("NORMAL CLIENTS")
        normal_clients = classified_clients_data[classified_clients_data['client_status'] == 'NORMAL CLIENT']
        st.dataframe(normal_clients)
        st.write(f"Total NORMAL CLIENTS: {len(normal_clients)}")

        st.subheader("NOT NORMAL CLIENTS")
        not_normal_clients = classified_clients_data[classified_clients_data['client_status'] == 'NOT NORMAL CLIENT']
        st.dataframe(not_normal_clients)
        st.write(f"Total NOT NORMAL CLIENTS: {len(not_normal_clients)}")
