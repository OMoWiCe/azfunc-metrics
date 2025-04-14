import json
import logging
import pyodbc
from datetime import datetime
import pandas as pd
import math
import azure.functions as func

# Configuration: Set up environment variables for SQL connection
SQL_CONNECTION_STRING = "Driver={ODBC Driver 17 for SQL Server};Server=tcp:omowice.database.windows.net,1433;Database=OMoWiCe_DB;Uid=omowice_azure_dbadmin;Pwd=u1PdU9t7nvY9;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"

# Function to retrieve parameters from the LOCATION_PARAMETERS table
def get_location_parameters(location_id):
    try:
        # Connect to SQL database
        connection = pyodbc.connect(SQL_CONNECTION_STRING)
        cursor = connection.cursor()

        cursor.execute("""
            SELECT AVG_DEVICES_PER_PERSON, AVG_SIMS_PER_PERSON, WIFI_USAGE_RATIO, CELLULAR_USAGE_RATIO, UPDATE_INTERVAL 
            FROM LOCATION_PARAMETERS WHERE LOCATION_ID = ?
        """, location_id)

        result = cursor.fetchone()
        if result:
            avg_devices_per_person, avg_sims_per_person, wifi_usage_ratio, cellular_usage_ratio, update_interval = result
            return avg_devices_per_person, avg_sims_per_person, wifi_usage_ratio, cellular_usage_ratio, update_interval
        else:
            logging.error(f"No parameters found for location: {location_id}")
            return None

    except Exception as e:
        logging.error(f"Error retrieving location parameters: {e}")
        return None
    finally:
        cursor.close()
        connection.close()

# Function to estimate live count based on the IoT data
def estimate_live_count(wifi_list, cellular_list, avg_devices_per_person, avg_sims_per_person, wifi_usage_ratio, cellular_usage_ratio):
    estimated_people_from_wifi = (len(wifi_list) / avg_devices_per_person) * wifi_usage_ratio
    estimated_people_from_cell = (len(cellular_list) / avg_sims_per_person) * cellular_usage_ratio
    estimated_live_occupancy = math.ceil(estimated_people_from_wifi + estimated_people_from_cell)
    logging.info(f"Estimated people from Wi-Fi: {estimated_people_from_wifi}, Estimated people from Cellular: {estimated_people_from_cell}, Estimated Count: {estimated_live_occupancy}")
    return estimated_live_occupancy

# Function to calculate turnover time based on ACTIVE_DEVICES and PENDING_DEACTIVATIONS
def calculate_turnover_time(location_id):
    try:
        # Connect to SQL database
        connection = pyodbc.connect(SQL_CONNECTION_STRING)
        cursor = connection.cursor()

        # Get the list of devices in PENDING_DEACTIVATIONS
        cursor.execute("""
            SELECT first_seen, last_seen
            FROM PENDING_DEACTIVATIONS
            WHERE location_id = ?
        """, location_id)

        devices = cursor.fetchall()

        # Calculate the median time difference between first_seen and last_seen
        time_diffs = []
        for device in devices:
            first_seen = device[0]
            last_seen = device[1]
            time_diff = (last_seen - first_seen).total_seconds()
            time_diffs.append(time_diff)

        # remove 0 values from time_diffs
        time_diffs = [diff for diff in time_diffs if diff > 0]

        if time_diffs:
            time_diffs.sort()
            median_time_diff = int(pd.Series(time_diffs).median())
        else:
            median_time_diff = 0  # If no devices are in the pending deactivation list, set turnover to 0

        # Remove devices from PENDING_DEACTIVATIONS after turnover time calculation
        cursor.execute("""
            DELETE FROM PENDING_DEACTIVATIONS WHERE location_id = ?
        """, location_id)

        connection.commit()
        logging.info(f"Turnover time calculated for location {location_id}: {median_time_diff} seconds")
        return median_time_diff  # Return the turnover time in seconds

    except Exception as e:
        logging.error(f"Error calculating turnover time: {e}")
        return 0

    finally:
        cursor.close()
        connection.close()

# Main function triggered by IoT Hub (Event Hub)
omowice_metrics_blueprint = func.Blueprint()
@omowice_metrics_blueprint.event_hub_message_trigger(
        arg_name="azeventhub", 
        event_hub_name="iothub-ehub-omowice-io-55488012-390c020d8e",
        connection="IOTHUB_LISTEN_ENDPOINT") 
        
def process_metrics(azeventhub: func.EventHubEvent):
    # Parse the incoming event (IoT Hub telemetry data)
    logging.info("--------------------------- Processing IoT Hub message ---------------------------")
    message_body = azeventhub.get_body().decode('utf-8') 
    logging.info(f"Received message: {message_body}")
    try:
        # Convert the message body to a dictionary
        message_body = json.loads(message_body)
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse message body as JSON: {e}")
        return  # Exit the function if parsing fails

    # Extract necessary data from the message
    location_id = message_body['location_id']
    wifi_occupancy_list = message_body['wifi_occupancy_list']
    cellular_occupancy_list = message_body['cellular_occupancy_list']
    local_timestamp = message_body['local_timestamp']   
    # Retrieve parameters from the LOCATION_PARAMETERS table
    params = get_location_parameters(location_id)
    if not params:
        logging.error(f"Failed to retrieve parameters for location: {location_id}. Using default values.")
        avg_devices_per_person, avg_sims_per_person, wifi_usage_ratio, cellular_usage_ratio, update_interval = 1.5, 2, 0.25, 0.75, 1
    else:
        logging.info(f"Parameters retrieved for location: {location_id}")
        avg_devices_per_person, avg_sims_per_person, wifi_usage_ratio, cellular_usage_ratio, update_interval = params   
    try:
        # Connect to SQL database
        connection = pyodbc.connect(SQL_CONNECTION_STRING)
        cursor = connection.cursor()   

        # Step 1: Update missed_count for all the devices related to the location_id
        cursor.execute("""
            UPDATE ACTIVE_DEVICES
            SET missed_count = missed_count + 1
            WHERE location_id = ?
        """, location_id)   
        logging.info(f"Missed count updated for all the devices in location: {location_id}") 

        # Step 2: Add unique devices to ACTIVE_DEVICES table and reset missed_count to 0 if they are present in the current occupancy list
        # Process Wi-Fi devices
        if wifi_occupancy_list:
            for device in wifi_occupancy_list:
                cursor.execute("""
                    IF NOT EXISTS (SELECT 1 FROM ACTIVE_DEVICES WHERE location_id = ? AND device_id = ?)
                    BEGIN
                        INSERT INTO ACTIVE_DEVICES (location_id, device_id, first_seen, last_seen, missed_count)
                        VALUES (?, ?, ?, ?, 0)
                    END
                    ELSE
                    BEGIN
                        UPDATE ACTIVE_DEVICES
                        SET last_seen = ?, missed_count = 0
                        WHERE location_id = ? AND device_id = ?
                    END
                """, (location_id, device, location_id, device, local_timestamp, local_timestamp, local_timestamp, location_id, device))    
        logging.info(f"Wi-Fi devices processed for location: {location_id}")
        # Process Cellular devices
        if cellular_occupancy_list:
            for device in cellular_occupancy_list:
                cursor.execute("""
                    IF NOT EXISTS (SELECT 1 FROM ACTIVE_DEVICES WHERE location_id = ? AND device_id = ?)
                    BEGIN
                        INSERT INTO ACTIVE_DEVICES (location_id, device_id, first_seen, last_seen, missed_count)
                        VALUES (?, ?, ?, ?, 0)
                    END
                    ELSE
                    BEGIN
                        UPDATE ACTIVE_DEVICES
                        SET last_seen = ?, missed_count = 0
                        WHERE location_id = ? AND device_id = ?
                    END
                """, (location_id, device, location_id, device,  local_timestamp, local_timestamp, local_timestamp, location_id, device))   
        logging.info(f"Cellular devices processed for location: {location_id}")
        # Step 3: Move devices to PENDING_DEACTIVATIONS if missed_count > threshold (e.g., 5)
        cursor.execute("""
            INSERT INTO PENDING_DEACTIVATIONS (location_id, device_id, first_seen, last_seen, missed_count)
            SELECT location_id, device_id, first_seen, last_seen, missed_count
            FROM ACTIVE_DEVICES
            WHERE location_id = ? AND missed_count > 5
        """, location_id)   
        # Step 4: Delete devices moved to PENDING_DEACTIVATIONS from ACTIVE_DEVICES
        cursor.execute("""
            DELETE FROM ACTIVE_DEVICES WHERE location_id = ? AND missed_count > 5
        """, location_id)   
        connection.commit()
        logging.info(f"Devices processed for location: {location_id}")  
    except Exception as e:
        logging.error(f"Error processing devices: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()  
    # Step 5: Calculate turnover time every 5 minutes of local time
    if datetime.strptime(local_timestamp, '%Y-%m-%dT%H:%M:%S.%f').minute % (update_interval*5) == 0:
        turnover_time = calculate_turnover_time(location_id) 
    else:
        turnover_time = 0   
    # Step 6: Estimate live count
    live_count = estimate_live_count(wifi_occupancy_list, cellular_occupancy_list, avg_devices_per_person, avg_sims_per_person, wifi_usage_ratio, cellular_usage_ratio)    
    # Step 7: Update the MAIN_METRICS table
    try:
        connection = pyodbc.connect(SQL_CONNECTION_STRING)
        cursor = connection.cursor() 
        local_timestamp_formated = datetime.strptime(local_timestamp, '%Y-%m-%dT%H:%M:%S.%f').strftime('%Y-%m-%d %H:%M') 
        if turnover_time != 0:
            cursor.execute("""
                IF EXISTS (SELECT 1 FROM MAIN_METRICS WHERE LOCATION_ID = ? AND [DATE] = CAST(? AS DATETIME))
                BEGIN
                    UPDATE MAIN_METRICS
                    SET LIVE_COUNT = ?,
                        TURNOVER_TIME = ?,
                        CREATED_AT = ?
                    WHERE LOCATION_ID = ? AND [DATE] = CAST(? AS DATETIME);
                END
                ELSE
                BEGIN
                    INSERT INTO MAIN_METRICS (LOCATION_ID, [DATE], LIVE_COUNT, TURNOVER_TIME, CREATED_AT)
                    VALUES (?, CAST(? AS DATETIME), ?, ?, ?);
                END
            """, (location_id, local_timestamp_formated, live_count, turnover_time, local_timestamp, location_id, local_timestamp_formated, location_id, local_timestamp_formated, live_count, turnover_time, local_timestamp))
        else:
            # Get the last turnover time (order it by) from the MAIN_METRICS table and keep it unchanged
            cursor.execute("""
                    SELECT TOP 1 TURNOVER_TIME FROM MAIN_METRICS WHERE LOCATION_ID = ? ORDER BY CREATED_AT DESC
                """, location_id)
            result = cursor.fetchone()
            last_turnover_time = result[0] if result else 0
            cursor.execute("""
                IF EXISTS (SELECT 1 FROM MAIN_METRICS WHERE LOCATION_ID = ? AND [DATE] = CAST(? AS DATETIME))
                BEGIN
                    UPDATE MAIN_METRICS
                    SET LIVE_COUNT = ?,
                        CREATED_AT = ?
                    WHERE LOCATION_ID = ? AND [DATE] = CAST(? AS DATETIME);
                END
                ELSE
                BEGIN
                    INSERT INTO MAIN_METRICS (LOCATION_ID, [DATE], LIVE_COUNT, TURNOVER_TIME, CREATED_AT)
                    VALUES (?, CAST(? AS DATETIME), ?, ?, ?);
                END
            """, (location_id, local_timestamp_formated, live_count, local_timestamp, location_id, local_timestamp_formated, location_id, local_timestamp_formated, live_count, last_turnover_time, local_timestamp))

        connection.commit()
        logging.info(f"Metrics updated for location: {location_id}")
    except Exception as e:
        logging.error(f"Error updating database: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()
