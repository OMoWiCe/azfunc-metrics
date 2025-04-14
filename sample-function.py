import logging
import json
import pyodbc
from datetime import datetime
import pandas as pd

# Configuration: Set up environment variables for SQL connection
SQL_CONNECTION_STRING = "Driver={ODBC Driver 18 for SQL Server};Server=tcp:omowice.database.windows.net,1433;Database=OMoWiCe_DB;Uid=omowice_azure_dbadmin;Pwd=u1PdU9t7nvY9;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"

# Function to retrieve parameters from the LOCATION_PARAMETERS table
def get_location_parameters(location_id):
    try:
        # Connect to SQL database
        connection = pyodbc.connect(SQL_CONNECTION_STRING)
        cursor = connection.cursor()

        cursor.execute("""
            SELECT AVG_DEVICES_PER_PERSON, AVG_SIMS_PER_PERSON, WIFI_USAGE_RATIO, CELLULAR_USAGE_RATIO 
            FROM LOCATION_PARAMETERS WHERE LOCATION_ID = ?
        """, location_id)

        result = cursor.fetchone()
        if result:
            avg_devices_per_person, avg_sims_per_person, wifi_usage_ratio, cellular_usage_ratio = result
            return avg_devices_per_person, avg_sims_per_person, wifi_usage_ratio, cellular_usage_ratio
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
def estimate_live_count(wifi_count, cellular_count, avg_devices_per_person, avg_sims_per_person, wifi_usage_ratio, cellular_usage_ratio):
    estimated_people_from_wifi = (wifi_count / avg_devices_per_person) * wifi_usage_ratio
    estimated_people_from_cell = (cellular_count / avg_sims_per_person) * cellular_usage_ratio
    return int(round(estimated_people_from_wifi + estimated_people_from_cell, 0))

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
        return median_time_diff  # Return the turnover time in seconds

    except Exception as e:
        logging.error(f"Error calculating turnover time: {e}")
        return 0

    finally:
        cursor.close()
        connection.close()

# Main function triggered by IoT Hub (Event Hub)
def process_metrics(azeventhub: func.EventHubEvent'):

    # Parse the incoming event (IoT Hub telemetry data)
    for message in azeventhub:
        message_body = azeventhub.get_body().decode('utf-8')

        # Extract necessary data from the message
        location_id = message_body['location_id']
        wifi_occupancy_list = message_body['wifi_occupancy_list']
        cellular_occupancy_list = message_body['cellular_occupancy_list']

        # Retrieve parameters from the LOCATION_PARAMETERS table
        params = get_location_parameters(location_id)
        if not params:
            continue  # Skip this message if parameters are not found

        avg_devices_per_person, avg_sims_per_person, wifi_usage_ratio, cellular_usage_ratio = params

        # Step 1: Add unique devices to ACTIVE_DEVICES table
        try:
            # Connect to SQL database
            connection = pyodbc.connect(SQL_CONNECTION_STRING)
            cursor = connection.cursor()

            # Process Wi-Fi devices
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
                        SET last_seen = ?
                        WHERE location_id = ? AND device_id = ?
                    END
                """, (location_id, device, location_id, device, message_body['local_timestamp'], location_id, device, message_body['local_timestamp']))

            # Process Cellular devices
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
                        SET last_seen = ?
                        WHERE location_id = ? AND device_id = ?
                    END
                """, (location_id, device, location_id, device, message_body['local_timestamp'], location_id, device, message_body['local_timestamp']))

            # Step 2: Update missed_count for devices that were not present in the current message
            cursor.execute("""
                UPDATE ACTIVE_DEVICES
                SET missed_count = missed_count + 1
                WHERE location_id = ? AND device_id NOT IN (?, ?) AND missed_count <= 5
            """, location_id, *wifi_occupancy_list, *cellular_occupancy_list)

            # Step 3: Move devices to PENDING_DEACTIVATIONS if missed_count > threshold (e.g., 5)
            cursor.execute("""
                INSERT INTO PENDING_DEACTIVATIONS (location_id, device_id, first_seen, last_seen)
                SELECT location_id, device_id, first_seen, last_seen
                FROM ACTIVE_DEVICES
                WHERE location_id = ? AND missed_count > 5
            """, location_id)

            # Step 4: Delete devices moved to PENDING_DEACTIVATIONS from ACTIVE_DEVICES
            cursor.execute("""
                DELETE FROM ACTIVE_DEVICES WHERE location_id = ? AND missed_count > 5
            """, location_id)

            connection.commit()

        except Exception as e:
            logging.error(f"Error processing devices: {e}")
        finally:
            cursor.close()
            connection.close()

        # Step 5: Calculate turnover time
        turnover_time = calculate_turnover_time(location_id)

        # Step 6: Estimate live count
        live_count = estimate_live_count(len(wifi_occupancy_list), len(cellular_occupancy_list),
                                         avg_devices_per_person, avg_sims_per_person, wifi_usage_ratio, cellular_usage_ratio)

        # Step 7: Update the MAIN_METRICS table
        try:
            connection = pyodbc.connect(SQL_CONNECTION_STRING)
            cursor = connection.cursor()

            cursor.execute("""
                IF EXISTS (SELECT 1 FROM MAIN_METRICS WHERE LOCATION_ID = ? AND [DATE] = CAST(SYSUTCDATETIME() AS DATE))
                BEGIN
                    UPDATE MAIN_METRICS
                    SET PREV_LIVE_COUNT = LIVE_COUNT,
                        PREV_TURNOVER_TIME = TURNOVER_TIME,
                        LIVE_COUNT = ?,
                        TURNOVER_TIME = ?
                    WHERE LOCATION_ID = ? AND [DATE] = CAST(SYSUTCDATETIME() AS DATE);
                END
                ELSE
                BEGIN
                    INSERT INTO MAIN_METRICS (LOCATION_ID, [DATE], LIVE_COUNT, TURNOVER_TIME, PREV_LIVE_COUNT, PREV_TURNOVER_TIME, CREATED_AT)
                    VALUES (?, CAST(SYSUTCDATETIME() AS DATE), ?, ?, NULL, NULL, SYSUTCDATETIME());
                END
            """, (location_id, live_count, turnover_time, location_id, location_id, live_count, turnover_time))

            connection.commit()
            logging.info(f"Metrics updated for location: {location_id}")
        except Exception as e:
            logging.error(f"Error updating database: {e}")
        finally:
            cursor.close()
            connection.close()
