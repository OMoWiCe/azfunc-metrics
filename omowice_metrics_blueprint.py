import json
import logging
import pyodbc
from datetime import datetime
import pandas as pd
import math
import os
import time
import azure.functions as func

# Configuration: Set up environment variables for SQL connection
SQL_CONNECTION_STRING = os.getenv("DATABASE_CONNECTION_STRING")

# Function to retrieve parameters from the LOCATION_PARAMETERS table
def get_location_parameters(location_id):
    max_retries = 5  # Maximum number of retries
    retry_delay = 5  # Delay between retries in seconds
    retries = 0

    while retries < max_retries:
        try:
            # Connect to SQL database
            connection = pyodbc.connect(SQL_CONNECTION_STRING, timeout=10)  # Set timeout to 10 seconds
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
                logging.error(f"[OMOWICE-METRICS] No parameters found for location: {location_id}")
                return None

        except pyodbc.OperationalError as e:
            retries += 1
            logging.warning(f"[OMOWICE-METRICS] SQL connection timeout. Retrying {retries}/{max_retries}...")
            if retries >= max_retries:
                logging.error(f"[OMOWICE-METRICS] Maximum retries reached. Unable to connect to the database.")
                return None
            time.sleep(retry_delay)  # Wait before retrying

        except Exception as e:
            logging.error(f"[OMOWICE-METRICS] Error retrieving location parameters: {e}")
            return None

        finally:
            try:
                cursor.close()
                connection.close()
            except:
                pass

# Function to estimate live count based on the IoT data
def estimate_live_count(wifi_list, cellular_list, avg_devices_per_person, avg_sims_per_person, wifi_usage_ratio, cellular_usage_ratio):
    wifi_count = len(wifi_list)
    cellular_count = len(cellular_list)

    # Estimates from both sources
    est_wifi = wifi_count if wifi_count < 7 else (wifi_count / avg_devices_per_person) * (1+ wifi_usage_ratio)
    est_cell = cellular_count * cellular_usage_ratio if cellular_count < 7 else (cellular_count / avg_sims_per_person) * (1 + cellular_usage_ratio)

    # Apply a confidence multiplier to est_wifi
    wifi_confidence_multiplier = 1 + (0.01 * wifi_count)  # Boost WiFi estimate based on count
    est_wifi *= wifi_confidence_multiplier

    # Apply weighting factors to estimates due to wifi more accurate than cellular
    wifi_weight = 0.9
    cellular_weight = 0.8

    # Combined estimate with adjusted weights
    combined_estimate = (est_wifi * wifi_weight) + (est_cell * cellular_weight)

    # Apply overlap adjustment (assume 10% overlap)
    overlap_adjustment = 0.9
    final_estimate = math.ceil(combined_estimate * overlap_adjustment)

    # Confidence calculation (improved accuracy)
    min_threshold = 1.0  # Prevents division blow-up on small sums
    total_estimates = est_wifi + est_cell
    adjusted_avg = max(total_estimates / 2, min_threshold)
    relative_diff = abs(est_wifi - est_cell) / adjusted_avg if total_estimates > 0 else 0  # 
    confidence = max(0, round((1 - relative_diff), 2))

    # Adjust confidence for WiFi presence (more reliable source)
    if wifi_count > 0:
        wifi_bias = 0.2 * math.log(wifi_count + 2)  # Logarithmic adjustment for diminishing returns
        confidence = min(round(confidence + wifi_bias, 2), 1.0)

    logging.info(
        f"[OMOWICE-METRICS] WiFi: {wifi_count}, Cellular: {cellular_count}, "
        f"Estimates => WiFi: {est_wifi:.2f}, Cellular: {est_cell:.2f}, Final: {final_estimate}, Confidence: {confidence}"
    )

    return final_estimate

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
        logging.info(f"[OMOWICE-METRICS] Turnover time calculated for location {location_id}: {median_time_diff} seconds")
        return median_time_diff  # Return the turnover time in seconds

    except Exception as e:
        logging.error(f"[OMOWICE-METRICS] Error calculating turnover time: {e}")
        return 0

    finally:
        cursor.close()
        connection.close()

########################### Main function triggered by IoT Hub (Event Hub) ###########################
omowice_metrics_blueprint = func.Blueprint()
@omowice_metrics_blueprint.event_hub_message_trigger(
        arg_name="azeventhub", 
        event_hub_name="iothub-ehub-omowice-io-55488012-390c020d8e",
        connection="IOTHUB_LISTEN_ENDPOINT") 
        
def process_metrics(azeventhub: func.EventHubEvent):
    # Parse the incoming event (IoT Hub telemetry data)
    logging.info("[OMOWICE-METRICS] --------------------------- Processing IoT Hub message ---------------------------")
    message_body = azeventhub.get_body().decode('utf-8') 
    logging.info(f"[OMOWICE-METRICS] Received message: {message_body}")
    try:
        # Convert the message body to a dictionary
        message_body = json.loads(message_body)
    except json.JSONDecodeError as e:
        logging.error(f"[OMOWICE-METRICS] Failed to parse message body as JSON: {e}")
        return  # Exit the function if parsing fails

    # Extract necessary data from the message
    location_id = message_body['location_id']
    wifi_occupancy_list = message_body['wifi_occupancy_list']
    cellular_occupancy_list = message_body['cellular_occupancy_list']
    local_timestamp = message_body['local_timestamp']   
    # Retrieve parameters from the LOCATION_PARAMETERS table
    params = get_location_parameters(location_id)
    if not params:
        # Setting default values if parameters are not found
        logging.error(f"[OMOWICE-METRICS] Failed to retrieve parameters for location: {location_id}. Using default values.")
        avg_devices_per_person, avg_sims_per_person, wifi_usage_ratio, cellular_usage_ratio, update_interval = 1.5, 2, 0.25, 0.75, 1
    else:
        logging.info(f"[OMOWICE-METRICS] Parameters retrieved for location: {location_id}")
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
        logging.info(f"[OMOWICE-METRICS] Missed count updated for all the devices in location: {location_id}") 

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
        logging.info(f"[OMOWICE-METRICS] Wi-Fi devices processed for location: {location_id}")
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
        logging.info(f"[OMOWICE-METRICS] Cellular devices processed for location: {location_id}")
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
        logging.info(f"[OMOWICE-METRICS] Devices processed for location: {location_id}")  
    except Exception as e:
        logging.error(f"[OMOWICE-METRICS] Error processing devices: {e}")
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
        # Update the MAIN_METRICS table with the new metrics
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
                    SELECT TOP 1 TURNOVER_TIME, [DATE] FROM MAIN_METRICS WHERE LOCATION_ID = ? ORDER BY CREATED_AT DESC
                """, location_id)
            result = cursor.fetchone()
            last_turnover_time = result[0] if result else 0
            last_update_time = result[1] if result else None
            # Update last_turnover_time to 0 if the last_update_time is older than 60 minutes
            if last_update_time and (datetime.now() - last_update_time).total_seconds() > 3600:
                last_turnover_time = 0
                # also remove all the devices if the last_update_time is too old
                cursor.execute("""
                    DELETE FROM PENDING_DEACTIVATIONS WHERE location_id = ?
                """, location_id)
                cursor.execute("""
                    DELETE FROM ACTIVE_DEVICES WHERE location_id = ?
                """, location_id)
                logging.info(f"[OMOWICE-METRICS] Last metrics update was older than 60 minutes. Setting turnover time to 0 for location: {location_id} and removing all the devices.")
            # Update the MAIN_METRICS table with the new metrics and old turnover time
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
        logging.info(f"[OMOWICE-METRICS] Metrics updated in the db for location: {location_id}")
    except Exception as e:
        logging.error(f"[OMOWICE-METRICS] Error updating database: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()
