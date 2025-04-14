# Register this blueprint by adding the following line of code 
# to your entry point file.  
# app.register_functions(omowice-metrics-blueprint) 
# 
# Please refer to https://aka.ms/azure-functions-python-blueprints

import logging
import azure.functions as func


omowice_metrics_blueprint = func.Blueprint()

@omowice_metrics_blueprint.event_hub_message_trigger(
        arg_name="azeventhub", 
        event_hub_name="iothub-ehub-omowice-io-55488012-390c020d8e",
        connection="IOTHUB_LISTEN_ENDPOINT") 

def process_metrics(azeventhub: func.EventHubEvent):
    # Improved logging to include event hub metadata
    logging.info(
        'Python EventHub trigger processed an event from Event Hub : %s',
        azeventhub.get_body().decode('utf-8')
    )

