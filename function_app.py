import logging
import azure.functions as func
import os
from nestedaaddb.nested_groups import SyncNestedGroups
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
import asyncio
import pytz
import datetime

# Function to get group list from Azure Blob Storage
def get_user_group_list():
    blob_service_url = os.environ["BLOB_SERVICE_URL"]
    blob_container_name = os.environ["BLOB_CONTAINER_NAME"]
    blob_name = os.environ["BLOB_NAME"]

    blob_service_client = BlobServiceClient(blob_service_url, credential=DefaultAzureCredential())
    blob_container_client = blob_service_client.get_container_client(blob_container_name)
    blob_client = blob_container_client.get_blob_client(blob_name)

    user_group_list = blob_client.download_blob().readall().decode('utf-8').splitlines()
    
    return user_group_list

app = func.FunctionApp()

@app.schedule(
    schedule=os.environ["TIMER_SCHEDULE"], 
    arg_name="myTimer", 
    run_on_startup=True,
    use_monitor=False
) 
async def sync_AAD_to_ADB_Unity_Catalog(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')
    
    # Get the current time in Australia/Brisbane
    brisbane_now = datetime.datetime.now(pytz.timezone('Australia/Brisbane'))
    logging.info('Python timer trigger function ran at %s AEST', brisbane_now)

    # Create an instance of SyncNestedGroups and load the configuration
    sn = SyncNestedGroups()

    # Process each Entra account from the list
    user_group_list = get_user_group_list()
    tasks = [sn.sync(entra_name, False) for entra_name in user_group_list] # False here means to actually Sync.  Change value to True if you want to DryRun only
    await asyncio.gather(*tasks)

    logging.info('Function execution completed.')