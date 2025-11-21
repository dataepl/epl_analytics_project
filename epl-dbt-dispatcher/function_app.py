import json
import logging
import os
import requests
import azure.functions as func
from datetime import datetime

# Create the Function App instance
app = func.FunctionApp()

# Configuration - these will come from your Function App settings in Azure
# Locally, you can set them in local.settings.json for testing
def get_config():
    """
    Retrieve configuration from environment variables.
    This helps us see what's configured and handle missing values gracefully.
    """
    config = {
        "GITHUB_OWNER": os.environ.get("GITHUB_OWNER", ""),
        "GITHUB_REPO": os.environ.get("GITHUB_REPO", ""),
        "GITHUB_PAT": os.environ.get("GITHUB_PAT", ""),
        "PATH_BEGINS": os.environ.get("PATH_BEGINS", ""),
        "PATH_ENDS": os.environ.get("PATH_ENDS", "")
        #"PATH_BEGINS": os.environ.get("PATH_BEGINS", "/blobServices/default/containers/ingestion/blobs/dsp_summary/"),
        #"PATH_ENDS": os.environ.get("PATH_ENDS", "__Solution.csv")
    }
    
    # Log configuration status (without exposing the PAT)
    logging.info(f"GitHub Owner: {config['GITHUB_OWNER']}")
    logging.info(f"GitHub Repo: {config['GITHUB_REPO']}")
    logging.info(f"PAT Configured: {'Yes' if config['GITHUB_PAT'] else 'No'}")
    logging.info(f"Path Filter - Begins: {config['PATH_BEGINS']}")
    logging.info(f"Path Filter - Ends: {config['PATH_ENDS']}")
    
    return config

@app.event_grid_trigger(arg_name="azeventgrid")
def OnBlobCreatedDispatchToGitHub(azeventgrid: func.EventGridEvent):
    """
    Triggered when a blob is created in Azure Storage.
    Dispatches an event to GitHub to trigger the dbt workflow.
    
    The flow works like this:
    1. A CSV file is uploaded to blob storage
    2. Event Grid detects this and calls our function
    3. We check if it's a file we care about (__Solution.csv)
    4. If yes, we tell GitHub to start the dbt workflow
    5. The GitHub Action then waits for Snowpipe and runs dbt
    """
    
    logging.info('=== Azure Function Triggered by Event Grid ===')
    
    # Get our configuration
    config = get_config()
    
    # Validate we have the required configuration
    if not config["GITHUB_OWNER"] or not config["GITHUB_REPO"]:
        logging.error("Missing GitHub owner or repo configuration!")
        raise ValueError("GitHub configuration incomplete")
    
    if not config["GITHUB_PAT"]:
        logging.error("Missing GitHub PAT!")
        raise ValueError("GitHub PAT not configured")
    
    # Parse the event that triggered us
    try:
        event_data = azeventgrid.get_json()
        subject = azeventgrid.subject
        event_type = azeventgrid.event_type
        event_time = azeventgrid.event_time
        
        # The blob URL is in the data section
        blob_url = event_data.get("url", "") if event_data else ""
        
        logging.info(f"Event Type: {event_type}")
        logging.info(f"Subject: {subject}")
        logging.info(f"Blob URL: {blob_url}")
        
    except Exception as e:
        logging.error(f"Failed to parse event: {str(e)}")
        raise
    
    # Check if this is a file we should process
    # This is a safety check - Event Grid should already filter for us
    if config["PATH_BEGINS"] and not subject.startswith(config["PATH_BEGINS"]):
        logging.info(f"Skipping - subject doesn't match PATH_BEGINS filter")
        return
    
    if config["PATH_ENDS"] and not subject.endswith(config["PATH_ENDS"]):
        logging.info(f"Skipping - subject doesn't match PATH_ENDS filter")
        return
    
    # Extract the filename from the URL or subject
    # Example: from ".../2025/02/2025-02-15-DSK4__Solution.csv" 
    # we get "2025-02-15-DSK4__Solution.csv"
    if blob_url:
        file_name = blob_url.split("/")[-1]
    else:
        file_name = subject.split("/")[-1]
    
    logging.info(f"Processing file: {file_name}")
    
    # Build the GitHub API URL
    github_url = f"https://api.github.com/repos/{config['GITHUB_OWNER']}/{config['GITHUB_REPO']}/dispatches"
    
    # Set up headers for GitHub API
    headers = {
        "Authorization": f"token {config['GITHUB_PAT']}",
        "Accept": "application/vnd.github+json",
        "Content-Type": "application/json",
    }
    
    # Create the payload for GitHub
    # This is what triggers the workflow
    payload = {
        "event_type": "blob-created",  # This must match the workflow trigger
        "client_payload": {
            "blob_url": blob_url,
            "file_name": file_name,
            "event_time": event_time.isoformat() if event_time else datetime.utcnow().isoformat()
        }
    }
    
    logging.info(f"Dispatching to GitHub: {github_url}")
    logging.info(f"Payload: {json.dumps(payload, indent=2)}")
    
    # Send the request to GitHub
    try:
        response = requests.post(
            github_url,
            headers=headers,
            data=json.dumps(payload),
            timeout=30
        )
        
        # GitHub returns 204 No Content on success
        if response.status_code == 204:
            logging.info("✅ SUCCESS: GitHub workflow dispatch accepted")
            logging.info(f"Check your GitHub Actions at: https://github.com/{config['GITHUB_OWNER']}/{config['GITHUB_REPO']}/actions")
        else:
            # Something went wrong
            logging.error(f"❌ FAILED: GitHub returned status code {response.status_code}")
            logging.error(f"Response body: {response.text}")
            
            # Common error explanations
            if response.status_code == 404:
                logging.error("404 indicates either the repo doesn't exist or the PAT doesn't have access")
            elif response.status_code == 401:
                logging.error("401 indicates the PAT is invalid or expired")
            elif response.status_code == 422:
                logging.error("422 indicates the workflow file might not exist or event_type doesn't match")
                
            # Raise exception to trigger retry
            raise Exception(f"GitHub dispatch failed with status {response.status_code}")
            
    except requests.exceptions.Timeout:
        logging.error("❌ Request to GitHub timed out after 30 seconds")
        raise
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Request to GitHub failed: {str(e)}")
        raise