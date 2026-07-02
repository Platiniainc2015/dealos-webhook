"""
Vapi → GHL Webhook Handler
===========================
This script runs as a FastAPI server that receives end-of-call reports from Vapi
and processes them into GoHighLevel (creates/updates contacts, creates opportunities,
moves pipeline stages, sends contracts, triggers buyer blasts).

Deployed as the intermediary between Vapi's server URL and GHL.
"""

import json
import re
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import requests
from buyer_disposition import process_under_contract_deal
from follow_up_scheduler import schedule_follow_ups, cancel_follow_ups, start_scheduler

app = FastAPI(title="DealOS Vapi-GHL Webhook Handler")

# Start the background follow-up scheduler on app startup
start_scheduler()

# ============ CONFIGURATION ============
GHL_API_KEY = "pit-56454803-dd76-4ad8-a567-01fe6b515da1"
GHL_LOCATION_ID = "RcG1IS88ALK6Yxsrz1yk"
GHL_PIPELINE_ID = "kX6agzFv6hrJMr6TMHgO"
GHL_BASE_URL = "https://services.leadconnectorhq.com"

# The tag that triggers the GHL workflow for contract sending
CONTRACT_READY_TAG = "Contract Ready to Send"

# Map Vapi call data fields to GHL custom fields
# NOTE: GHL's 'Email' is a standard field, not a custom field, so it's handled separately.
CUSTOM_FIELD_MAP = {
    "address": "custom_field_id_for_address", # Replace with actual GHL Custom Field ID for Address
    "phone_number": "custom_field_id_for_phone", # Replace with actual GHL Custom Field ID for Phone Number
    # Add other custom fields as needed
}

# ============ GHL API FUNCTIONS ============
def get_ghl_headers( ):
    return {
        "Authorization": f"Bearer {GHL_API_KEY}",
        "Version": "2021-07-28",
        "Content-Type": "application/json",
    }

def create_or_update_contact(contact_data: dict):
    print(f"Attempting to create or update contact with data: {contact_data}")
    search_url = f"{GHL_BASE_URL}/contacts/search/or"
    query_params = []
    if contact_data.get("email"):
        query_params.append(f"email={contact_data['email']}")
    if contact_data.get("phone"):
        query_params.append(f"phone={contact_data['phone']}")

    if not query_params:
        print("No email or phone provided for contact search. Cannot create/update contact.")
        return None

    search_url += "?" + "&".join(query_params)

    try:
        search_response = requests.get(search_url, headers=get_ghl_headers())
        search_response.raise_for_status()
        existing_contacts = search_response.json().get("contacts", [])

        if existing_contacts:
            contact_id = existing_contacts[0]["id"]
            print(f"Existing contact found with ID: {contact_id}. Updating contact.")
            update_url = f"{GHL_BASE_URL}/contacts/{contact_id}"
            response = requests.put(update_url, headers=get_ghl_headers(), json=contact_data)
            response.raise_for_status()
            print(f"Contact {contact_id} updated successfully.")
            return contact_id
        else:
            print("No existing contact found. Creating new contact.")
            create_url = f"{GHL_BASE_URL}/contacts/"
            response = requests.post(create_url, headers=get_ghl_headers(), json=contact_data)
            response.raise_for_status()
            new_contact_id = response.json().get("contact", {}).get("id")
            print(f"New contact {new_contact_id} created successfully.")
            return new_contact_id
    except requests.exceptions.RequestException as e:
        print(f"Error creating or updating contact: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"GHL API Error Response: {e.response.text}")
        return None

def add_tag_to_contact(contact_id: str, tag: str):
    print(f"Attempting to add tag '{tag}' to contact {contact_id}")
    url = f"{GHL_BASE_URL}/contacts/{contact_id}/tags"
    headers = get_ghl_headers()
    payload = {"tags": [tag]}
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        print(f"Tag '{tag}' added to contact {contact_id} successfully.")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error adding tag '{tag}' to contact {contact_id}: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"GHL API Error Response: {e.response.text}")
        return False

def create_opportunity(contact_id: str, opportunity_name: str, pipeline_stage_id: str):
    print(f"Attempting to create opportunity for contact {contact_id}")
    url = f"{GHL_BASE_URL}/opportunities/"
    headers = get_ghl_headers()
    payload = {
        "contactId": contact_id,
        "name": opportunity_name,
        "pipelineId": GHL_PIPELINE_ID,
        "pipelineStageId": pipeline_stage_id,
        "status": "open",
        "locationId": GHL_LOCATION_ID,
    }
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        opportunity_id = response.json().get("opportunity", {}).get("id")
        print(f"Opportunity {opportunity_id} created successfully for contact {contact_id}.")
        return opportunity_id
    except requests.exceptions.RequestException as e:
        print(f"Error creating opportunity for contact {contact_id}: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"GHL API Error Response: {e.response.text}")
        return None

def update_opportunity_stage(opportunity_id: str, pipeline_stage_id: str):
    print(f"Attempting to update opportunity {opportunity_id} to stage {pipeline_stage_id}")
    url = f"{GHL_BASE_URL}/opportunities/{opportunity_id}"
    headers = get_ghl_headers()
    payload = {
        "pipelineStageId": pipeline_stage_id,
        "status": "open",
    }
    try:
        response = requests.put(url, headers=headers, json=payload)
        response.raise_for_status()
        print(f"Opportunity {opportunity_id} stage updated successfully.")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error updating opportunity {opportunity_id} stage: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"GHL API Error Response: {e.response.text}")
        return False

# ============ VAPI WEBHOOK HANDLER ============

def extract_contact_info(messages: list):
    contact_info = {"full_name": None, "email": None, "phone": None, "address": None}
    for message in messages:
        if message.get("role") == "user":
            # Attempt to extract full name
            name_match = re.search(r"My name is (.*?)(?:\.|,|$)", message["content"], re.IGNORECASE)
            if name_match: 
                contact_info["full_name"] = name_match.group(1).strip()
            
            # Attempt to extract email
            email_match = re.search(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", message["content"])
            if email_match:
                contact_info["email"] = email_match.group(0).strip()

            # Attempt to extract phone number (simple regex, might need refinement)
            phone_match = re.search(r"\b\d{3}[-.]?\d{3}[-.]?\d{4}\b", message["content"])
            if phone_match:
                contact_info["phone"] = phone_match.group(0).strip()

            # Attempt to extract address (this is a very basic example, will need more robust logic)
            address_match = re.search(r"(?:\d{1,})\s(?:[A-Za-z0-9\s]+)\,\s(?:[A-Za-z]+)\,\s(?:[A-Za-z]{2})\s(?:\d{5})", message["content"])
            if address_match:
                contact_info["address"] = address_match.group(0).strip()

    return contact_info


@app.post("/vapi-webhook")
async def handle_vapi_webhook(request: Request):
    try:
        data = await request.json()
        print(f"Received Vapi webhook: {json.dumps(data, indent=2)}")

        call_status = data.get("status")
        customer_sentiment = data.get("customerSentiment")
        call_messages = data.get("messages", [])
        ended_reason = data.get("endedReason")
        phone_number = data.get("customer", {}).get("phone")

        if call_status == "ended":
            print("Call has ended. Processing contact information and tags.")
            contact_info = extract_contact_info(call_messages)
            
            # Ensure phone number from call data is prioritized if available
            if phone_number and not contact_info.get("phone"):
                contact_info["phone"] = phone_number

            # Prepare contact data for GHL
            ghl_contact_data = {
                "firstName": contact_info.get("full_name", "").split(" ")[0] if contact_info.get("full_name") else "",
                "lastName": " ".join(contact_info.get("full_name", "").split(" ")[1:]) if contact_info.get("full_name") else "",
                "email": contact_info.get("email"), # Email is a standard field
                "phone": contact_info.get("phone"),
                "locationId": GHL_LOCATION_ID,
                "customFields": [],
            }

            # Add custom fields from map
            for vapi_field, ghl_field_id in CUSTOM_FIELD_MAP.items():
                if contact_info.get(vapi_field):
                    ghl_contact_data["customFields"].append({
                        "id": ghl_field_id,
                        "value": contact_info[vapi_field]
                    })
            
            # Filter out empty custom fields
            ghl_contact_data["customFields"] = [cf for cf in ghl_contact_data["customFields"] if cf["value"]]

            contact_id = create_or_update_contact(ghl_contact_data)

            if contact_id:
                # Example: Add a tag if sentiment is positive and call ended normally
                if customer_sentiment == "positive" and ended_reason == "call_ended_by_user":
                    add_tag_to_contact(contact_id, CONTRACT_READY_TAG)
                    # Further actions like creating opportunity or sending contract can be added here
                    # For instance, if you have a specific pipeline stage for 'Contract Ready'
                    # opportunity_id = create_opportunity(contact_id, "New Deal", "your_pipeline_stage_id")
                    # if opportunity_id:
                    #     update_opportunity_stage(opportunity_id, "another_stage_id")

            return JSONResponse(content={"message": "Webhook processed"})

        return JSONResponse(content={"message": "Webhook received, but no action taken for this status"})

    except Exception as e:
        print(f"Error processing Vapi webhook: {str(e)}")
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.post("/server-url")
async def handle_server_url(request: Request):
    try:
        data = await request.json()
        message = data.get("message", {})
        # If Vapi is asking for dynamic variables
        if message.get("type") == "conversation-update":
            return JSONResponse(content={})
        # If Vapi is calling a tool
        if message.get("type") == "tool-calls":
            tool_calls = message.get("toolCalls", [])
            results = []
            for tool_call in tool_calls:
                tool_name = tool_call.get("function", {}).get("name")
                # Handle specific tool logic here if needed
                results.append({
                    "toolCallId": tool_call.get("id"),
                    "result": "success"
                })
            return JSONResponse(content={"results": results})
        return JSONResponse(content={})
    except Exception as e:
        print(f"Error in server-url: {str(e)}")
        return JSONResponse(content={"error": str(e)}, status_code=500)

if __name__ == "__main__":
    import uvicorn
    # Use the PORT environment variable if available, otherwise default to 8000
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
