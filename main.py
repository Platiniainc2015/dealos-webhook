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

# Pipeline Stage IDs
STAGES = {
    "new_lead": "5307f6c7-cd8c-4734-9e9f-f3501729e3b1",
    "bot_called": "40d12da1-42fe-455b-a356-b745c5890547",
    "offer_made": "ef4bf4e9-603e-45d2-9d7f-18593d8076c5",
    "negotiating": "83fbfe4a-4ad3-4705-8356-c68c1843a15d",
    "creative_finance": "c797a5ff-7405-42fd-9f93-2e3bc4fc0334",
    "under_contract": "78a2c23a-af3f-4f23-9041-982c77225dff",
    "closed_paid": "580962d1-90d5-414d-879a-1012da0c7c1f",
    "follow_up": "36e5716f-5691-4fe2-8623-03a5be1e2680",
    "dead_lead": "9280eec3-f84f-4b3e-876d-15842c3b4489",
}

# Custom Field IDs mapped to Vapi extraction keys
CUSTOM_FIELD_MAP = {
    "property_address": "AWsgetu0ZbNd3tY9nQOL",
    "property_city": "01x2V6lOskknzGsxeHmP",
    "property_state": "GEbgniLi2k93QWu1jgGJ",
    "property_zipcode": "MxF1lkkUNiECmoMUsUlJ",
    "bedrooms": "qU71RLaxwlkaLEtX1ulg",
    "bathrooms": "ExO9VvSHcLb8rDtvXRwf",
    "property_condition": "ioi151BPAsu8qenEZXCR",
    "legal_description": "aDwFpXJ0TGDuv6tVg9ZC",
    "arv": "rLRx5x66TTJFJZjvQZDr",
    "estimated_repairs": "arQ6thbZ6OopYF6GTZwD",
    "offer_amount": "On3AI9NOiSdDKL67E2KD",
    "earnest_money_deposit": "3td9mx81PTxO457qZAjy",
    "mortgage_balance": "IW4LI6PMIcGFUmtYnOzk",
    "asking_price": "IcriLoo4QNE5Hlb3LTyQ",
    "seller_motivation": "WKdZEHoKyACRVy0fmDLG",
    "timeline_to_sell": "HGCZCaUNNBRlHCTXqqgX",
    "deal_type": "FdL5UxT5qivcJeEM1QXr",
    "property_occupied_or_vacant": "wXOcufHPuuAJ34dauADa",
    "monthly_mortgage_payment": "j4dBgG33nDGIkR3a9zvp",
    "internal_notes": "TINaWlCzma2S8AxTCfW0",
    "down_payment_amount": "yuQcDKbLdGP8wlpGsYU8",
    "financed_balance": "3Y79RyIlZmICX6RFV9Wd",
    "monthly_payment_amount": "XJsN7AGl9m3jqQN5jJAH",
    "first_payment_date": "XulbvRznk9r9Okk2jZW6",
    "balloon_due_date": "N273vr3XGYVAH1B1qVP4",
    "balloon_payment_amount": "IDAgnX4I692M5zoTmUu8",
    "estimated_balloon_payment_amount": "Mc2zXHG4kqo4rCqz1jLo",
    "purchase_price": "6fx70HEk4nd5f8DsQDW3",
    "coseller_full_name": "XpevADxN4AOVDqR7R4dh",
    "closing_date": "sL4j3qyaE1dQXOjIFQIz",
}

# Contract names in GHL (must match exactly what's in Documents & Contracts)
CONTRACT_NAMES = {
    "cash": "Purchase Agreement - Platinia Inc",
    "creative finance": "Creative Finance Agreement - Platinia Inc",
    "seller finance": "Creative Finance Agreement - Platinia Inc",
    "sub-to": "Creative Finance Agreement - Platinia Inc",
    "subject to": "Creative Finance Agreement - Platinia Inc",
    "subject-to": "Creative Finance Agreement - Platinia Inc",
    "hybrid": "Creative Finance Agreement - Platinia Inc",
}

# GHL API Headers
GHL_HEADERS = {
    "Authorization": f"Bearer {GHL_API_KEY}",
    "Version": "2021-07-28",
    "Content-Type": "application/json",
    "Accept": "application/json",
}


def determine_stage(call_data: dict) -> str:
    """Determine which pipeline stage to place the opportunity based on call outcome."""
    analysis = call_data.get("analysis", {})
    structured_data = analysis.get("structuredData", {}) or {}
    summary = analysis.get("summary", "").lower()
    
    # Check structured data first
    deal_type = structured_data.get("deal_type", "").lower()
    offer_amount = structured_data.get("offer_amount", "")
    seller_motivation = structured_data.get("seller_motivation", "").lower()
    
    # If seller accepted offer or contract mentioned
    if any(word in summary for word in ["accepted", "contract", "agreed", "signed"]):
        return "under_contract"
    
    # If creative finance terms discussed
    if deal_type in ["creative", "creative finance", "seller finance", "sub-to", "subject to"]:
        return "creative_finance"
    
    # If offer was made
    if offer_amount and offer_amount != "N/A":
        if any(word in summary for word in ["counter", "negotiat", "think about"]):
            return "negotiating"
        return "offer_made"
    
    # If call ended with no interest
    if any(word in summary for word in ["not interested", "hung up", "do not call", "no answer", "voicemail"]):
        return "dead_lead"
    
    # If follow up needed
    if any(word in summary for word in ["call back", "follow up", "later", "busy"]):
        return "follow_up"
    
    # Default: bot called
    return "bot_called"


def extract_contact_info(call_data: dict) -> dict:
    """Extract contact information from Vapi call data."""
    # Get the customer phone from the call
    customer = call_data.get("customer", {})
    phone = customer.get("number", "")
    
    # Get structured data from analysis
    analysis = call_data.get("analysis", {})
    structured_data = analysis.get("structuredData", {}) or {}
    
    # Try to extract name from structured data or transcript
    first_name = structured_data.get("seller_first_name", "")
    last_name = structured_data.get("seller_last_name", "")
    
    if not first_name:
        first_name = structured_data.get("first_name", "Unknown")
    if not last_name:
        last_name = structured_data.get("last_name", "Seller")
    
    return {
        "phone": phone,
        "firstName": first_name,
        "lastName": last_name,
        "structured_data": structured_data,
    }


def build_custom_fields(structured_data: dict) -> list:
    """Build the custom fields array for GHL from Vapi structured data."""
    custom_fields = []
    
    for vapi_key, ghl_field_id in CUSTOM_FIELD_MAP.items():
        value = structured_data.get(vapi_key, "")
        if value and value != "N/A" and value != "":
            custom_fields.append({
                "id": ghl_field_id,
                "field_value": str(value)
            })
    
    return custom_fields


def create_or_update_contact(contact_info: dict, custom_fields: list) -> str:
    """Create or update a contact in GHL. Returns contact ID."""
    # First try to find existing contact by phone
    phone = contact_info.get("phone", "")
    if phone:
        search_url = f"{GHL_BASE_URL}/contacts/search/duplicate"
        search_params = {
            "locationId": GHL_LOCATION_ID,
            "phone": phone
        }
        search_resp = requests.get(search_url, headers=GHL_HEADERS, params=search_params)
        if search_resp.status_code == 200:
            search_data = search_resp.json()
            contact = search_data.get("contact")
            if contact:
                # Update existing contact
                contact_id = contact["id"]
                update_url = f"{GHL_BASE_URL}/contacts/{contact_id}"
                update_data = {
                    "firstName": contact_info.get("firstName"),
                    "lastName": contact_info.get("lastName"),
                    "customFields": custom_fields,
                }
                requests.put(update_url, headers=GHL_HEADERS, json=update_data)
                return contact_id
    
    # Create new contact
    create_url = f"{GHL_BASE_URL}/contacts/"
    create_data = {
        "locationId": GHL_LOCATION_ID,
        "firstName": contact_info.get("firstName", "Unknown"),
        "lastName": contact_info.get("lastName", "Seller"),
        "phone": phone,
        "customFields": custom_fields,
    }
    
    resp = requests.post(create_url, headers=GHL_HEADERS, json=create_data)
    if resp.status_code in [200, 201]:
        return resp.json().get("contact", {}).get("id", "")
    
    return ""


def create_opportunity(contact_id: str, stage_id: str, structured_data: dict) -> str:
    """Create an opportunity in the DealOS pipeline."""
    property_address = structured_data.get("property_address", "Unknown Property")
    deal_type = structured_data.get("deal_type", "Cash")
    offer_amount = structured_data.get("offer_amount", "0")
    
    # Clean monetary value
    monetary_value = 0
    if offer_amount:
        cleaned = re.sub(r'[^\d.]', '', str(offer_amount))
        try:
            monetary_value = int(float(cleaned))
        except (ValueError, TypeError):
            monetary_value = 0
    
    opp_name = f"{property_address} - {deal_type} Deal"
    
    create_url = f"{GHL_BASE_URL}/opportunities/"
    create_data = {
        "pipelineId": GHL_PIPELINE_ID,
        "locationId": GHL_LOCATION_ID,
        "name": opp_name,
        "pipelineStageId": stage_id,
        "status": "open",
        "contactId": contact_id,
        "monetaryValue": monetary_value,
    }
    
    resp = requests.post(create_url, headers=GHL_HEADERS, json=create_data)
    if resp.status_code in [200, 201]:
        return resp.json().get("opportunity", {}).get("id", "")
    
    return ""


def send_contract(contact_id: str, deal_type: str, contact_info: dict):
    """
    Send the appropriate contract to the seller based on deal type.
    Uses GHL's Documents & Contracts API to send the document for signature.
    """
    # Determine which contract to send
    deal_type_lower = deal_type.lower() if deal_type else "cash"
    contract_name = CONTRACT_NAMES.get(deal_type_lower, CONTRACT_NAMES["cash"])
    
    # Get the contact's email for sending
    contact_url = f"{GHL_BASE_URL}/contacts/{contact_id}"
    contact_resp = requests.get(contact_url, headers=GHL_HEADERS)
    
    recipient_email = ""
    recipient_phone = ""
    if contact_resp.status_code == 200:
        contact_data = contact_resp.json().get("contact", {})
        recipient_email = contact_data.get("email", "")
        recipient_phone = contact_data.get("phone", "")
    
    # Log the contract send attempt
    print(f"[CONTRACT] Sending '{contract_name}' to contact {contact_id}")
    print(f"[CONTRACT] Email: {recipient_email}, Phone: {recipient_phone}")
    
    # GHL Documents API - Send document for signature
    # The document is sent via the GHL workflow trigger or directly via API
    # Since GHL doesn't have a direct "send document" API endpoint,
    # we trigger it via the workflow webhook
    
    contract_webhook_url = f"https://services.leadconnectorhq.com/hooks/{GHL_LOCATION_ID}/webhook-trigger/6b859ad6-fec2-4896-bacd-1284a0a2f4fa"
    
    contract_payload = {
        "action": "send_contract",
        "contact_id": contact_id,
        "contract_name": contract_name,
        "deal_type": deal_type,
        "recipient_email": recipient_email,
        "recipient_phone": recipient_phone,
        "seller_name": f"{contact_info.get('firstName', '')} {contact_info.get('lastName', '')}",
    }
    
    # Send to GHL workflow for contract processing
    resp = requests.post(contract_webhook_url, json=contract_payload)
    print(f"[CONTRACT] Webhook response: {resp.status_code}")
    
    return {
        "contract_name": contract_name,
        "sent_to": recipient_email or recipient_phone,
        "status": "triggered" if resp.status_code == 200 else "failed",
    }


def trigger_follow_up_sequence(contact_id: str, stage_key: str, contact_info: dict, structured_data: dict):
    """
    Trigger follow-up SMS sequence based on the pipeline stage.
    Adds a tag to the contact AND schedules automated SMS drip.
    """
    # Add follow-up tag to contact
    tag_map = {
        "follow_up": "Follow Up Needed",
        "offer_made": "Offer Made - Awaiting Response",
        "negotiating": "In Negotiation",
        "bot_called": "Initial Call Complete",
    }
    
    tag = tag_map.get(stage_key, "")
    if tag:
        tag_url = f"{GHL_BASE_URL}/contacts/{contact_id}/tags"
        tag_data = {"tags": [tag]}
        requests.post(tag_url, headers=GHL_HEADERS, json=tag_data)
        print(f"[FOLLOW-UP] Added tag '{tag}' to contact {contact_id}")
    
    # Schedule automated SMS follow-up sequence
    follow_up_data = {
        "first_name": contact_info.get("firstName", "there"),
        "last_name": contact_info.get("lastName", ""),
        "property_address": structured_data.get("property_address", "your property"),
    }
    scheduled_count = schedule_follow_ups(contact_id, stage_key, follow_up_data)
    print(f"[FOLLOW-UP] Scheduled {scheduled_count} SMS messages for {contact_id}")
    
    return tag


@app.post("/vapi/webhook")
async def vapi_webhook(request: Request):
    """
    Main webhook endpoint that receives Vapi end-of-call reports.
    Processes the call data and pushes it to GHL.
    """
    try:
        payload = await request.json()
        
        # Vapi sends different message types
        message_type = payload.get("message", {}).get("type", "")
        
        # We only care about end-of-call-report
        if message_type == "end-of-call-report":
            call_data = payload.get("message", {})
        elif "call" in payload:
            call_data = payload
        else:
            # Could be status-update, transcript, etc. - acknowledge but don't process
            return JSONResponse(content={"status": "acknowledged", "processed": False})
        
        # Extract contact info
        contact_info = extract_contact_info(call_data)
        structured_data = contact_info.pop("structured_data", {})
        
        # Build custom fields
        custom_fields = build_custom_fields(structured_data)
        
        # Add call summary as internal notes
        analysis = call_data.get("analysis", {})
        summary = analysis.get("summary", "")
        if summary:
            custom_fields.append({
                "id": CUSTOM_FIELD_MAP["internal_notes"],
                "field_value": summary
            })
        
        # Create/update contact in GHL
        contact_id = create_or_update_contact(contact_info, custom_fields)
        
        if not contact_id:
            return JSONResponse(
                content={"status": "error", "message": "Failed to create/update contact"},
                status_code=500
            )
        
        # Determine pipeline stage
        stage_key = determine_stage(call_data)
        stage_id = STAGES[stage_key]
        
        # Create opportunity
        opp_id = create_opportunity(contact_id, stage_id, structured_data)
        
        # ===== POST-CALL ACTIONS BASED ON STAGE =====
        
        contract_result = None
        disposition_results = None
        follow_up_tag = None
        
        # If deal is Under Contract → Send contract + Notify David + Blast buyers
        if stage_key == "under_contract":
            # Send the appropriate contract
            deal_type = structured_data.get("deal_type", "Cash")
            contract_result = send_contract(contact_id, deal_type, contact_info)
            
            # Trigger buyer disposition
            deal_info = {
                "seller_name": f"{contact_info.get('firstName', '')} {contact_info.get('lastName', '')}",
                "property_address": structured_data.get("property_address", "Unknown"),
                "property_city": structured_data.get("property_city", ""),
                "property_state": structured_data.get("property_state", ""),
                "offer_amount": structured_data.get("offer_amount", "N/A"),
                "arv": structured_data.get("arv", "N/A"),
                "deal_type": structured_data.get("deal_type", "Cash"),
                "bedrooms": structured_data.get("bedrooms", "N/A"),
                "bathrooms": structured_data.get("bathrooms", "N/A"),
            }
            disposition_results = process_under_contract_deal(deal_info)
        
        # If follow-up needed → Add tag + schedule SMS drip
        elif stage_key in ["follow_up", "offer_made", "negotiating", "bot_called"]:
            follow_up_tag = trigger_follow_up_sequence(contact_id, stage_key, contact_info, structured_data)
        
        return JSONResponse(content={
            "status": "success",
            "contact_id": contact_id,
            "opportunity_id": opp_id,
            "stage": stage_key,
            "fields_updated": len(custom_fields),
            "contract_sent": contract_result,
            "disposition": disposition_results,
            "follow_up_tag": follow_up_tag,
        })
    
    except Exception as e:
        print(f"[ERROR] Webhook processing failed: {str(e)}")
        return JSONResponse(
            content={"status": "error", "message": str(e)},
            status_code=500
        )


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "DealOS Vapi-GHL Webhook Handler"}


@app.post("/vapi/server-url")
async def vapi_server_url(request: Request):
    """
    Vapi Server URL endpoint - handles all Vapi server events.
    This is the URL set in Vapi's Advanced > Server URL field.
    """
    try:
        payload = await request.json()
        message = payload.get("message", {})
        msg_type = message.get("type", "")
        
        # Handle different Vapi event types
        if msg_type == "end-of-call-report":
            # Process end of call - same as webhook
            call_data = message
            contact_info = extract_contact_info(call_data)
            structured_data = contact_info.pop("structured_data", {})
            custom_fields = build_custom_fields(structured_data)
            
            analysis = call_data.get("analysis", {})
            summary = analysis.get("summary", "")
            if summary:
                custom_fields.append({
                    "id": CUSTOM_FIELD_MAP["internal_notes"],
                    "field_value": summary
                })
            
            contact_id = create_or_update_contact(contact_info, custom_fields)
            
            if contact_id:
                stage_key = determine_stage(call_data)
                stage_id = STAGES[stage_key]
                create_opportunity(contact_id, stage_id, structured_data)
                
                # Post-call actions
                if stage_key == "under_contract":
                    deal_type = structured_data.get("deal_type", "Cash")
                    send_contract(contact_id, deal_type, contact_info)
                    deal_info = {
                        "seller_name": f"{contact_info.get('firstName', '')} {contact_info.get('lastName', '')}",
                        "property_address": structured_data.get("property_address", "Unknown"),
                        "property_city": structured_data.get("property_city", ""),
                        "property_state": structured_data.get("property_state", ""),
                        "offer_amount": structured_data.get("offer_amount", "N/A"),
                        "arv": structured_data.get("arv", "N/A"),
                        "deal_type": structured_data.get("deal_type", "Cash"),
                        "bedrooms": structured_data.get("bedrooms", "N/A"),
                        "bathrooms": structured_data.get("bathrooms", "N/A"),
                    }
                    process_under_contract_deal(deal_info)
                elif stage_key in ["follow_up", "offer_made", "negotiating", "bot_called"]:
                    trigger_follow_up_sequence(contact_id, stage_key, contact_info, structured_data)
            
            return JSONResponse(content={"status": "success"})
        
        elif msg_type == "status-update":
            # Call status changed (ringing, in-progress, ended)
            return JSONResponse(content={"status": "acknowledged"})
        
        elif msg_type == "assistant-request":
            # Dynamic assistant configuration request
            return JSONResponse(content={"status": "acknowledged"})
        
        elif msg_type == "function-call":
            # Handle function calls from the assistant
            function_call = message.get("functionCall", {})
            func_name = function_call.get("name", "")
            
            if func_name == "transferCall":
                return JSONResponse(content={
                    "result": "Call transferred successfully"
                })
            
            return JSONResponse(content={"result": "Function processed"})
        
        else:
            return JSONResponse(content={"status": "acknowledged"})
    
    except Exception as e:
        print(f"[ERROR] Server URL processing failed: {str(e)}")
        return JSONResponse(
            content={"status": "error", "message": str(e)},
            status_code=500
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
