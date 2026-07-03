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

# Contract names in GHL (must match exactly what's in Documents & Contracts )
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
    analysis = call_data.get("analysis") or {}
    structured_data = analysis.get("structuredData") or {}
    summary = analysis.get("summary") or ""
    summary = summary.lower()
    
    # Extract variables needed for stage determination
    deal_type = structured_data.get("deal_type", "").lower()
    offer_amount = structured_data.get("offer_amount", "")
    
    # Temporarily force 'under_contract' for testing if call ended by user
    ended_reason = call_data.get("endedReason", "")
    if ended_reason == "call_ended_by_user":
        return "under_contract"

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

def extract_email_from_text(text: str) -> str:
    """Extract and clean email address from raw text/transcripts."""
    if not text:
        return ""
    
    # If the text is already just a clean email (with no spaces/extra words)
    # but maybe has some surrounding spaces, clean and return it.
    cleaned_input = text.strip()
    if "@" in cleaned_input and " " not in cleaned_input:
        match = re.search(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', cleaned_input)
        if match:
            return match.group(0).lower()

    # Convert common spoken/written separators to standard email chars
    # Require word boundaries for "at" and "dot" to avoid matching inside words like "Platinia" or "contract"
    temp = re.sub(r'\b[\(\[]?\s*at\s*[\)\]]?\b', ' @ ', text, flags=re.IGNORECASE)
    temp = re.sub(r'\s*@\s*', ' @ ', temp)
    temp = re.sub(r'\b[\(\[]?\s*dot\s*[\)\]]?\b', '.', temp, flags=re.IGNORECASE)
    temp = re.sub(r'\s*\.\s*', '.', temp)
    
    # Split text into words
    words = temp.split()
    
    # Find the index of the word containing "@"
    at_index = -1
    for i, w in enumerate(words):
        if "@" in w:
            at_index = i
            break
            
    if at_index == -1:
        return ""
        
    # Split the word containing "@" into local part and domain part
    at_word = words[at_index]
    local_part_start, domain_part_start = at_word.split("@", 1)
    
    # We will build the local part (left of @)
    local_words = []
    if local_part_start:
        local_words.append(local_part_start)
        
    # Stop-words that indicate the email address has ended/not started yet
    stop_words = {
        "is", "are", "was", "were", "am", "be", "been", "do", "does", "did", "have", "has", "had",
        "my", "your", "his", "her", "its", "our", "their", "mine", "yours", "hers", "ours", "theirs",
        "the", "a", "an", "this", "that", "these", "those", "here", "there",
        "i", "you", "he", "she", "it", "we", "they", "me", "him", "her", "us", "them",
        "to", "for", "of", "in", "on", "at", "by", "from", "with", "about", "against", "between", "into", "through", "during",
        "and", "but", "or", "so", "because", "as", "if", "when", "while",
        "email", "address", "phone", "number", "contract", "agreement", "sent", "send", "receive", "received", "got", "get", "gotten",
        "please", "now", "just", "confirm", "confirming", "confirmation", "say", "saying", "said", "tell", "telling", "told"
    }
    
    # Go backwards from the @ word to collect email words
    for i in range(at_index - 1, -1, -1):
        word = words[i].lower().strip(".,?!:;()")
        if not word or word in stop_words or any(char in word for char in "@,?!:;()"):
            break
        local_words.insert(0, words[i])
        
    # We will build the domain part (right of @)
    domain_words = []
    if domain_part_start:
        domain_words.append(domain_part_start)
        
    # Go forwards from the @ word to collect domain words
    for i in range(at_index + 1, len(words)):
        word = words[i].lower().strip(".,?!:;()")
        if not word or word in stop_words or not re.match(r'^[a-z0-9.-]+$', word):
            break
        domain_words.append(words[i])
        
    # Join parts and remove all internal whitespace/punctuation at the edges
    local_str = "".join(local_words).strip(".,?!:;()-_+ ")
    domain_str = "".join(domain_words).strip(".,?!:;()-_+ ")
    
    # Build complete email candidate
    email_clean = f"{local_str}@{domain_str}"
    
    # Validate final format
    final_match = re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email_clean)
    if final_match:
        return email_clean.lower()
        
    return ""

def extract_contact_info(call_data: dict) -> dict:
    """Extract contact information from Vapi call data."""
    # Get the customer phone from the call
    customer = call_data.get("customer") or {}
    phone = customer.get("number") or ""
    
    # Get structured data from analysis
    analysis = call_data.get("analysis") or {}
    structured_data = analysis.get("structuredData") or {}
    
    # 1. Try customer object
    email = customer.get("email") or ""
    if email:
        email = extract_email_from_text(email)
    
    # 2. Try structured data fields
    if not email:
        raw_email = (
            structured_data.get("seller_email") or
            structured_data.get("email") or
            structured_data.get("email_address") or
            structured_data.get("customer_email") or
            ""
        )
        if raw_email:
            email = extract_email_from_text(raw_email)
    
    # 3. Search raw transcript and summary
    if not email:
        transcript = call_data.get("transcript") or ""
        email = extract_email_from_text(transcript)
        
    if not email:
        summary = analysis.get("summary") or ""
        email = extract_email_from_text(summary)
    
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
        "email": email,
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
            "number": phone
        }
        search_resp = requests.get(search_url, headers=GHL_HEADERS, params=search_params)
        if search_resp.status_code == 200:
            search_data = search_resp.json()
            contact = search_data.get("contact")
            if contact:
                # Update existing contact
                contact_id = contact["id"]
                existing_email = contact.get("email")
                new_email = contact_info.get("email")
                
                # Preserve existing email if new email is empty/missing
                email_to_update = new_email if new_email else existing_email
                
                update_url = f"{GHL_BASE_URL}/contacts/{contact_id}"
                update_data = {
                    "firstName": contact_info.get("firstName"),
                    "lastName": contact_info.get("lastName"),
                    "email": email_to_update or None,
                    "customFields": custom_fields,
                }
                update_resp = requests.put(update_url, headers=GHL_HEADERS, json=update_data)
                if update_resp.status_code not in [200, 201]:
                    print(f"[GHL ERROR] Failed to update contact {contact_id}. Status: {update_resp.status_code}, Response: {update_resp.text}")
                return contact_id
            else:
                print(f"[GHL INFO] No duplicate contact found for number: {phone}")
        else:
            print(f"[GHL ERROR] Duplicate search failed. Status: {search_resp.status_code}, Response: {search_resp.text}")
    
    # Create new contact
    create_url = f"{GHL_BASE_URL}/contacts/"
    create_data = {
        "locationId": GHL_LOCATION_ID,
        "firstName": contact_info.get("firstName", "Unknown"),
        "lastName": contact_info.get("lastName", "Seller"),
        "email": contact_info.get("email") or None,  # GHL prefers None or omitted over empty string
        "phone": phone or None,                      # GHL prefers None or omitted over empty string
        "customFields": custom_fields,
    }
    
    resp = requests.post(create_url, headers=GHL_HEADERS, json=create_data)
    if resp.status_code in [200, 201]:
        contact = resp.json().get("contact")
        if contact:
            return contact.get("id", "")
    else:
        print(f"[GHL ERROR] Failed to create contact. Status: {resp.status_code}, Response: {resp.text}")
    
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
        opportunity = resp.json().get("opportunity")
        if opportunity:
            return opportunity.get("id", "")
    else:
        print(f"[GHL ERROR] Failed to create opportunity. Status: {resp.status_code}, Response: {resp.text}")
    
    return ""

def send_contract(contact_id: str, deal_type: str, contact_info: dict):
    """
    Send the appropriate contract to the seller based on deal type.
    Adds a tag to the contact to trigger the GHL workflow for contract sending.
    """
    # Determine which contract to send
    deal_type_lower = deal_type.lower() if deal_type else "cash"
    contract_name = CONTRACT_NAMES.get(deal_type_lower, CONTRACT_NAMES["cash"])
    
    # Add a tag to the contact to trigger the GHL workflow for contract sending
    tag_url = f"{GHL_BASE_URL}/contacts/{contact_id}/tags"
    tag_data = {"tags": [CONTRACT_READY_TAG]}
    resp = requests.post(tag_url, headers=GHL_HEADERS, json=tag_data)
    
    # Log the contract send attempt
    print(f"[CONTRACT] Added tag '{CONTRACT_READY_TAG}' to contact {contact_id}")
    
    return {
        "contract_name": contract_name,
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
        analysis = call_data.get("analysis") or {}
        summary = analysis.get("summary") or ""
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
        stage_id = STAGES.get(stage_key, STAGES["bot_called"])
        
        # Create opportunity
        opportunity_id = create_opportunity(contact_id, stage_id, structured_data)
        
        # Post-call actions based on stage
        actions_taken = []
        
        if stage_key == "under_contract":
            # 1. Send contract
            deal_type = structured_data.get("deal_type", "Cash")
            contract_status = send_contract(contact_id, deal_type, contact_info)
            actions_taken.append(f"contract_sent_{deal_type}")
            
            # 2. Trigger buyer blast (if applicable)
            deal_info = {
                "seller_name": f"{contact_info.get('firstName', '')} {contact_info.get('lastName', '')}".strip() or "Unknown Seller",
                "property_address": structured_data.get("property_address", "Unknown Address"),
                "property_city": structured_data.get("property_city", ""),
                "property_state": structured_data.get("property_state", ""),
                "offer_amount": structured_data.get("offer_amount", "N/A"),
                "arv": structured_data.get("arv", "N/A"),
                "deal_type": deal_type,
                "bedrooms": structured_data.get("bedrooms", "N/A"),
                "bathrooms": structured_data.get("bathrooms", "N/A"),
            }
            process_under_contract_deal(deal_info)
            actions_taken.append("buyer_blast_triggered")
            
            # 3. Cancel any pending follow-ups
            cancel_follow_ups(contact_id)
        else:
            # Trigger follow-up sequence for other stages
            tag_added = trigger_follow_up_sequence(contact_id, stage_key, contact_info, structured_data)
            actions_taken.append(f"follow_up_triggered_{stage_key}")
        
        return JSONResponse(content={
            "status": "success",
            "contact_id": contact_id,
            "opportunity_id": opportunity_id,
            "stage": stage_key,
            "actions": actions_taken
        })
    except Exception as e:
        print(f"Error processing webhook: {str(e)}")
        return JSONResponse(
            content={"status": "error", "message": str(e)},
            status_code=500
        )

def handle_send_contract_tool(arguments: dict, call_data: dict) -> str:
    """
    Handle the send_contract tool call from Vapi during the call.
    Creates/updates the contact with the provided email, and triggers the GHL contract workflow.
    """
    email = arguments.get("email") or arguments.get("seller_email") or ""
    if email:
        email = extract_email_from_text(email)
        
    deal_type = arguments.get("deal_type") or arguments.get("dealType") or "Cash"
    first_name = arguments.get("first_name") or arguments.get("firstName") or ""
    last_name = arguments.get("last_name") or arguments.get("lastName") or ""
    property_address = arguments.get("property_address") or arguments.get("propertyAddress") or ""
    
    # Get phone from call data
    customer = call_data.get("customer") or {}
    phone = customer.get("number") or ""
    
    # If email wasn't provided or was malformed, search transcript
    if not email:
        transcript = call_data.get("transcript") or ""
        email = extract_email_from_text(transcript)
        
    if not email:
        # Check summary/analysis if available
        analysis = call_data.get("analysis") or {}
        summary = analysis.get("summary") or ""
        email = extract_email_from_text(summary)
        
    if not email:
        return "Error: Could not extract a valid email address. Please ask the seller for their email address."
        
    # Get structured data from call if available
    analysis = call_data.get("analysis") or {}
    structured_data = analysis.get("structuredData") or {}
    
    # Merge/override arguments into structured data
    if property_address:
        structured_data["property_address"] = property_address
    if deal_type:
        structured_data["deal_type"] = deal_type
        
    # Build contact info
    if not first_name:
        first_name = structured_data.get("seller_first_name") or structured_data.get("first_name") or "Unknown"
    if not last_name:
        last_name = structured_data.get("seller_last_name") or structured_data.get("last_name") or "Seller"
        
    contact_info = {
        "phone": phone,
        "firstName": first_name,
        "lastName": last_name,
        "email": email,
    }
    
    # Build custom fields
    custom_fields = build_custom_fields(structured_data)
    
    # Add summary if available
    summary = analysis.get("summary") or ""
    if summary:
        custom_fields.append({
            "id": CUSTOM_FIELD_MAP["internal_notes"],
            "field_value": summary
        })
        
    # Create or update contact in GHL
    contact_id = create_or_update_contact(contact_info, custom_fields)
    if not contact_id:
        return "Error: Failed to create or update contact in GoHighLevel."
        
    # Create opportunity in 'under_contract' stage since we are sending contract
    stage_id = STAGES.get("under_contract", STAGES["bot_called"])
    create_opportunity(contact_id, stage_id, structured_data)
    
    # Trigger contract send via GHL tag
    send_contract(contact_id, deal_type, contact_info)
    
    # Trigger buyer blast
    deal_info = {
        "seller_name": f"{first_name} {last_name}".strip() or "Unknown Seller",
        "property_address": structured_data.get("property_address", "Unknown Address"),
        "property_city": structured_data.get("property_city", ""),
        "property_state": structured_data.get("property_state", ""),
        "offer_amount": structured_data.get("offer_amount", "N/A"),
        "arv": structured_data.get("arv", "N/A"),
        "deal_type": deal_type,
        "bedrooms": structured_data.get("bedrooms", "N/A"),
        "bathrooms": structured_data.get("bathrooms", "N/A"),
    }
    process_under_contract_deal(deal_info)
    
    # Cancel follow-ups
    cancel_follow_ups(contact_id)
    
    return f"Success: Contact updated/created with email {email}. Contract '{deal_type}' has been sent to the seller via text and email."

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "DealOS Webhook"}

@app.post("/vapi/server-url")
async def vapi_server_url(request: Request):
    """
    This endpoint is called by Vapi during the call to get dynamic instructions
    or to handle specific tool calls.
    """
    try:
        payload = await request.json()
        message = payload.get("message", {})
        
        # If Vapi is asking for dynamic variables
        if message.get("type") == "conversation-update":
            return JSONResponse(content={})
        
        # If Vapi is calling a tool
        if message.get("type") == "tool-calls":
            tool_calls = message.get("toolCalls", [])
            results = []
            for tool_call in tool_calls:
                function_info = tool_call.get("function", {})
                tool_name = function_info.get("name")
                arguments = function_info.get("arguments", {})
                
                # Arguments might be passed as a JSON string
                if isinstance(arguments, str):
                    try:
                        arguments = json.loads(arguments)
                    except Exception:
                        arguments = {}
                
                if tool_name in ["send_contract", "sendContract", "send_contract_to_seller", "sendContractToSeller"]:
                    # Get call context from payload
                    call_data = payload.get("message", {}).get("call") or payload.get("call") or {}
                    # We might have analysis/transcript in the message
                    if "transcript" not in call_data and "transcript" in payload.get("message", {}):
                        call_data["transcript"] = payload["message"]["transcript"]
                    if "analysis" not in call_data and "analysis" in payload.get("message", {}):
                        call_data["analysis"] = payload["message"]["analysis"]
                        
                    res_msg = handle_send_contract_tool(arguments, call_data)
                    results.append({
                        "toolCallId": tool_call.get("id"),
                        "result": res_msg
                    })
                else:
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

