"""
Buyer Disposition Backend — DealOS
====================================
This module handles the buyer-side of the wholesale business:
1. When a deal goes "Under Contract", it blasts matching buyers
2. Matches buyers by tag (Cash Buyer, Creative Finance Buyer, Subject-To Buyer)
3. Sends SMS notifications to matching buyers via GHL
4. Sends notification to David Bishop about new deals
"""

import requests
import json

# ============ CONFIGURATION ============
GHL_API_KEY = "pit-56454803-dd76-4ad8-a567-01fe6b515da1"
GHL_LOCATION_ID = "RcG1IS88ALK6Yxsrz1yk"
GHL_BASE_URL = "https://services.leadconnectorhq.com"
DAVID_PHONE = "+13344876569"

GHL_HEADERS = {
    "Authorization": f"Bearer {GHL_API_KEY}",
    "Version": "2021-07-28",
    "Content-Type": "application/json",
    "Accept": "application/json",
}

# Tag IDs for buyer matching
BUYER_TAGS = {
    "cash": "vguU3apI4frsux00wTvD",
    "creative_finance": "Ai1Ed7K6dVgft2j2r4ch",
    "subject_to": "PkzKAK4sibMug1XJqBxT",
}

# Deal type to buyer tag mapping
DEAL_TYPE_TO_TAG = {
    "cash": ["cash"],
    "creative finance": ["creative_finance"],
    "seller finance": ["creative_finance"],
    "sub-to": ["subject_to", "creative_finance"],
    "subject to": ["subject_to", "creative_finance"],
    "subject-to": ["subject_to", "creative_finance"],
    "hybrid": ["creative_finance", "cash"],
}


def get_buyers_by_tag(tag_name: str) -> list:
    """Get all contacts with a specific buyer tag."""
    # Search contacts by tag
    search_url = f"{GHL_BASE_URL}/contacts/"
    params = {
        "locationId": GHL_LOCATION_ID,
        "query": tag_name,
        "limit": 100,
    }
    
    resp = requests.get(search_url, headers=GHL_HEADERS, params=params)
    if resp.status_code == 200:
        data = resp.json()
        contacts = data.get("contacts", [])
        # Filter to only those with the buyer tag
        buyers = []
        for contact in contacts:
            tags = [t.lower() for t in contact.get("tags", [])]
            if tag_name.lower().replace("_", " ") in " ".join(tags):
                buyers.append(contact)
        return buyers
    return []


def notify_david(deal_info: dict):
    """Send David a notification SMS about a new deal under contract."""
    seller_name = deal_info.get("seller_name", "Unknown Seller")
    property_address = deal_info.get("property_address", "Unknown Address")
    offer_amount = deal_info.get("offer_amount", "N/A")
    deal_type = deal_info.get("deal_type", "Cash")
    arv = deal_info.get("arv", "N/A")
    
    message = (
        f"🏠 NEW DEAL UNDER CONTRACT!\n\n"
        f"Seller: {seller_name}\n"
        f"Property: {property_address}\n"
        f"Offer: ${offer_amount}\n"
        f"ARV: ${arv}\n"
        f"Type: {deal_type}\n\n"
        f"Contract has been sent for signature.\n"
        f"Check GHL for full details."
    )
    
    # Find David's contact or create one
    search_url = f"{GHL_BASE_URL}/contacts/search/duplicate"
    params = {"locationId": GHL_LOCATION_ID, "number": DAVID_PHONE}
    resp = requests.get(search_url, headers=GHL_HEADERS, params=params)
    
    david_contact_id = None
    if resp.status_code == 200:
        contact = resp.json().get("contact")
        if contact:
            david_contact_id = contact["id"]
    
    if not david_contact_id:
        # Create David's contact
        create_data = {
            "locationId": GHL_LOCATION_ID,
            "firstName": "David",
            "lastName": "Bishop",
            "phone": DAVID_PHONE,
            "tags": ["owner"],
        }
        resp = requests.post(f"{GHL_BASE_URL}/contacts/", headers=GHL_HEADERS, json=create_data)
        if resp.status_code in [200, 201]:
            david_contact_id = resp.json().get("contact", {}).get("id")
    
    if david_contact_id:
        # Send SMS via GHL conversations API
        sms_url = f"{GHL_BASE_URL}/conversations/messages"
        sms_data = {
            "type": "SMS",
            "contactId": david_contact_id,
            "message": message,
        }
        requests.post(sms_url, headers=GHL_HEADERS, json=sms_data)
    
    return david_contact_id


def blast_buyers(deal_info: dict):
    """
    Send deal details to matching buyers based on deal type.
    Returns the number of buyers notified.
    """
    deal_type = deal_info.get("deal_type", "cash").lower()
    property_address = deal_info.get("property_address", "Unknown")
    offer_amount = deal_info.get("offer_amount", "N/A")
    arv = deal_info.get("arv", "N/A")
    bedrooms = deal_info.get("bedrooms", "N/A")
    bathrooms = deal_info.get("bathrooms", "N/A")
    city = deal_info.get("property_city", "")
    state = deal_info.get("property_state", "")
    
    # Determine which buyer tags to target
    target_tags = DEAL_TYPE_TO_TAG.get(deal_type, ["cash"])
    
    # Build the buyer blast message
    message = (
        f"🔥 NEW DEAL ALERT — Platinia Inc\n\n"
        f"📍 {property_address}, {city}, {state}\n"
        f"🏠 {bedrooms} bed / {bathrooms} bath\n"
        f"💰 Contract Price: ${offer_amount}\n"
        f"📈 ARV: ${arv}\n"
        f"📋 Type: {deal_type.title()}\n\n"
        f"Reply YES if interested or call (334) 216-2338"
    )
    
    notified_count = 0
    notified_ids = set()
    
    for tag_key in target_tags:
        tag_name = tag_key.replace("_", " ")
        buyers = get_buyers_by_tag(tag_name)
        
        for buyer in buyers:
            buyer_id = buyer.get("id")
            if buyer_id and buyer_id not in notified_ids:
                # Send SMS to buyer
                sms_url = f"{GHL_BASE_URL}/conversations/messages"
                sms_data = {
                    "type": "SMS",
                    "contactId": buyer_id,
                    "message": message,
                }
                resp = requests.post(sms_url, headers=GHL_HEADERS, json=sms_data)
                if resp.status_code in [200, 201]:
                    notified_count += 1
                notified_ids.add(buyer_id)
    
    return notified_count


def process_under_contract_deal(deal_info: dict) -> dict:
    """
    Main function called when a deal moves to 'Under Contract'.
    Handles:
    1. Notifying David
    2. Blasting matching buyers
    """
    results = {
        "david_notified": False,
        "buyers_notified": 0,
    }
    
    # Notify David
    david_id = notify_david(deal_info)
    if david_id:
        results["david_notified"] = True
    
    # Blast buyers
    buyer_count = blast_buyers(deal_info)
    results["buyers_notified"] = buyer_count
    
    return results


if __name__ == "__main__":
    # Test with sample deal
    test_deal = {
        "seller_name": "John Smith",
        "property_address": "456 Oak Avenue",
        "property_city": "Birmingham",
        "property_state": "Alabama",
        "offer_amount": "95000",
        "arv": "160000",
        "deal_type": "Cash",
        "bedrooms": "4",
        "bathrooms": "2",
    }
    
    print("Testing buyer disposition...")
    results = process_under_contract_deal(test_deal)
    print(f"Results: {json.dumps(results, indent=2)}")
