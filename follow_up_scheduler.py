"""
Follow-Up SMS Scheduler — DealOS
==================================
Handles automated SMS follow-up sequences based on pipeline stage.
Uses a background scheduler to send timed messages.

Sequences:
- "Follow Up Needed" → Day 1, Day 3, Day 7 texts
- "Offer Made" → Day 1, Day 2, Day 5 texts
- "Negotiating" → Day 1, Day 3 texts
- "Bot Called" (no answer/voicemail) → Day 1, Day 3, Day 7 texts
"""

import threading
import time
import json
import os
from datetime import datetime, timedelta
import requests

# ============ CONFIGURATION ============
GHL_API_KEY = "pit-56454803-dd76-4ad8-a567-01fe6b515da1"
GHL_LOCATION_ID = "RcG1IS88ALK6Yxsrz1yk"
GHL_BASE_URL = "https://services.leadconnectorhq.com"

GHL_HEADERS = {
    "Authorization": f"Bearer {GHL_API_KEY}",
    "Version": "2021-07-28",
    "Content-Type": "application/json",
    "Accept": "application/json",
}

# File to persist scheduled follow-ups across restarts
SCHEDULE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scheduled_followups.json")

# Follow-up message templates by stage
FOLLOW_UP_TEMPLATES = {
    "follow_up": [
        {
            "delay_hours": 24,
            "message": "Hi {{first_name}}, this is Alex from Platinia Inc. I wanted to follow up on our conversation about your property at {{property_address}}. We're still interested in making you a fair cash offer. Would you like to chat for a few minutes? Reply YES or call us at (334) 216-2338."
        },
        {
            "delay_hours": 72,
            "message": "Hey {{first_name}}, just checking in — I know selling a property is a big decision. We can close in as little as 14 days with no repairs needed on your end. If you'd like to hear our offer for {{property_address}}, just reply OFFER or give us a call at (334) 216-2338."
        },
        {
            "delay_hours": 168,
            "message": "{{first_name}}, last follow-up from Platinia Inc regarding {{property_address}}. Our offer still stands and we can work with your timeline. If you change your mind, we're here — just reply or call (334) 216-2338. No pressure either way!"
        },
    ],
    "offer_made": [
        {
            "delay_hours": 24,
            "message": "Hi {{first_name}}, this is Alex from Platinia Inc. I wanted to follow up on the offer we discussed for {{property_address}}. Have you had a chance to think it over? We're flexible and happy to answer any questions. Reply or call (334) 216-2338."
        },
        {
            "delay_hours": 48,
            "message": "Hey {{first_name}}, just a friendly reminder — our offer for {{property_address}} is still on the table. We can close fast with no hassle on your end. Let me know if you'd like to move forward or if you have any questions! (334) 216-2338"
        },
        {
            "delay_hours": 120,
            "message": "{{first_name}}, checking in one more time about {{property_address}}. If the price wasn't quite right, we may be able to adjust. Would you like to discuss? Reply YES or call (334) 216-2338."
        },
    ],
    "negotiating": [
        {
            "delay_hours": 24,
            "message": "Hi {{first_name}}, Alex from Platinia Inc here. I've been thinking about our conversation regarding {{property_address}} and I believe we can find a number that works for both of us. Would you like to continue our discussion? Reply or call (334) 216-2338."
        },
        {
            "delay_hours": 72,
            "message": "{{first_name}}, I want to make sure we can get this deal done for {{property_address}}. We're motivated buyers and can be flexible on terms. Let's talk — reply YES or call (334) 216-2338."
        },
    ],
    "bot_called": [
        {
            "delay_hours": 4,
            "message": "Hi, this is Alex from Platinia Inc. I tried calling earlier about your property — we buy houses for cash and can close quickly. If you're interested in a no-obligation offer, reply YES or call us at (334) 216-2338."
        },
        {
            "delay_hours": 72,
            "message": "Hey there, Alex from Platinia Inc again. We're still interested in making you a cash offer on your property. No repairs needed, we handle everything. Reply OFFER if you'd like to hear what we can do, or call (334) 216-2338."
        },
        {
            "delay_hours": 168,
            "message": "Last message from Platinia Inc — we buy properties as-is for cash and close in 14 days. If you ever want to explore selling, just reply or call (334) 216-2338. We're here when you're ready!"
        },
    ],
}


def load_schedule():
    """Load scheduled follow-ups from file."""
    if os.path.exists(SCHEDULE_FILE):
        try:
            with open(SCHEDULE_FILE, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return []
    return []


def save_schedule(schedule):
    """Save scheduled follow-ups to file."""
    with open(SCHEDULE_FILE, 'w') as f:
        json.dump(schedule, f, indent=2, default=str)


def send_sms(contact_id: str, message: str) -> bool:
    """Send an SMS to a contact via GHL conversations API."""
    sms_url = f"{GHL_BASE_URL}/conversations/messages"
    sms_data = {
        "type": "SMS",
        "contactId": contact_id,
        "message": message,
    }
    
    try:
        resp = requests.post(sms_url, headers=GHL_HEADERS, json=sms_data)
        if resp.status_code in [200, 201]:
            print(f"[FOLLOW-UP] SMS sent to {contact_id}: {message[:50]}...")
            return True
        else:
            print(f"[FOLLOW-UP] SMS failed for {contact_id}: {resp.status_code} - {resp.text[:100]}")
            return False
    except Exception as e:
        print(f"[FOLLOW-UP] SMS error for {contact_id}: {str(e)}")
        return False


def personalize_message(template: str, contact_data: dict) -> str:
    """Replace template variables with actual contact data."""
    message = template
    message = message.replace("{{first_name}}", contact_data.get("first_name", "there"))
    message = message.replace("{{last_name}}", contact_data.get("last_name", ""))
    message = message.replace("{{property_address}}", contact_data.get("property_address", "your property"))
    return message


def schedule_follow_ups(contact_id: str, stage_key: str, contact_data: dict):
    """
    Schedule follow-up SMS messages for a contact based on their pipeline stage.
    Messages are scheduled with delays and will be sent by the background worker.
    """
    templates = FOLLOW_UP_TEMPLATES.get(stage_key, [])
    if not templates:
        return
    
    schedule = load_schedule()
    now = datetime.utcnow()
    
    for template in templates:
        send_at = now + timedelta(hours=template["delay_hours"])
        message = personalize_message(template["message"], contact_data)
        
        entry = {
            "contact_id": contact_id,
            "message": message,
            "send_at": send_at.isoformat(),
            "stage": stage_key,
            "status": "pending",
            "created_at": now.isoformat(),
        }
        schedule.append(entry)
    
    save_schedule(schedule)
    print(f"[FOLLOW-UP] Scheduled {len(templates)} messages for contact {contact_id} (stage: {stage_key})")
    return len(templates)


def process_pending_messages():
    """Process and send any pending scheduled messages that are due."""
    schedule = load_schedule()
    now = datetime.utcnow()
    updated = False
    sent_count = 0
    
    for entry in schedule:
        if entry["status"] != "pending":
            continue
        
        send_at = datetime.fromisoformat(entry["send_at"])
        if now >= send_at:
            success = send_sms(entry["contact_id"], entry["message"])
            entry["status"] = "sent" if success else "failed"
            entry["sent_at"] = now.isoformat()
            updated = True
            if success:
                sent_count += 1
    
    if updated:
        save_schedule(schedule)
    
    return sent_count


def cancel_follow_ups(contact_id: str):
    """Cancel all pending follow-ups for a contact (e.g., when they go Under Contract)."""
    schedule = load_schedule()
    cancelled = 0
    
    for entry in schedule:
        if entry["contact_id"] == contact_id and entry["status"] == "pending":
            entry["status"] = "cancelled"
            cancelled += 1
    
    if cancelled > 0:
        save_schedule(schedule)
        print(f"[FOLLOW-UP] Cancelled {cancelled} pending messages for contact {contact_id}")
    
    return cancelled


def background_scheduler():
    """Background thread that checks for and sends pending messages every 60 seconds."""
    print("[FOLLOW-UP] Background scheduler started")
    while True:
        try:
            sent = process_pending_messages()
            if sent > 0:
                print(f"[FOLLOW-UP] Sent {sent} scheduled messages")
        except Exception as e:
            print(f"[FOLLOW-UP] Scheduler error: {str(e)}")
        time.sleep(60)  # Check every 60 seconds


def start_scheduler():
    """Start the background scheduler thread."""
    thread = threading.Thread(target=background_scheduler, daemon=True)
    thread.start()
    return thread


if __name__ == "__main__":
    # Test the scheduler
    print("Testing follow-up scheduler...")
    
    test_data = {
        "first_name": "John",
        "last_name": "Smith",
        "property_address": "123 Main St",
    }
    
    count = schedule_follow_ups("test_contact_123", "follow_up", test_data)
    print(f"Scheduled {count} follow-up messages")
    
    schedule = load_schedule()
    print(f"\nCurrent schedule ({len(schedule)} entries):")
    for entry in schedule:
        print(f"  - Send at: {entry['send_at']} | Status: {entry['status']}")
        print(f"    Message: {entry['message'][:60]}...")
