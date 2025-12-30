from flask import Flask, request, jsonify
import requests
import re
import logging

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

WEBHOOK_URL = "https://hr-adv.bitrix24.ru/rest/1/k108sy1elhe6hihc/"

# Timezone mapping for major Russian cities
CITY_TIMEZONE_MAP = {
    'москва': 'МСК (UTC+3)',
    'санкт-петербург': 'МСК (UTC+3)',
    'петербург': 'МСК (UTC+3)',
    'спб': 'МСК (UTC+3)',
    'екатеринбург': 'ЕКБ (UTC+5)',
    'новосибирск': 'НСК (UTC+7)',
    'красноярск': 'КРС (UTC+7)',
    'иркутск': 'ИРК (UTC+8)',
    'владивосток': 'ВЛД (UTC+10)',
    'хабаровск': 'ХБР (UTC+10)',
    'казань': 'МСК (UTC+3)',
    'нижний новгород': 'МСК (UTC+3)',
    'челябинск': 'ЧЛБ (UTC+5)',
    'самара': 'СМР (UTC+4)',
    'омск': 'ОМС (UTC+6)',
    'ростов-на-дону': 'МСК (UTC+3)',
    'уфа': 'УФА (UTC+5)',
    'пермь': 'ПРМ (UTC+5)',
    'воронеж': 'МСК (UTC+3)',
    'волгоград': 'ВЛГ (UTC+3)',
    'краснодар': 'МСК (UTC+3)',
    'саратов': 'СРТ (UTC+3)',
    'тюмень': 'ТЮМ (UTC+5)',
    'тольятти': 'СМР (UTC+4)',
    'ижевск': 'ИЖ (UTC+4)',
    'барнаул': 'БРН (UTC+7)',
    'ульяновск': 'УЛЬ (UTC+4)',
    'иркутск': 'ИРК (UTC+8)',
    'ярославль': 'МСК (UTC+3)',
    'владимир': 'МСК (UTC+3)',
    'махачкала': 'МСК (UTC+3)',
    'томск': 'ТМС (UTC+7)',
    'оренбург': 'ОРН (UTC+5)',
    'кемерово': 'КМР (UTC+7)',
    'новокузнецк': 'НКЗ (UTC+7)',
    'рязань': 'МСК (UTC+3)',
    'астрахань': 'АСТ (UTC+4)',
    'пенза': 'МСК (UTC+3)',
    'липецк': 'МСК (UTC+3)',
    'киров': 'МСК (UTC+3)',
    'чебоксары': 'МСК (UTC+3)',
    'калининград': 'КЛН (UTC+2)',
    'брянск': 'МСК (UTC+3)',
    'курск': 'МСК (UTC+3)',
    'иваново': 'МСК (UTC+3)',
    'магнитогорск': 'МГН (UTC+5)',
    'тверь': 'МСК (UTC+3)',
    'ставрополь': 'МСК (UTC+3)',
    'нижний тагил': 'ЕКБ (UTC+5)',
    'белгород': 'МСК (UTC+3)',
    'архангельск': 'МСК (UTC+3)',
    'владикавказ': 'МСК (UTC+3)',
    'калуга': 'МСК (UTC+3)',
    'сочи': 'МСК (UTC+3)',
    'смоленск': 'МСК (UTC+3)',
    'волжский': 'ВЛГ (UTC+3)',
    'курган': 'КРГ (UTC+5)',
    'орёл': 'МСК (UTC+3)',
    'череповец': 'МСК (UTC+3)',
    'вологда': 'МСК (UTC+3)',
    'мурманск': 'МСК (UTC+3)',
    'сургут': 'СРГ (UTC+5)',
    'тамбов': 'МСК (UTC+3)',
    'петрозаводск': 'МСК (UTC+3)',
    'кострома': 'МСК (UTC+3)',
    'нижневартовск': 'НВТ (UTC+5)',
}

def clean_phone(phone):
    """Remove all non-digit characters from phone number"""
    if not phone:
        return None
    cleaned = re.sub(r'[^\d]', '', str(phone))
    # Remove leading 8 or 7
    if cleaned.startswith('8'):
        cleaned = '7' + cleaned[1:]
    elif not cleaned.startswith('7'):
        cleaned = '7' + cleaned
    return cleaned

def get_timezone_by_city(city):
    """Get timezone by city name"""
    if not city:
        return None
    
    city_lower = city.lower().strip()
    return CITY_TIMEZONE_MAP.get(city_lower)

def get_contact_info(contact_id):
    """Get contact information by ID"""
    try:
        response = requests.post(
            f"{WEBHOOK_URL}crm.contact.get",
            json={"ID": contact_id}
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get("result"):
                return data["result"]
    except Exception as e:
        logging.error(f"Error getting contact info: {e}")
    
    return None

def get_deal_info(deal_id):
    """Get deal information by ID"""
    try:
        response = requests.post(
            f"{WEBHOOK_URL}crm.deal.get",
            json={"ID": deal_id}
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get("result"):
                return data["result"]
    except Exception as e:
        logging.error(f"Error getting deal info: {e}")
    
    return None

def update_contact(contact_id, fields):
    """Update contact fields"""
    try:
        response = requests.post(
            f"{WEBHOOK_URL}crm.contact.update",
            json={
                "ID": contact_id,
                "fields": fields
            }
        )
        
        if response.status_code == 200:
            data = response.json()
            return data.get("result", False)
    except Exception as e:
        logging.error(f"Error updating contact: {e}")
    
    return False

def update_deal(deal_id, fields):
    """Update deal fields"""
    try:
        response = requests.post(
            f"{WEBHOOK_URL}crm.deal.update",
            json={
                "ID": deal_id,
                "fields": fields
            }
        )
        
        if response.status_code == 200:
            data = response.json()
            return data.get("result", False)
    except Exception as e:
        logging.error(f"Error updating deal: {e}")
    
    return False

@app.route('/', methods=['POST', 'GET'])
@app.route('/webhook', methods=['POST', 'GET'])
def webhook():

    """Handle Bitrix24 webhook for deal creation/update"""
            # Get deal ID from request
        data = request.json if request.is_json else request.form.to_dict()
        
        # Try different formats Bitrix24 might send
        deal_id = None
        
        # Format 1: document_id[2] = "DEAL_123"
        if 'document_id[2]' in data:
            doc_id = data.get('document_id[2]', '')
            if doc_id.startswith('DEAL_'):
                deal_id = doc_id.replace('DEAL_', '')
        
        # Format 2: Standard webhook formats
        if not deal_id:
            deal_id = data.get('FIELDS[ID]') or data.get('data[FIELDS][ID]') or data.get('deal_id')
        
        if not deal_id:
            logging.warning(f"No deal ID in request: {data}")
            return jsonify({"status": "error", "message": "No deal ID provided"}), 400
        
        logging.info(f"Processing deal {deal_id}")
        
        # Get deal information
        deal = get_deal_info(deal_id)
        if not deal:
            return jsonify({"status": "error", "message": "Deal not found"}), 404
        
        contact_id = deal.get('CONTACT_ID')
        if not contact_id:
            return jsonify({"status": "error", "message": "No contact linked to deal"}), 400
        
        # Get contact information
        contact = get_contact_info(contact_id)
        if not contact:
            return jsonify({"status": "error", "message": "Contact not found"}), 404
        
        # Extract phone number
        phone = None
        if contact.get('PHONE'):
            phones = contact['PHONE']
            if isinstance(phones, list) and len(phones) > 0:
                phone = phones[0].get('VALUE')
            elif isinstance(phones, dict):
                phone = phones.get('VALUE')
        
        # Clean phone number
        cleaned_phone = clean_phone(phone)
        
        # Prepare updates for contact
        contact_updates = {}
        
        if cleaned_phone:
            # Generate messenger links
            whatsapp_link = f"https://wa.me/{cleaned_phone}"
            telegram_link = f"https://t.me/+{cleaned_phone}"
            
            contact_updates['UF_CRM_1734865844'] = whatsapp_link  # WhatsApp Link field
            contact_updates['UF_CRM_1734865855'] = telegram_link  # Telegram Link field
            
            logging.info(f"Generated links for contact {contact_id}: WA={whatsapp_link}, TG={telegram_link}")
        
        # Update contact if there are changes
        if contact_updates:
            update_contact(contact_id, contact_updates)
        
        # Prepare updates for deal
        deal_updates = {}
        
        # Copy city from contact to deal (if exists)
        city = contact.get('ADDRESS_CITY')
        if city:
            deal_updates['UF_CRM_1159'] = city  # City field in deal
            logging.info(f"Copied city to deal: {city}")
            
            # Determine timezone
            timezone = get_timezone_by_city(city)
            if timezone:
                deal_updates['UF_CRM_1161'] = timezone  # Timezone field
                logging.info(f"Set timezone: {timezone}")
        
        # Create call link
        if cleaned_phone:
            call_link = f"tel:+{cleaned_phone}"
            deal_updates['UF_CRM_1163'] = call_link
            logging.info(f"Created call link: {call_link}")
        
        # Create deal title: "Name - Vacancy"
        contact_name = contact.get('NAME', 'Кандидат')
        vacancy = deal.get('TITLE', '')
        
        # Only update title if it's not already in the correct format
        if ' - ' not in vacancy:
            new_title = f"{contact_name} - {vacancy}"
            deal_updates['TITLE'] = new_title
            logging.info(f"Updated deal title: {new_title}")
        
        # Update deal if there are changes
        if deal_updates:
            update_deal(deal_id, deal_updates)
        
        return jsonify({
            "status": "success",
            "deal_id": deal_id,
            "contact_id": contact_id,
            "updates": {
                "contact": contact_updates,
                "deal": deal_updates
            }
        })
    
    except Exception as e:
        logging.error(f"Error processing webhook: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({"status": "ok"})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
