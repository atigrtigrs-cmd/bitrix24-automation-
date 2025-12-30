from flask import Flask, request, jsonify
import requests
import logging
import re

# Configure logging
logging.basicConfig(level=logging.INFO)

# Bitrix24 webhook URL
WEBHOOK_URL = "https://hr-adv.bitrix24.ru/rest/1/68owo53rxcs5276q/"

app = Flask(__name__)

# City to timezone mapping
CITY_TIMEZONES = {
    'москва': 'МСК (UTC+3)',
    'санкт-петербург': 'МСК (UTC+3)',
    'петербург': 'МСК (UTC+3)',
    'спб': 'МСК (UTC+3)',
    'екатеринбург': 'ЕКБ (UTC+5)',
    'новосибирск': 'НСК (UTC+7)',
    'красноярск': 'КРС (UTC+7)',
    'владивосток': 'ВЛД (UTC+10)',
    'иркутск': 'ИРК (UTC+8)',
    'хабаровск': 'ХБР (UTC+10)',
    'омск': 'ОМС (UTC+6)',
    'челябинск': 'ЧЛБ (UTC+5)',
    'казань': 'КЗН (UTC+3)',
    'нижний новгород': 'НН (UTC+3)',
    'самара': 'СМР (UTC+4)',
    'ростов': 'РСТ (UTC+3)',
    'уфа': 'УФА (UTC+5)',
    'пермь': 'ПРМ (UTC+5)',
    'воронеж': 'ВРН (UTC+3)',
    'волгоград': 'ВЛГ (UTC+3)',
    'краснодар': 'КРД (UTC+3)',
    'саратов': 'СРТ (UTC+3)',
    'тюмень': 'ТЮМ (UTC+5)',
    'тольятти': 'ТЛТ (UTC+4)',
    'ижевск': 'ИЖВ (UTC+4)',
    'барнаул': 'БРН (UTC+7)',
    'ульяновск': 'УЛН (UTC+4)',
    'иркутск': 'ИРК (UTC+8)',
    'ярославль': 'ЯРС (UTC+3)',
    'махачкала': 'МХЧ (UTC+3)',
    'томск': 'ТМС (UTC+7)',
    'оренбург': 'ОРБ (UTC+5)',
    'кемерово': 'КМР (UTC+7)',
    'новокузнецк': 'НКЗ (UTC+7)',
    'рязань': 'РЗН (UTC+3)',
    'астрахань': 'АСТ (UTC+4)',
    'пенза': 'ПНЗ (UTC+3)',
    'липецк': 'ЛПЦ (UTC+3)',
    'киров': 'КРВ (UTC+3)',
    'чебоксары': 'ЧБК (UTC+3)',
    'калининград': 'КЛД (UTC+2)',
    'тула': 'ТЛ (UTC+3)',
    'курск': 'КРС (UTC+3)',
    'сочи': 'СЧИ (UTC+3)',
    'ставрополь': 'СТВ (UTC+3)',
    'улан-удэ': 'УУ (UTC+8)',
    'тверь': 'ТВР (UTC+3)',
    'магнитогорск': 'МГН (UTC+5)',
    'иваново': 'ИВН (UTC+3)',
    'брянск': 'БРН (UTC+3)',
    'белгород': 'БЛГ (UTC+3)',
    'сургут': 'СРГ (UTC+5)',
    'владимир': 'ВЛД (UTC+3)',
    'нижний тагил': 'НТГ (UTC+5)',
    'архангельск': 'АРХ (UTC+3)',
    'чита': 'ЧТ (UTC+9)',
    'смоленск': 'СМЛ (UTC+3)',
    'калуга': 'КЛГ (UTC+3)',
    'волжский': 'ВЛЖ (UTC+3)',
    'курган': 'КРГ (UTC+5)',
    'орел': 'ОРЛ (UTC+3)',
    'череповец': 'ЧРП (UTC+3)',
    'владикавказ': 'ВКВ (UTC+3)',
    'мурманск': 'МРМ (UTC+3)',
    'саранск': 'СРН (UTC+3)',
    'вологда': 'ВЛГ (UTC+3)',
    'тамбов': 'ТМБ (UTC+3)',
    'стерлитамак': 'СТР (UTC+5)',
    'грозный': 'ГРЗ (UTC+3)',
    'якутск': 'ЯКТ (UTC+9)',
    'кострома': 'КСТ (UTC+3)',
    'комсомольск-на-амуре': 'КМС (UTC+10)',
    'петрозаводск': 'ПТЗ (UTC+3)',
    'таганрог': 'ТГР (UTC+3)',
    'нижневартовск': 'НВР (UTC+5)',
    'йошкар-ола': 'ЙОЛ (UTC+3)',
    'братск': 'БРТ (UTC+8)',
    'новороссийск': 'НВР (UTC+3)',
    'дзержинск': 'ДЗР (UTC+3)',
}

def normalize_phone(phone):
    """Normalize phone number to international format"""
    if not phone:
        return None
    
    # Remove all non-digit characters
    digits = re.sub(r'\D', '', phone)
    
    # Handle Russian numbers
    if digits.startswith('8') and len(digits) == 11:
        digits = '7' + digits[1:]
    elif digits.startswith('9') and len(digits) == 10:
        digits = '7' + digits
    
    return digits

def get_deal_info(deal_id):
    """Get deal information from Bitrix24"""
    try:
        response = requests.get(f"{WEBHOOK_URL}crm.deal.get", params={"ID": deal_id})
        if response.status_code == 200:
            data = response.json()
            return data.get('result', {})
    except Exception as e:
        logging.error(f"Error getting deal info: {e}")
    return None

def get_contact_info(contact_id):
    """Get contact information from Bitrix24"""
    try:
        response = requests.get(f"{WEBHOOK_URL}crm.contact.get", params={"ID": contact_id})
        if response.status_code == 200:
            data = response.json()
            return data.get('result', {})
    except Exception as e:
        logging.error(f"Error getting contact info: {e}")
    return None

def update_contact(contact_id, fields):
    """Update contact in Bitrix24"""
    try:
        response = requests.post(
            f"{WEBHOOK_URL}crm.contact.update",
            json={"ID": contact_id, "fields": fields}
        )
        return response.status_code == 200
    except Exception as e:
        logging.error(f"Error updating contact: {e}")
        return False

def update_deal(deal_id, fields):
    """Update deal in Bitrix24"""
    try:
        response = requests.post(
            f"{WEBHOOK_URL}crm.deal.update",
            json={"ID": deal_id, "fields": fields}
        )
        return response.status_code == 200
    except Exception as e:
        logging.error(f"Error updating deal: {e}")
        return False

def get_timezone_from_city(city):
    """Get timezone from city name"""
    if not city:
        return None
    
    city_lower = city.lower().strip()
    return CITY_TIMEZONES.get(city_lower)

@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({"status": "ok"})

@app.route('/', methods=['POST', 'GET'])
@app.route('/webhook', methods=['POST', 'GET'])
def webhook():
    """Handle Bitrix24 webhook for deal creation/update"""
    try:
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
        
        # Process contact data
        contact_updates = {}
        
        # Get phone number
        phones = contact.get('PHONE', [])
        phone = None
        if phones and isinstance(phones, list) and len(phones) > 0:
            phone = phones[0].get('VALUE', '')
        
        # Generate messenger links if phone exists
        if phone:
            normalized_phone = normalize_phone(phone)
            if normalized_phone:
                # WhatsApp link
                contact_updates['UF_CRM_WHATSAPP_LINK'] = f"https://wa.me/{normalized_phone}"
                
                # Telegram link
                contact_updates['UF_CRM_TELEGRAM_LINK'] = f"https://t.me/+{normalized_phone}"
                
                logging.info(f"Generated links for contact {contact_id} with phone {normalized_phone}")
        
        # Update contact if there are changes
        if contact_updates:
            update_contact(contact_id, contact_updates)
        
        # Process deal data
        deal_updates = {}
        
        # Get city from deal - try both field IDs
        city = deal.get('UF_CRM_CITY') or deal.get('UF_CRM_694F054732342')
        
        # Set timezone based on city
        if city:
            timezone = get_timezone_from_city(city)
            if timezone:
                deal_updates['UF_CRM_TIMEZONE'] = timezone
                logging.info(f"Set timezone {timezone} for city {city}")
        
        # Generate phone links for deal
        if phone and normalized_phone:
            # Old text fields (keep for backward compatibility)
            deal_updates['UF_CRM_CALL_LINK'] = f"tel:+{normalized_phone}"
            deal_updates['UF_CRM_1767001460714'] = f"https://wa.me/{normalized_phone}"  # Ссылка на вацап
            deal_updates['UF_CRM_1767001473947'] = f"https://t.me/+{normalized_phone}"  # ссылка на тг
            
            # New clickable URL fields
            deal_updates['UF_CRM_WHATSAPP_URL'] = f"https://wa.me/{normalized_phone}"  # WhatsApp (кликабельная)
            deal_updates['UF_CRM_TELEGRAM_URL'] = f"https://t.me/+{normalized_phone}"  # Telegram (кликабельная)
        
        # Update deal title: "Name - Job Title"
        contact_name = contact.get('NAME', '') + ' ' + contact.get('LAST_NAME', '')
        contact_name = contact_name.strip()
        
        job_title = deal.get('TITLE', '')
        
        if contact_name and job_title:
            new_title = f"{contact_name} - {job_title}"
            deal_updates['TITLE'] = new_title
            logging.info(f"Updated deal title to: {new_title}")
        
        # Update deal if there are changes
        if deal_updates:
            update_deal(deal_id, deal_updates)
        
        return jsonify({"status": "success", "deal_id": deal_id})
    
    except Exception as e:
        logging.error(f"Error processing webhook: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
