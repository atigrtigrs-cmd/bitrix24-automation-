from flask import Flask, request, jsonify
import requests
import logging
import re
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO)

# Bitrix24 webhook URL
WEBHOOK_URL = "https://hr-adv.bitrix24.ru/rest/1/rk34vfgy3owygm3k/"

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
    'старый оскол': 'МСК (UTC+3)',
    'белгородская область': 'МСК (UTC+3)',
}

# English to Russian city name translations
CITY_TRANSLATIONS = {
    'stary oskol': 'старый оскол',
    'moscow': 'москва',
    'saint petersburg': 'санкт-петербург',
    'st petersburg': 'санкт-петербург',
    'petersburg': 'санкт-петербург',
    'spb': 'спб',
    'yekaterinburg': 'екатеринбург',
    'ekaterinburg': 'екатеринбург',
    'novosibirsk': 'новосибирск',
    'krasnoyarsk': 'красноярск',
    'vladivostok': 'владивосток',
    'irkutsk': 'иркутск',
    'khabarovsk': 'хабаровск',
    'omsk': 'омск',
    'chelyabinsk': 'челябинск',
    'kazan': 'казань',
    'nizhny novgorod': 'нижний новгород',
    'nizhny-novgorod': 'нижний новгород',
    'samara': 'самара',
    'rostov': 'ростов',
    'rostov-on-don': 'ростов',
    'ufa': 'уфа',
    'perm': 'пермь',
    'voronezh': 'воронеж',
    'volgograd': 'волгоград',
    'krasnodar': 'краснодар',
    'saratov': 'саратов',
    'tyumen': 'тюмень',
    'tolyatti': 'тольятти',
    'togliatti': 'тольятти',
    'izhevsk': 'ижевск',
    'barnaul': 'барнаул',
    'ulyanovsk': 'ульяновск',
    'yaroslavl': 'ярославль',
    'makhachkala': 'махачкала',
    'tomsk': 'томск',
    'orenburg': 'оренбург',
    'kemerovo': 'кемерово',
    'novokuznetsk': 'новокузнецк',
    'ryazan': 'рязань',
    'astrakhan': 'астрахань',
    'penza': 'пенза',
    'lipetsk': 'липецк',
    'kirov': 'киров',
    'cheboksary': 'чебоксары',
    'kaliningrad': 'калининград',
    'tula': 'тула',
    'kursk': 'курск',
    'sochi': 'сочи',
    'stavropol': 'ставрополь',
    'ulan-ude': 'улан-удэ',
    'ulan ude': 'улан-удэ',
    'tver': 'тверь',
    'magnitogorsk': 'магнитогорск',
    'ivanovo': 'иваново',
    'bryansk': 'брянск',
    'belgorod': 'белгород',
    'surgut': 'сургут',
    'vladimir': 'владимир',
    'nizhny tagil': 'нижний тагил',
    'nizhny-tagil': 'нижний тагил',
    'arkhangelsk': 'архангельск',
    'chita': 'чита',
    'smolensk': 'смоленск',
    'kaluga': 'калуга',
    'volzhsky': 'волжский',
    'kurgan': 'курган',
    'orel': 'орел',
    'oryol': 'орел',
    'cherepovets': 'череповец',
    'vladikavkaz': 'владикавказ',
    'murmansk': 'мурманск',
    'saransk': 'саранск',
    'vologda': 'вологда',
    'tambov': 'тамбов',
    'sterlitamak': 'стерлитамак',
    'grozny': 'грозный',
    'groznyy': 'грозный',
    'yakutsk': 'якутск',
    'kostroma': 'кострома',
    'komsomolsk-on-amur': 'комсомольск-на-амуре',
    'komsomolsk-na-amure': 'комсомольск-на-амуре',
    'petrozavodsk': 'петрозаводск',
    'taganrog': 'таганрог',
    'nizhnevartovsk': 'нижневартовск',
    'yoshkar-ola': 'йошкар-ола',
    'yoshkar ola': 'йошкар-ола',
    'bratsk': 'братск',
    'novorossiysk': 'новороссийск',
    'novorossiisk': 'новороссийск',
    'dzerzhinsk': 'дзержинск',
    'belgorod oblast': 'белгородская область',
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

def extract_city_from_text(text):
    """Extract city name from text (comments, descriptions, etc.)"""
    if not text:
        return None
    
    text_lower = text.lower()
    
    # Try to find "Город: XXX" or "City: XXX" pattern
    patterns = [
        r'город[:\s]+([а-яёa-z\s\-]+)',
        r'city[:\s]+([а-яёa-z\s\-]+)',
        r'г\.[:\s]+([а-яёa-z\s\-]+)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text_lower)
        if match:
            city_raw = match.group(1).strip()
            # Clean up city name (remove trailing punctuation, newlines, etc.)
            city_clean = re.split(r'[\n\r,;]', city_raw)[0].strip()
            
            # Translate English city names to Russian
            if city_clean in CITY_TRANSLATIONS:
                return CITY_TRANSLATIONS[city_clean]
            
            return city_clean
    
    return None

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
    
    # Try direct match
    if city_lower in CITY_TIMEZONES:
        return CITY_TIMEZONES[city_lower]
    
    # Try translation
    if city_lower in CITY_TRANSLATIONS:
        translated_city = CITY_TRANSLATIONS[city_lower]
        return CITY_TIMEZONES.get(translated_city)
    
    return None

@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({"status": "ok"})

@app.route('/', methods=['POST', 'GET'])
@app.route('/webhook', methods=['POST', 'GET'])
def webhook():
    """Handle Bitrix24 webhook for deal creation/update"""
    try:
        # Log raw request for debugging
        logging.info(f"Received request: method={request.method}, content_type={request.content_type}")
        logging.info(f"Request args: {request.args}")
        logging.info(f"Request form: {request.form}")
        logging.info(f"Request headers: {dict(request.headers)}")
        
        # Get deal ID from request
        data = {}
        if request.is_json:
            data = request.json or {}
        else:
            data = request.form.to_dict()
            # Also check query parameters
            data.update(request.args.to_dict())
        
        logging.info(f"Parsed data: {data}")
        
        # Try different formats Bitrix24 might send
        deal_id = None
        
        # Format 1: document_id[2] = "DEAL_123"
        if 'document_id[2]' in data:
            doc_id = data.get('document_id[2]', '')
            if doc_id.startswith('DEAL_'):
                deal_id = doc_id.replace('DEAL_', '')
        
        # Format 2: Standard webhook formats
        if not deal_id:
            deal_id = (data.get('FIELDS[ID]') or 
                      data.get('data[FIELDS][ID]') or 
                      data.get('deal_id') or
                      data.get('ID') or
                      data.get('id'))
        
        # Format 3: Check if there's a PLACEMENT parameter (automation call)
        # In this case, we need to extract deal ID from somewhere else
        if not deal_id and 'PLACEMENT' in data:
            # This is likely an automation call without deal ID
            # We'll need to get the deal ID from the context
            logging.info("Automation call detected, but no deal ID found")
            # Try to get from document_type
            if 'document_type' in data:
                # This might give us hints about what to do
                logging.info(f"Document type: {data.get('document_type')}")
        
        if not deal_id:
            logging.warning(f"No deal ID in request. Full data: {data}")
            return jsonify({"status": "error", "message": "No deal ID provided"}), 400
        
        logging.info(f"Processing deal {deal_id}")
        
        # Get deal information
        deal = get_deal_info(deal_id)
        if not deal:
            return jsonify({"status": "error", "message": "Deal not found"}), 404
        
        contact_id = deal.get('CONTACT_ID')
        if not contact_id:
            logging.warning(f"No contact linked to deal {deal_id}")
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
        normalized_phone = None
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
        
        # Get city from deal - try multiple sources
        city = None
        
        # 1. Try to get from deal fields
        city = deal.get('UF_CRM_CITY') or deal.get('UF_CRM_694F054732342')
        
        # 2. If not found, try to extract from deal comments
        if not city:
            deal_comments = deal.get('COMMENTS', '')
            if deal_comments:
                city = extract_city_from_text(deal_comments)
                if city:
                    logging.info(f"Extracted city '{city}' from deal comments")
                    # Update the city field in deal
                    deal_updates['UF_CRM_CITY'] = city.title()
        
        # 3. If still not found, try to extract from contact comments
        if not city:
            contact_comments = contact.get('COMMENTS', '')
            if contact_comments:
                city = extract_city_from_text(contact_comments)
                if city:
                    logging.info(f"Extracted city '{city}' from contact comments")
                    # Update the city field in deal
                    deal_updates['UF_CRM_CITY'] = city.title()
        
        # Set timezone based on city
        if city:
            timezone = get_timezone_from_city(city)
            if timezone:
                deal_updates['UF_CRM_TIMEZONE'] = timezone
                logging.info(f"Set timezone {timezone} for city {city}")
            else:
                logging.warning(f"No timezone found for city: {city}")
        
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
        contact_name = contact.get('NAME', '') + ' ' + (contact.get('LAST_NAME', '') or '')
        contact_name = contact_name.strip()
        
        job_title = deal.get('TITLE', '')
        
        # Don't update title if it already contains contact name
        if contact_name and job_title and contact_name not in job_title:
            new_title = f"{contact_name} - {job_title}"
            deal_updates['TITLE'] = new_title
            logging.info(f"Updated deal title to: {new_title}")
        
        # Update deal if there are changes
        if deal_updates:
            success = update_deal(deal_id, deal_updates)
            if success:
                logging.info(f"Successfully updated deal {deal_id} with fields: {list(deal_updates.keys())}")
            else:
                logging.error(f"Failed to update deal {deal_id}")
        
        return jsonify({"status": "success", "deal_id": deal_id, "updates": list(deal_updates.keys())})
    
    except Exception as e:
        logging.error(f"Error processing webhook: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500

# ============================================================================
# STALE DEALS CHECKER
# ============================================================================

DAYS_THRESHOLD = 2  # Количество РАБОЧИХ дней без изменений
EXCLUDED_STAGES = ["WON", "LOSE", "UC_3IJV6C"]  # Готов работать, Провал, Кадровый резерв

def count_business_days(start_date, end_date):
    """Посчитать количество рабочих дней между двумя датами (исключая выходные)"""
    business_days = 0
    current_date = start_date
    
    while current_date < end_date:
        if current_date.weekday() < 5:  # Monday to Friday
            business_days += 1
        current_date += timedelta(days=1)
    
    return business_days

def get_stage_name_for_notification(stage_id):
    """Получить название стадии по ID"""
    stage_names = {
        "NEW": "Новый отклик",
        "UC_S2PZ7V": "В обработке",
        "PREPAYMENT_INVOICE": "Телефонное интервью",
        "EXECUTING": "Перезвонить соискателю",
        "FINAL_INVOICE": "Отправить инструкцию",
        "UC_O7P21K": "Название",
        "WON": "Готов работать",
        "LOSE": "Провал",
        "UC_3IJV6C": "Кадровый резерв"
    }
    return stage_names.get(stage_id, stage_id)

@app.route('/check-stale-deals', methods=['GET'])
def check_stale_deals():
    """Проверить застрявшие сделки и отправить уведомления менеджерам"""
    
    try:
        logging.info("Starting stale deals check...")
        
        # Получить все открытые сделки
        deals = []
        start = 0
        
        while True:
            response = requests.get(
                f"{WEBHOOK_URL}crm.deal.list",
                params={
                    "start": start,
                    "filter": {"CLOSED": "N"},
                    "select": ["ID", "TITLE", "STAGE_ID", "ASSIGNED_BY_ID", "DATE_MODIFY", "MOVED_TIME"]
                }
            )
            
            result = response.json()
            
            if "result" in result and result["result"]:
                deals.extend(result["result"])
                start += 50
                if len(result["result"]) < 50:
                    break
            else:
                break
        
        logging.info(f"Found {len(deals)} open deals")
        
        # Проверить на застой
        stale_deals = []
        now = datetime.now()
        
        for deal in deals:
            if deal.get("STAGE_ID") in EXCLUDED_STAGES:
                continue
            
            date_modify_str = deal.get("DATE_MODIFY") or deal.get("MOVED_TIME")
            if not date_modify_str:
                continue
            
            try:
                date_modify = datetime.fromisoformat(date_modify_str.replace("+03:00", ""))
                business_days_stale = count_business_days(date_modify, now)
                
                if business_days_stale >= DAYS_THRESHOLD:
                    days_stale = (now - date_modify).days
                    stale_deals.append({
                        "id": deal["ID"],
                        "title": deal.get("TITLE", "Без названия"),
                        "stage_id": deal.get("STAGE_ID"),
                        "assigned_by_id": deal.get("ASSIGNED_BY_ID"),
                        "days_stale": days_stale,
                        "business_days_stale": business_days_stale,
                        "last_modified": date_modify.strftime("%d.%m.%Y %H:%M")
                    })
            except Exception as e:
                logging.error(f"Error processing deal {deal.get('ID')}: {e}")
                continue
        
        logging.info(f"Found {len(stale_deals)} stale deals")
        
        if not stale_deals:
            return jsonify({"status": "success", "message": "No stale deals found", "count": 0})
        
        # Сгруппировать по менеджерам
        grouped = {}
        for deal in stale_deals:
            manager_id = deal["assigned_by_id"]
            if manager_id not in grouped:
                grouped[manager_id] = []
            grouped[manager_id].append(deal)
        
        # Отправить уведомления
        notifications_sent = 0
        
        for manager_id, deals_list in grouped.items():
            message = f"⏰ У вас {len(deals_list)} сделок без изменений более {DAYS_THRESHOLD} рабочих дней:\n\n"
            
            for deal in deals_list:
                message += f"📋 Сделка #{deal['id']}: {deal['title']}\n"
                message += f"   Стадия: {get_stage_name_for_notification(deal['stage_id'])}\n"
                message += f"   Без изменений: {deal['business_days_stale']} раб. дн. ({deal['days_stale']} календ. дн., с {deal['last_modified']})\n"
                message += f"   Ссылка: https://hr-adv.bitrix24.ru/crm/deal/details/{deal['id']}/\n\n"
            
            message += "Пожалуйста, обработайте эти сделки или переведите на следующую стадию."
            
            try:
                response = requests.post(
                    f"{WEBHOOK_URL}im.notify",
                    json={"to": manager_id, "message": message, "type": "USER"}
                )
                
                if "result" in response.json():
                    notifications_sent += 1
                    logging.info(f"Notification sent to user {manager_id}")
            except Exception as e:
                logging.error(f"Error sending notification to {manager_id}: {e}")
        
        return jsonify({
            "status": "success",
            "stale_deals_count": len(stale_deals),
            "notifications_sent": notifications_sent,
            "managers_notified": list(grouped.keys())
        })
    
    except Exception as e:
        logging.error(f"Error in stale deals check: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
