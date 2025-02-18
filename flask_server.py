from flask import Flask, request
import requests
import json
import os
from dataclasses import dataclass
import pandas as pd
from fast_bitrix24 import Bitrix
import sched, time

sms_api_key = os.environ['MTS_API_KEY']
mng_phone = os.environ['MANAGER_PHONE']
BITRIX_CODE = '1g7e615r73o9p9it'
BITRIX_URL = r'https://b24-mgttck.bitrix24.ru/rest/1/' + BITRIX_CODE
endpoint = Bitrix(BITRIX_URL)

### Bitrix


def get_contacts():
    truncated_contact_items = endpoint.get_all("crm.contact.list")
    result = []
    for tci in truncated_contact_items:
        ans = endpoint.call("crm.contact.get", items={"ID": tci["ID"]})
        result.append(ans['order0000000000'])
    return result


def get_deals():
    return endpoint.get_all("crm.deal.list")


def is_contact_in_deal(deal_item: dict, contact: dict | str):
    if type(contact) is dict: contact = contact["ID"]
    return deal_item['CONTACT_ID'] == contact


def search_deals_by_contact(deals: list[dict], contact: dict | str):
    contact_deals = []
    for d in deals:
        if is_contact_in_deal(d, contact): contact_deals.append(d)
    return contact_deals


def get_deals_sum(contact_deals: list[dict]):
    deals_sum = 0
    for d in contact_deals:
        if d['STAGE_SEMANTIC_ID'] != 'S':  # The deal is not successfully
            continue
        deals_sum += float(d['OPPORTUNITY'])
    return deals_sum


### Exolve
    
    
def send_SMS(recepient: str, send_str: str):
    payload = {'number': crm_phone, 'destination': recepient, 'text': send_str}
    r = requests.post(r'https://api.exolve.ru/messaging/v1/SendSMS', headers={'Authorization': 'Bearer '+sms_api_key}, data=json.dumps(payload))
    print(r.text)
    return r.text, r.status_code

### Functional


@dataclass
class DB:
    task_table = pd.DataFrame(columns=['ID', 'S1', 'S2', 'S3', 'S4', 'S5', 'S6', 'S7', 'S8', 'sale', 'score']).set_index(['ID'])
    phone_table = pd.DataFrame(columns=['phone', 'client_id']).set_index(['phone'])

    def add_client_phones(self, client_full_record):
        contact_phones = [ph.get('VALUE', '') for ph in client_full_record.get('PHONE', [{}])]
        if len(contact_phones) == 0:
            return 0
        for c in contact_phones:
            self.phone_table[c] = [client_full_record.get('ID')]
        return 1

    def add_task_item(self, client_full_record):
        if not self.add_client_phones(client_full_record):
            return
        self.task_table[client_full_record.get('ID')] = 0

    def get_phone(self, client_ID):
        phone_indexes = self.phone_table['ID'] == client_ID
        return self.phone_table.loc[phone_indexes][0]


STAGE_DICT = {'День Земли!': 1, 'Без пакета': 2, 'Без стаканов': 3, 'Экономим электричество!': 4, 'Отсортировали мусор!': 5,
              'Сдали батарейки!': 6, 'Теперь чисто!': 7, 'Сдали одежду!': 8}
BYE_TEXT = 'Также вы можете приобрести наши эко-товары и поднять свой рейтинг в зависимости от суммы покупок!'
SEND_STRINGS = {1: 'Приветствуем Вас! Вы готовы поддержать Час Земли и отказаться от электричества и телефона всего на час? Если да, то приглашаем вас на игру! Известите нас ответным СМС о выполнении первого задания и подтвердите этим своё участие. Пройдите несколько заданий, присылайте нам результаты, зарабатывайте очки в нашей игре!',
                2: 'Откажитесь от покупки пластиковых пакетов - берите с собой сумку или свой пакет!',
                3: 'Попробуйте хотя бы следующих 6 дней отказаться от одноразовых стаканов!',
                4: 'Попробуйте хотя бы следующих 5 дней выключать свет за собой!',
                5: 'Хотя бы следующих 4 дня предлагаем вам сортировать мусор!',
                6: 'Предлагаем вам сдать в переработку батарейки.',
                7: 'Позаботьтесь о себе и природе - проведите дома уборку!',
                8: 'Старую одежду вы можете сдать в специальные пункты приёма.'}
WHAT_TO_ANSWER = 'В качестве подтверждения выполнения задания пришлите нам: '
app = Flask(__name__)
db = DB()
START_DELAY = 2  # For test
DELAY = 20
s = sched.scheduler(time.time, time.sleep)


def send_notif(stage: int):
    index_to_send = db.task_table.index
    if stage>1:
        index_to_send = db.task_table['S1'] == 1
    message = SEND_STRINGS[stage] + f'\n' + WHAT_TO_ANSWER + list(STAGE_DICT)[stage] + f'\n' + BYE_TEXT
    print(message)
    for ai in db.task_table.loc[index_to_send]:
        recipient = db.get_phone(ai['ID'])
        send_SMS(recipient, message)


def text_to_stage(text: str):
    for k in STAGE_DICT:
        if k in text:
            return STAGE_DICT[k]
    return -1


def set_by_tel(tel, stage, num=1):
    client_id = db.phone_table[tel]
    print('client_id')
    print(client_id)
    if db.task_table[client_id, 'S1'] == 0:
        return -1  # Client is not participant!
    db.task_table[client_id, 'S'+str(stage)] = num
    return num


@app.route('/receive_data', methods=['POST'])
def receive_data():
    print('Receiving...')
    SMS_data = request.form.to_dict()
    print(request.url)
    print(request.form)
    print(SMS_data)
    print(request.args)
    if SMS_data.get('event_id') == 'DIRECTION_OUTGOING':
        print('SMS not received')
        return '-2', 200
    stage = text_to_stage(SMS_data.get('text'))
    print(stage)
    if stage == -1:
        return '-3', 200
    num = set_by_tel(SMS_data.get('sender'), stage)
    return str(num), 200


def start_eco_day():
    print('Initing...')
    db.__init__()
    contacts = get_contacts()
    print(contacts)
    for c in contacts:
        db.add_client_phones(c)
        db.add_task_item(c)
    print(db.task_table)
    print(db.phone_table)
    print('Inited')


def finish_eco_day():
    dls = get_deals()
    for ai in db.task_table.index:
        if db.task_table.loc[ai, 'S1'] == 0: continue
        current_deals = search_deals_by_contact(dls, ai)
        client_sum = get_deals_sum(current_deals)
        db.task_table.loc[ai, 'sale'] = client_sum
        db.task_table.loc[ai, 'score'] = db.task_table.loc[ai, ['S1', 'S2', 'S3', 'S4', 'S5', 'S6', 'S7', 'S8']].sum()*100+db.task_table.loc[ai, 'sale']
    winner = db.task_table['score'].values.argmax()
    send_SMS(db.task_table.index[winner], 'Вы победили!')


def main():
    # Schedule tasks
    print('Started')
    s.enter(START_DELAY, 1, start_eco_day, argument=())
    for ai in range(8):
        s.enter(START_DELAY+DELAY*(ai+1), 1, send_notif, argument=(ai,))
    s.enter(DELAY*10, 1, finish_eco_day, argument=())
    app.run(host='0.0.0.0', port=5000)
    s.run()


if __name__ == '__main__':
    main()
