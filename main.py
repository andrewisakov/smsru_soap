#!/usr/bin/python3

import pickle
import threading
import logging
import datetime
import queue
import psycopg2 as pg2
from zeep import Client
import func
# import kts
DEBUG = True

SMSRU_LOGIN = 'raoul1975'
SMSRU_PSW='Terrorum1975'
WSDL = 'http://smsc.ru/sys/soap.php?wsdl'

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

handler = logging.FileHandler('/var/tmp/taxi_smsd_soap-%s.log' % \
            datetime.datetime.now().date())
handler.setLevel(logging.DEBUG)

formatter =  logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)

ROUTER = ('192.168.222.219', 5555)
LOCAL = ('0.0.0.0', 8556)


class SMSRU(threading.Thread):
    def __init__(self, data, out_queue):
        threading.Thread.__init__(self)
        self.data = data
        self.out_queue = out_queue
        self.phone = ('+7' + self.data['phone']) if len(self.data['phone']) == 10 else self.data['phone']
        self.text = self.data.get('sms_message') or self.data.get('message')

    def send(self, callback=None):
        cliz = Client(WSDL)
        return cliz.service.send_sms(login=SMSRU_LOGIN,
                                     psw=SMSRU_PSW,
                                     phones=self.phone,
                                     mes=self.text)

    def run(self):
        logger.debug('SMSRU.%s %s' % (self.phone, self.data))
        if self.text:
            try:
                db = pg2.connect('dbname=50eb8a3c0e444176ea5139ad5de941cd79daa8b9 '
                                 'user=freeswitch password="freeswitch" '
                                 'host=192.168.222.179 port=5432')
                c = db.cursor()
                sim_id = -42
                INSERT = ('insert into sms(sim_id, phone, date_time, text, direction) '
                          'values (%s, %s, %s, %s, %s)')
                result = self.send()
                if result.error:
                    direction = -42
                else:
                    direction = 42
                ARGS = (sim_id, self.phone[-10:], datetime.datetime.now(),
                        self.text, direction)
                c.execute(INSERT, ARGS)
                db.commit()
                c.close()
                self.out_queue.put(['SMS_SENDED', self.data])

            except Exception as e:
                logger.error(e)
        else:
            logger.error('Text message is None!')

class Dispatcher(threading.Thread):
    def __init__(self, in_queue, out_queue):
        threading.Thread.__init__(self)
        self.in_queue = in_queue
        self.out_queue = out_queue
        self.active = True
        self.events = {'SMS_RU:SEND_SMS': SMSRU}

    def sms_send(self, method, data):
        if (len(data['phone'][-10:]) == 10) and data:
            sms = method(data, self.out_queue)
            sms.start()

    def run(self):
        while self.active:
            try:
                ev, data = self.in_queue.get()
                logger.debug('Dispatcher %s: %s' % (ev, data))
                try:
                    self.sms_send(self.events[ev.upper()], data)
                except Exception as e:
                    logger.warning('Dispatcher exception 1: %s %s %s' % (e, ev, data))
            except Exception as e:
                logger.warning('Dispatcher exception 2: %s' % e)


def main():
    in_queue = queue.Queue()
    out_queue = queue.Queue()

    dispatcher = Dispatcher(in_queue, out_queue,)
    if not DEBUG: dispatcher.daemon = True
    dispatcher.start()
    sender = func.so_sender(out_queue, ROUTER, logger)
    if not DEBUG: sender.daemon = True
    sender.start()
    receiver = func.so_server(in_queue, LOCAL, logger)
    if not DEBUG: receiver.daemon = True
    receiver.start()
    if not DEBUG:
        out_queue.put(['SUBSCRIBE', {'events': ['SMSRU:SMS_SEND', ], 'port': LOCAL[1]}])
        dispatcher.join()
    else:
        in_queue.put(['SMS_RU:SEND_SMS', {'order': {
            'cashless': 0, 'callback_state': '1', 'id': 5473059},
            'phone': '+79278831370', 'order_id': 5473059,
            'date_time': datetime.datetime(2019, 7, 16, 11, 59, 29, 491161),
            'sms_message': 'Волга\nсеребристый\n404\n15 мин\nРасчёт по таксометру.\n',
            'message': ['tmVas_privetstvuet_sluzba_taxi.wav',
                        'tmMashina_vyehala_k_vam.wav', 'tmVolga.wav',
                        'tmColor.wav', 'tmsilver.wav', 'tmgos_nomer.wav',
                        'tm400.wav', 'tm4.wav', 'tmMashina_podjedet_k_vam_cherez.wav',
                        'tm15.wav', 'tmminut_.wav']}])

        print(out_queue.get())


if __name__=='__main__':
    if DEBUG:
        main()
    else:
        func.daemon_start(main, logger, debug=True)
