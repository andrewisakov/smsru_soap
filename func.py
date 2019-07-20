#!/usr/bin/python3
"""Это старый код, работает - не трожь!
   Может возникнуть вопрос: а почему не socketserver?
   Отвечаю: его не удалось запустить в режиме сервиса/демона.
"""
import time
import datetime
import threading
import socket
import pickle
import sys
import os
import fcntl
#import logging


SO_BUFFER = 1024
timestamp = datetime.datetime.now


class so_sender(threading.Thread):

    def __init__(self, out_sock_queue, ROUTER, logger, stop=False):
        threading.Thread.__init__(self)
        self.stop = stop
        self.out_sock_queue = out_sock_queue
        self.name = 'so_sender'
        self.ROUTER = ROUTER
        self.logger = logger

    def run(self):
        logger = self.logger
        while True:
            so = socket.socket()
            data = self.out_sock_queue.get()
            logger.debug('func.so_sender: передать %s' % data)
            data = pickle.dumps(data, protocol=2)
            while data:
                try:
                    so.send(data[:SO_BUFFER])  # Отправить часть
                    data = data[SO_BUFFER:]   # отрезать отправленное
                except Exception as e:
                    try:
                        logger.debug('func.so_sender: подключение')
                        so.connect(self.ROUTER)
                    except Exception as e:
                        logger.error(
                            'ERROR %s CONNECT exception %s' % (self.name, e))
                        self.active = False
                        break  # while data
            logger.debug('func.so_sender: %s передано...' %
                          'не' if data else '')
            so.close()


def so_receiver(conn, in_queue, se_no, logger):
    active = True
    name = 'func.so_receiver_%s' % se_no

    received = b''
    logger.debug('%s запущен' % name)
    while active:
        try:
            data = conn.recv(SO_BUFFER)
            received += data
            # print ('%s %s recieved=%s' % (timestamp(), name, received))
            try:
                received = pickle.loads(received)
                in_queue.put(received)

                # print ('%s: RECVD %s' % (self.name))
                logger.debug('%s: приянто %s (%s)' %
                              (name, received, in_queue.qsize()))
                conn.close()
                break
            except Exception as e:
                pass
        except Exception as e:
            logger.error('ERROR %s exception (2) %s -- %s' %
                          (name, e))
            break
    logger.info('INFO %s завершён.' % name)


class so_server(threading.Thread):

    def __init__(self, in_queue, LOCAL, logger):
        threading.Thread.__init__(self)
        self.active = True
        self.in_queue = in_queue
        self.name = 'func.so_server'
        self.LOCAL = LOCAL
        self.logger = logger

    def run(self):
        session_no = 0
        logger = self.logger
        while self.active:
            so = socket.socket()
            so.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            so.bind(self.LOCAL)
            so.listen(128)
            while 1:
                try:
                    conn, raddr = so.accept()
                    logger.debug('%s принято соединение (%s, %s)' %
                                  (self.name, conn, raddr))
                    try:
                        session_no += 1
                        so_receiver_ = threading.Thread(
                            target=so_receiver, args=(conn, self.in_queue, session_no, logger))
                        so_receiver_.start()
                    except Exception as e:
                        # logging.debug ('so_server exception (1) %s' % e)
                        logger.error('%s exception (1) -- %s' %
                                      (self.name, e))
                        break
                except Exception as e:
                    # ODlogging.debug ('so_server exception (2) %s' % e)
                    logger.error('%s exception (2) -- %s' %
                                  (self.name, e))
                    pass
            so.close()


def daemon_start(fun_to_start, logger, debug=False):
    #logger = None
    std_pipes_to_logger = True

    process_id = os.fork()
    logger.debug('daemon_start 1 pid %s' % process_id)
    if process_id < 0:
        logger.error('daemon_start pid < 0! exit...')
        sys.exit(1)
    elif process_id != 0:
        sys.exit(0)

    process_id = os.setsid()
    logger.debug('daemon_start 2 pid %s' % process_id)
    if process_id == -1:
        sys.exit(1)

    devnull = '/dev/null'
    if hasattr(os, 'devnull'):
        devnull = os.devnull
    null_descriptor = open(devnull, 'w')
    if not debug:
        for descriptor in (sys.stdin, sys.stdout, sys.stderr):
          descriptor.close()
          descriptor = null_descriptor

    os.umask(0o027)
    os.chdir('/')
    lockfile = open('/tmp/taxi_smsd_n.pid', 'w')
    fcntl.lockf(lockfile, fcntl.LOCK_EX|fcntl.LOCK_NB)

    lockfile.write('%s' % (os.getpid()))
    lockfile.flush()
    logger.debug('daemon_start %s' % fun_to_start)
    fun_to_start()
