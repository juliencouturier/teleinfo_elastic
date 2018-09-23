#!/usr/bin/env python
# -*- coding: utf-8 -*-


import time, logging, datetime, traceback
import argparse
import serial, elasticsearch

startFrame = 0x02
endFrame = 0x03
def get_teleinfo(Cport, app_buffer, timeout = 5):
    startTime = time.time()
    s = app_buffer
    # Wait for startFrame
    while not chr(startFrame) in s:
        s = ''.join([chr(ord(char) & 127) for char in Cport.readline()])
        if s == '':
            logging.warning('No data on serial port')
            return {}
        if time.time() - startTime > timeout:
            logging.warning('Start frame char not received')
            return {}
    s = s[s.index(chr(startFrame))+1:]
    frame_data = {}
    while not chr(endFrame) in s:
        elt = s.split(' ')
        if len(elt) == 3:
            if checksum(s) == ord(elt[2][0]):
              if elt[0] in ['ADCO', 'OPTTARIF', 'PTEC', 'DEMAIN', 'HHPHC', 'MOTDETAT']:
                frame_data[elt[0]] = elt[1]
              else:
                try:
                  frame_data[elt[0]] = float(elt[1])
                except:
                  frame_data[elt[0]] = elt[1]
            else:
                logging.error('Le checksum de "%s" ne correspond pas : on calcule %d et on lit %d' % (s, checksum(s), ord(elt[2][0])))
        if time.time() - startTime > timeout:
            logging.warning('End frame char not received')
            return frame_data
        s = ''.join([chr(ord(char) & 127) for char in Cport.readline()])
        if s == '':
            logging.warning('No data on serial port')
            return frame_data
    app_buffer = s[s.index(chr(endFrame))+1:]
    return frame_data


def checksum(textval):
  cksum = 0;
  for acharidx in range(len(textval) - 4):
    cksum += ord(textval[acharidx])
  cksum = (cksum & 0x00003F) + 0x20;
  return cksum;

def main():
    serialport = serial.Serial("/dev/ttyS0", baudrate=1200, timeout=5.0)
    ES = elasticsearch.Elasticsearch(['192.168.10.41', '192.168.10.61'])
    appbuffer = ''
#    while True:
    try:
            teleinfo_data = get_teleinfo(serialport, appbuffer,10)
            #print teleinfo_data
            if len(teleinfo_data) > 1:
                teleinfo_data['date'] = datetime.datetime.now()
                ES.index('elec', 'teleinfo', teleinfo_data)
            else:
              logging.error('No data')
    except:
            logging.error(traceback.format_exc())
            print traceback.format_exc()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,handlers=[logging.StreamHandler()])
    main()
