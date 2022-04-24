import time, logging, traceback
import argparse
import serial, elasticsearch
import requests
import os
from json import JSONEncoder
import json
from datetime import datetime


jeedom_url = 'http://127.0.0.1/core/api/jeeApi.php'
#jeedom_key = 'Tg24fBykZOXQ6PjkcKfQyqNmZyNS8rEX'
jeedom_key = 'R97g3SZOqwAt2YWX2v5luY3Mf0xnvODb'
jeedom_mapping = {
  'HCHC' : 737,
  'HCHP' : 736,
  'PAPP' : 735,
  'PTEC' : 738,
  'IINST1' : 739,
  'IINST2' : 740,
  'IINST3' : 741,
  }

startFrame = 0x02
endFrame = 0x03

logger = logging.getLogger(__name__)

class JSONDateTimeEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%dT%H:%M:%S')
        elif isinstance(obj, datetime.date):
            return obj.strftime('%Y-%m-%d')
        # Let the base class default method raise the TypeError
        return JSONEncoder.default(self, obj)

def save_items(items_to_save, filename='elec_metrics.json'):
    """Save items into a file as json list"""
    with open(filename,'a') as myfile:
      for my_info in items_to_save:
        item_as_string = json.dumps(my_info, cls=JSONDateTimeEncoder)
        myfile.write(item_as_string+'\n')

def load_items(filename='elec_metrics.json'):
    """Load items from a file"""
    result_list = []
    if os.path.isfile(filename):
        with open(filename,'r') as myfile:
            for line in myfile:
                try:
                    item_as_dict = json.loads(line)
                    result_list.append(item_as_dict)
                except:
                    logging.exception(u'Cannot parse line %s' % line)
                    continue
    return result_list

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
        if len(elt) >= 3:
            if len(elt[2]) == 0:
              elt[2] = ' '
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
        else:
          logging.warning('On ne trouve pas 3 elements mais %s : "%s"' % (len(elt), s))
        if time.time() - startTime > timeout:
            logger.warning('End frame char not received')
            return frame_data
        s = ''.join([chr(ord(char) & 127) for char in Cport.readline()])
        # print '"%s" chksum : "%s"' % (s, chr(checksum(s)))
        if s == '':
            logger.warning('No data on serial port')
            return frame_data
    app_buffer = s[s.index(chr(endFrame))+1:]
    return frame_data


def checksum(textval):
  cksum = 0;
  for acharidx in range(len(textval) - 4):
    cksum += ord(textval[acharidx])
  cksum = (cksum & 0x00003F) + 0x20;
  return cksum;


def push_to_jeedom(elec_data):
    for anelt, cmd_id in jeedom_mapping.items():
        if anelt in elec_data:
            try:
                r = requests.get('%s?apikey=%s&type=virtual&id=%s&value=%s' % (jeedom_url, jeedom_key, cmd_id, elec_data[anelt]))
                if r.status_code != 200:
                    logger.error('Jeedom does not accept the value for command %s : %s'%(cmd_id, r.content))
            except:
                logger.exception('Error while sending to Jeedom')

def push_to_elastic(ES, elec_data):
  try:
    ES.index('elec', 'teleinfo', elec_data)
    backup = load_items()
    while len(backup) > 0:        
        my_info = backup.pop()
        try:
            ES.index('elec', 'teleinfo', my_info)
        except:
            logging.exception(u'Cannot index document')
            break  
    if len(backup) == 0 and os.path.isfile('elec_metrics.json'):
        os.remove('elec_metrics.json')
  except:
    logger.exception('Error while sending to Elastic')
    save_items([elec_data])


def main():
    serialport = serial.Serial("/dev/ttyS0", baudrate=1200, timeout=5.0)
    ES = elasticsearch.Elasticsearch(['192.168.10.61', '192.168.10.62'])
    appbuffer = ''
    try:
      teleinfo_data = get_teleinfo(serialport, appbuffer,10)
      print(teleinfo_data)
      if len(teleinfo_data) > 1:
          teleinfo_data['date'] = datetime.now()
          push_to_jeedom(teleinfo_data)
          push_to_elastic(ES, teleinfo_data)
      else:
        logger.error('No data')
    except:
      logger.exception("Impossible de charger les donnees")

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,handlers=[logging.StreamHandler()])
    main()
