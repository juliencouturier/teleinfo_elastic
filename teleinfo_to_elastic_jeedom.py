#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time, logging, datetime, traceback
import argparse
import serial, elasticsearch
import requests
import json
import os
from json import JSONEncoder
from datetime import datetime
from tzlocal import get_localzone

SYSTEM_TZ = get_localzone()

default_jeedom_mapping = {
  'HCHC' : 737,
  'HCHP' : 736,
  'PAPP' : 735,
  'PTEC' : 738,
  'IINST1' : 739,
  'IINST2' : 740,
  'IINST3' : 741,
  }

SPEC_DATA_TYPE = {
    "ADCO" : str,
    "OPTARIF" : str,
    "BASE" : int,
    "HCHC" : int,
    "HCHP" : int,
    "EJPHN" : int,
    "EJPHPM" : int,
    "GAZ" : int,
    "AUTRE" : int,
    "PTEC" : str,
    "MOTDETAT" : str,
    "ISOUSC" : int,
    "BBRHCJB" : int,
    "BBRHPJB" : int,
    "BBRHCJW" : int,
    'BBRHPJW' : int,
    "BBRHCJR" : int,
    "BBRHPJR" : int,
    "PEJP" : int,
    "DEMAIN" : str,
    "IINST" : int,
    "IINST1" : int,
    "IINST2" : int,
    "IINST3" : int,
    "ADPS" : int,
    "IMAX" : int,
    "IMAX1" : int,
    "IMAX2" : int,
    "IMAX3" : int,
    "HHPHC" : str,
    "PAPP" : int,
    "PPOT" : str,
    "ADIR1" : int,
    "ADIR2" : int,
    "ADIR3" : int,
    "ADSC" : str,
    "VTIC" : str,
    "DATE" : str,
    "NGTF" : str,
    "LTARF" : str,
    "EAST" : int,
    "EASF01" : int,
    "EASF02" : int,
    "EASF03" : int,
    "EASF04" : int,
    "EASF05" : int,
    "EASF06" : int,
    "EASF07" : int,    
    "EASF08" : int,
    "EASF09" : int,
    "EASF10" : int,
    "EASD01" : int,
    "EASD02" : int,
    "EASD03" : int,
    "EASD04" : int,
    "EAIT" : int,
    "ERQ1" : int,
    "ERQ2" : int,
    "ERQ3" : int,
    "ERQ4" : int,
    "IRMS1" : int,
    "IRMS2" : int,
    "IRMS3" : int,
    "URMS1" : int,
    "URMS2" : int,
    "URMS3" : int,
    "PREF" : int,
    "PCOUP" : int,
    "SINSTS" : int,
    "SINSTS1" : int,
    "SINSTS2" : int,
    "SINSTS3" : int,
    "SMAXSN" : int,
    "SMAXSN1" : int,
    "SMAXSN2" : int,
    "SMAXSN3" : int,
    "SMAXSN-1" : int,
    "SMAXSN1-1" : int,
    "SMAXSN2-1" : int,
    "SMAXSN3-1" : int,    
    "SINSTI" : int,
    "SMAXIN" : int,
    "SMAXIN-1" : int,
    "CCASN" : int,
    "CCASN-1" : int,
    "CCAIN" : int,
    "CCAIN-1" : int,
    "UMOY1" : int,
    "UMOY2" : int,
    "UMOY3" : int,
    "STGE" : str,   
    "DPM1" : str,    
    "FPM1" : str,    
    "DPM2" : str,    
    "FPM2" : str,    
    "DPM3" : str,    
    "FPM3" : str,    
    "MSG1" : str,    
    "MSG2" : str,    
    "PRM" : str,    
    "RELAIS" : str,    
    "NTARF" : str, 
    "NJOURF" : str, 
    "NJOURF+1" : str, 
    "PJOURF+1" : str, 
    "PPOINTE" : str, 
}

startFrame = 0x02
endFrame = 0x03

logger = logging.getLogger(__name__)

class JSONDateTimeEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%dT%H:%M:%S%z')
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

def checksum(textval):
  cksum = 0
  for acharidx in range(len(textval) - 4):
    cksum += ord(textval[acharidx])
  cksum = (cksum & 0x00003F) + 0x20
  return cksum

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))


class TeleInfoRetriever():
    app_buffer = ''

    def __init__(self, *args, **kwargs):
        self.serialport = serial.Serial(kwargs.get('serial_port', "/dev/ttyS0"), baudrate=kwargs.get('baud_rate', 1200), timeout=5.0)
        self.index = kwargs.get('elastic_index', 'elec.%Y')
        self.ES = elasticsearch.Elasticsearch(kwargs.get('elastic_url', 'http://127.0.0.1:9200').split(','), retry_on_timeout=True, max_retries=5)
        self.jeedom_mapping = kwargs.get('jeedom_mapping', default_jeedom_mapping)
        self.jeedom_url = kwargs.get('jeedom_url', 'http://127.0.0.1/core/api/jeeApi.php')
        self.jeedom_key = kwargs.get('jeedom_key')

    def get_teleinfo(self, timeout = 10):
        startTime = time.time()
        s = self.app_buffer
        # Wait for startFrame
        while not chr(startFrame) in s:
            s = ''.join([chr(ord(char) & 127) for char in self.serialport.readline()])
            if s == '':
                logging.warning('No data on serial port')
                return {}
            if (time.time() - startTime) > timeout:
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
            elif s == '\n':
                pass
            else:
                logging.warning('On ne trouve pas 3 elements mais %s : "%s"' % (len(elt), s))
            if time.time() - startTime > timeout:
                logger.warning('End frame char not received')
                return frame_data
            s = ''.join([chr(ord(char) & 127) for char in self.serialport.readline()])
            # print '"%s" chksum : "%s"' % (s, chr(checksum(s)))
            if s == '':
                logger.warning('No data on serial port')
                return frame_data
        self.app_buffer = s[s.index(chr(endFrame))+1:]
        return frame_data

    def push_to_jeedom(self, elec_data, **kwargs):
        for anelt, cmd_id in self.jeedom_mapping.items():
            if anelt in elec_data:
                try:
                    r = requests.get('%s?apikey=%s&plugin=virtual&type=event&id=%s&value=%s' % (self.jeedom_url, self.jeedom_key, cmd_id, elec_data[anelt]))
                    if r.status_code != 200:
                        logger.error('Jeedom does not accept the value for command %s : %s'%(cmd_id, r.content))
                except:
                    logger.exception('Error while sending to Jeedom')

    def format_data(self, data, allowed_data=SPEC_DATA_TYPE):
        result = {}
        for key, val in data.items():
            if key in allowed_data:
                try:
                    result[key] = allowed_data[key](val)
                except:
                    logging.warning('%s is not a valid %s', val, key)
                    result[key] = None
        result['date'] = data['date']
        return result

    def push_to_elastic(self, elec_data, allowed_data=SPEC_DATA_TYPE):
        try:
            data = self.format_data(elec_data, allowed_data)
            self.ES.index(index=data['date'].strftime(self.index), body=data, id=data['date'].strftime('%Y%m%d%H%M%S'))
            backup = load_items()
            while len(backup) > 0:
                my_info = backup.pop()
                data = self.format_data(my_info, allowed_data)
                try:
                    date_metric = datetime.strptime(data['date'], '%Y-%m-%dT%H:%M:%S%z')
                except:
                    logging.exception(u'Document date is invalid : %s', data['date'])
                    continue
                try:
                    self.ES.index(index=date_metric.strftime(self.index), body=data, id=date_metric.strftime('%Y%m%d%H%M%S'))
                except:
                    logging.exception(u'Cannot index document')
                    break
            if len(backup) == 0 and os.path.isfile('elec_metrics.json'):
                os.remove('elec_metrics.json')
        except:
            logger.exception('Error while sending to Elastic')
            save_items([elec_data])

    def read_and_push(self, **kwargs):
        try:
            teleinfo_data = self.get_teleinfo()
            # print(teleinfo_data)
            if len(teleinfo_data) > 1:
                teleinfo_data['date'] = SYSTEM_TZ.localize(datetime.now())
                self.push_to_jeedom(teleinfo_data, **kwargs)
                self.push_to_elastic(teleinfo_data)
                return 1
            else:
                logger.error('No data')
        except:
            logger.exception("Impossible de charger les donnees")
        return 0


def main(*args, **kwargs):
    teleinfo = TeleInfoRetriever(**kwargs)
    info_count = kwargs.get('nb_info', 0)
    if info_count > 0:
        cntr = 0
        while cntr <= info_count:
            cntr += teleinfo.read_and_push()
    else:
        while True:
            teleinfo.read_and_push()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Index jeedom informations into elastic')
    parser.add_argument('-j', '--jeedom_url', type=str, default='http://127.0.0.1/core/api/jeeApi.php',
                        help='Jeedom API URL like : http://127.0.0.1/core/api/jeeApi.php')
    parser.add_argument('-k', '--jeedom_key', type=str,
                        help='Jeedom API Key')
    parser.add_argument('-e', '--elastic_url', type=str, default='http://127.0.0.1:9200',
                        help='Elastic URL')
    parser.add_argument('-i', '--elastic_index', type=str, default='elec.%Y',
                        help='Elastic index to store data')
    parser.add_argument('-p', '--serial_port', type=str, default='/dev/ttyS0',
                        help='Incoming data serial port')
    parser.add_argument('-b', '--baud_rate', type=int, default=1200,
                        help='Serial port baud rate')
    parser.add_argument('-n', '--nb_info', type=int, default=0,
                        help='Telinfo count (0 : continuous)')
    parser.add_argument('-c', '--config', type=argparse.FileType('r'),
                        help='JSON or YAML config File with those parameters')
    args = parser.parse_args()
    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # add formatter to ch
    ch.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(ch)
    logging.basicConfig(level=logging.WARNING,handlers=[logging.StreamHandler()])
    if args.config is not None:
        try:
            config_dict = json.load(args.config)
        except:
            config_dict = yaml.parse(args.config)
        main(**config_dict)
    else:
        main(**args.__dict__)    
