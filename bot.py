# encoding=utf8
# @author Dan Michael O. Heggø <danmichaelo@gmail.com>
import re
import sys
import logging
import logging.config
import time

import codecs
import unicodecsv
import requests
import simplejson as json
from lxml import etree
import mwclient
import yaml

from wikidataeditor import Wikidata

# Set up logging
logging.config.dictConfig(yaml.load(open('logging.yml', 'r')))
logger = logging.getLogger('bot')

# Initialize and login
config = json.load(open('config.json', 'r'))

wd = Wikidata('DanmicholoBot (+http://tools.wmflabs.org/~danmicholobot)')

if wd.login(config['user'], config['pass']):
    logger.info('Hurra, vi er innlogga')
else:
    logger.error('Innloggingen feilet')
    sys.exit(1)

nowp = mwclient.Site('no.wikipedia.org')


def process_item(wd, page, autid):
    global nowp
    response = wd.get_entities('nowiki', page)
    q_number = response['entities'].keys()[0]
    if q_number == '-1':
        logger.error('Finnes ingen wikidataside for %s', page)

        p = nowp.pages[page]
        if not p.exists:
            logger.error('Finnes ingen wikipediaside for %s', page)
            return

        res = wd.add_entity('nowiki', 'nb', page)
        q_number = res['entity']['id']

    logger.info('Page: %s (%s)', page, q_number)

    r2 = requests.get('http://sru.bibsys.no/search/authority', params={
        'version': '1.2',
        'operation': 'searchRetrieve',
        'startRecord': '1',
        'maximumRecords': '1',
        'query': 'rec.identifier="%s"' % (autid),
        'recordSchema': 'marcxchange'
    })

    reference = {
        'P248': [  # nevnt i
            {
                'snaktype': 'value',
                'property': 'P248',  # nevnt i
                'datatype': 'wikibase-item',
                'datavalue': {
                    'type': 'wikibase-entityid',
                    'value': {
                        'entity-type': 'item',
                        'numeric-id': 16889143    # BIBSYS autoritetsregister
                    }
                }
            }],
        'P813': [  # aksessdato
            {
                'snaktype': 'value',
                'property': 'P813',  # aksessdato
                'datatype': 'time',
                'datavalue': {
                    'type': 'time',
                    'value': {
                        'time': '+00000002014-04-04T00:00:00Z',
                        'timezone': 0,
                        'before': 0,
                        'after': 0,
                        'precision': 11,
                        'calendarmodel': 'http://www.wikidata.org/entity/Q1985727'
                    }
                }
            }
        ]}

    claim = wd.create_claim_if_not_exists(q_number, 'P1015', autid)
    if claim:
        wd.set_reference(q_number, claim, reference)

    gender = ''
    dom = etree.fromstring(r2.text.encode('utf8'))
    gender = dom.xpath('//marc:record/marc:datafield[@tag="375"]/marc:subfield[@code="a"]/text()', namespaces=dom.nsmap)
    if len(gender) == 1:
        gender = gender[0]
        claim = None
        if gender == 'male':
            claim = wd.create_claim_if_not_exists(q_number, 'P21', {'entity-type': 'item', 'numeric-id': 6581097})
        elif gender == 'female':
            claim = wd.create_claim_if_not_exists(q_number, 'P21', {'entity-type': 'item', 'numeric-id': 6581072})

        if claim:
            wd.set_reference(q_number, claim, reference)


# Read CSV and process items
bs = unicodecsv.reader(codecs.open('data/2014-04-04-Bibsysmatch.csv', 'r'), delimiter=';')

# x90532701;Carlo_Aall;1962-;1962
for row in bs:
    autid = row[0]
    aname = row[1].replace('_', ' ')
    bd = row[2].split('-')
    birth = bd[0]
    death = ''
    if len(bd) > 1:
        death = bd[1]

    process_item(wd, aname, autid)
