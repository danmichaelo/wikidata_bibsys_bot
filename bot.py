# encoding=utf8
import re
import sys
import logging
import time

import codecs
import unicodecsv
import requests
import simplejson as json
from lxml import etree

config = json.load(open('config.json', 'r'))
bs = unicodecsv.reader(codecs.open('data/2014-04-04-Bibsysmatch.csv', 'r'), delimiter=';')

logger = logging.getLogger('local')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s  %(message)s'))
logger.addHandler(handler)

# Respect https://www.mediawiki.org/wiki/Maxlag
lagpattern = re.compile(r'Waiting for [^ ]*: (?P<lag>[0-9.]+) seconds? lagged')

session = requests.Session()
session.headers.update({'User-Agent': 'DanmicholoBot (+http://tools.wmflabs.org/~danmicholobot)'})


def raw_api_call(args):
    global session
    while True:
        url = 'https://www.wikidata.org/w/api.php'
        args['format'] = 'json'
        args['maxlag'] = 5
        #print args

        # for k, v in args.iteritems():
        #     if type(v) == unicode:
        #         args[k] = v.encode('utf-8')
        #     else:
        #         args[k] = v

        #data = urllib.urlencode(args)
        logger.debug(args)
        response = session.post(url, data=args)
        response = json.loads(response.text)

        logger.debug(response)

        if 'error' not in response:
            return response

        code = response['error'].pop('code', 'Unknown')
        info = response['error'].pop('info', '')
        if code == 'maxlag':
            lag = lagpattern.search(info)
            if lag:
                logger.warn('Pausing due to database lag: %s', info)
                time.sleep(int(lag.group('lag')))
                continue

        logger.error("Unknown API error: %s\n%s\nResponse:\n%s",
                     info,
                     json.dumps(args, indent="\t"),
                     json.dumps(response, indent="\t"))
        return response
        #sys.exit(1)


def login(user, pwd):
    args = {
        'action': 'login',
        'lgname': user,
        'lgpassword': pwd
    }
    response = raw_api_call(args)
    if response['login']['result'] == 'NeedToken':
        args['lgtoken'] = response['login']['token']
        response = raw_api_call(args)

    return (response['login']['result'] == 'Success')


def pageinfo(entity):
    args = {
        'action': 'query',
        'prop': 'info',
        'intoken': 'edit',
        'titles': entity
    }
    return raw_api_call(args)


def get_entities(site, page):
    args = {
        'action': 'wbgetentities',
        'sites': site,
        'titles': page
    }
    return raw_api_call(args)


def get_claims(entity, property):
    args = {
        'action': 'wbgetclaims',
        'entity': entity,
        'property': property
    }
    return raw_api_call(args)


def create_claim(entity, property, value):

    response = pageinfo(entity)
    itm = response['query']['pages'].items()[0][1]
    baserevid = itm['lastrevid']
    edittoken = itm['edittoken']

    args = {
        'action': 'wbcreateclaim',
        'bot': 1,
        'entity': entity,
        'property': property,
        'snaktype': 'value',
        'value': json.dumps(value),
        'token': edittoken,
        'baserevid': baserevid
    }
    logger.info("  Sleeping 4 secs")
    time.sleep(4)
    response = raw_api_call(args)
    return response


def create_claim_if_not_exists(entity, property, value):

    response = get_claims(entity, property)

    if property in response['claims']:
        curval = response['claims'][property][0]['mainsnak']['datavalue']['value']
        if value == curval:
            logger.info('  %s: Claim already exists with the same value', entity)
        else:
            logger.warn('  %s: Claim already exists. Existing value: %s, new value: %s', entity, curval, value)
        return None

    logger.info('  %s: Claim does not exist', entity)

    return create_claim(entity, property, value)


def process_item(page, autid):
    response = get_entities('nowiki', page)
    q_number = response['entities'].keys()[0]
    if q_number == '-1':
        logger.error('Finnes ingen wikidataside for %s', page)
        return

    logger.info('Page: %s (%s)', page, q_number)

    response = requests.get('http://sru.bibsys.no/search/authority', params={
        'version': '1.2',
        'operation': 'searchRetrieve',
        'startRecord': '1',
        'maximumRecords': '1',
        'query': 'rec.identifier="%s"' % (autid),
        'recordSchema': 'marcxchange'
    })

    logger.info('Adding: %s', autid)
    create_claim_if_not_exists(q_number, 'P1015', autid)

    gender = ''
    dom = etree.fromstring(response.text.encode('utf8'))
    gender = dom.xpath('//marc:record/marc:datafield[@tag="375"]/marc:subfield[@code="a"]/text()', namespaces=dom.nsmap)
    if len(gender) == 1:
        gender = gender[0]
        if gender == 'male':
            logger.info('Setting gender to: male')
            create_claim_if_not_exists(q_number, 'P21', {'entity-type': 'item', 'numeric-id': 6581097})
        elif gender == 'female':
            logger.info('Setting gender to: female')
            create_claim_if_not_exists(q_number, 'P21', {'entity-type': 'item', 'numeric-id': 6581072})


if login(config['user'], config['pass']):
    logger.info('Hurra, vi er innlogga')
else:
    logger.error('Innloggingen feilet')
    sys.exit(1)


#x90532701;Carlo_Aall;1962-;1962
for row in bs:
    autid = row[0]
    aname = row[1]
    bd = row[2].split('-')
    birth = bd[0]
    death = ''
    if len(bd) > 1:
        death = bd[1]

    process_item(aname, autid)
