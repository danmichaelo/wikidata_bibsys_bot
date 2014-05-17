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


file_handler = logging.FileHandler('bot.log')
file_handler.setLevel(logging.INFO)
logger.addHandler(file_handler)

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


def set_reference(entity, claim, reference):

    #print json.dumps(reference, indent='\t')

    statement = claim['id']
    if 'references' in claim:
        for ref in claim['references']:
            if ref['snaks'] == reference:
                logger.info('  Reference already exists')
                return

    logger.info('  Adding reference')
    time.sleep(2)

    response = pageinfo(entity)
    itm = response['query']['pages'].items()[0][1]
    baserevid = itm['lastrevid']
    edittoken = itm['edittoken']

    args = {
        'action': 'wbsetreference',
        'bot': 1,
        'statement': statement,
        'snaks': json.dumps(reference),
        'token': edittoken,
        'baserevid': baserevid
    }
    #logger.info("  Sleeping 2 secs")
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

    logger.info('  %s: Adding claim %s = %s', entity, property, value)
    time.sleep(2)
    response = raw_api_call(args)
    return response['claim']['id']


def create_claim_if_not_exists(entity, property, value):

    response = get_claims(entity, property)

    if property in response['claims']:
        curval = response['claims'][property][0]['mainsnak']['datavalue']['value']
        if value == curval:
            logger.info('  Claim %s already exists with the same value %s', entity, property, value)
            return response['claims'][property][0]
        else:
            logger.warn('  Claim %s already exists. Existing value: %s, new value: %s', entity, property, curval, value)
        return None

    return create_claim(entity, property, value)


def process_item(page, autid):
    response = get_entities('nowiki', page)
    q_number = response['entities'].keys()[0]
    if q_number == '-1':
        logger.error('Finnes ingen wikidataside for %s', page)
        return

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

    claim = create_claim_if_not_exists(q_number, 'P1015', autid)
    if claim:
        set_reference(q_number, claim, reference)

    gender = ''
    dom = etree.fromstring(r2.text.encode('utf8'))
    gender = dom.xpath('//marc:record/marc:datafield[@tag="375"]/marc:subfield[@code="a"]/text()', namespaces=dom.nsmap)
    if len(gender) == 1:
        gender = gender[0]
        claim = None
        if gender == 'male':
            claim = create_claim_if_not_exists(q_number, 'P21', {'entity-type': 'item', 'numeric-id': 6581097})
        elif gender == 'female':
            claim = create_claim_if_not_exists(q_number, 'P21', {'entity-type': 'item', 'numeric-id': 6581072})

        if claim:
            set_reference(q_number, claim, reference)


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
