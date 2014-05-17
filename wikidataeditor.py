# encoding=utf8
# @author Dan Michael O. Heggø <danmichaelo@gmail.com>
import requests
import logging
import time
import re
import simplejson as json
import yaml

logger = logging.getLogger('wikidataeditor')


class Wikidata:

    def __init__(self, user_agent):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': user_agent})

        # Respect https://www.mediawiki.org/wiki/Maxlag
        self.lagpattern = re.compile(r'Waiting for [^ ]*: (?P<lag>[0-9.]+) seconds? lagged')

    def raw_api_call(self, args):
        while True:
            url = 'https://www.wikidata.org/w/api.php'
            args['format'] = 'json'
            args['maxlag'] = 5
            # print args

            # for k, v in args.iteritems():
            #     if type(v) == unicode:
            #         args[k] = v.encode('utf-8')
            #     else:
            #         args[k] = v

            # data = urllib.urlencode(args)
            logger.debug(args)
            response = self.session.post(url, data=args)
            response = json.loads(response.text)

            logger.debug(response)

            if 'error' not in response:
                return response

            code = response['error'].pop('code', 'Unknown')
            info = response['error'].pop('info', '')
            if code == 'maxlag':
                lag = self.lagpattern.search(info)
                if lag:
                    logger.warn('Pausing due to database lag: %s', info)
                    time.sleep(int(lag.group('lag')))
                    continue

            logger.error("Unknown API error: %s\n%s\nResponse:\n%s",
                         info,
                         json.dumps(args, indent="\t"),
                         json.dumps(response, indent="\t"))
            return response
            # sys.exit(1)

    def login(self, user, pwd):
        args = {
            'action': 'login',
            'lgname': user,
            'lgpassword': pwd
        }
        response = self.raw_api_call(args)
        if response['login']['result'] == 'NeedToken':
            args['lgtoken'] = response['login']['token']
            response = self.raw_api_call(args)

        return (response['login']['result'] == 'Success')

    def pageinfo(self, entity):
        args = {
            'action': 'query',
            'prop': 'info',
            'intoken': 'edit',
            'titles': entity
        }
        return self.raw_api_call(args)

    def get_entities(self, site, page):
        args = {
            'action': 'wbgetentities',
            'sites': site,
            'titles': page
        }
        return self.raw_api_call(args)

    def get_props(self, q_number, props='labels|descriptions|aliases', languages=None):
        args = {
            'action': 'wbgetentities',
            'props': props,
            'ids': q_number
        }

        if languages:
            args['languages'] = languages

        result = self.raw_api_call(args)

        if result['success'] != 1:
            return None
        return result['entities'][q_number]

    def set_reference(self, entity, claim, reference):
        """
        Add a reference (snak) to a claim unless an *exactly*
        similar reference already exists. Note that only a minor
        modification will cause the reference to be re-added.
        """

        # logger.debug(json.dumps(claim, indent='\t'))

        statement = claim['id']
        if 'references' in claim:
            for ref in claim['references']:
                if ref['snaks'] == reference:
                    logger.info('  Reference already exists')
                    return

        logger.info('  Adding reference')
        time.sleep(2)

        response = self.pageinfo(entity)
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
        return self.raw_api_call(args)

    def get_claims(self, entity, prop):
        args = {
            'action': 'wbgetclaims',
            'entity': entity,
            'property': prop
        }
        resp = self.raw_api_call(args)
        if 'claims' in resp and prop in resp['claims']:
            return resp['claims'][prop]
        return []

    def create_claim(self, entity, prop, value):

        response = self.pageinfo(entity)
        itm = response['query']['pages'].items()[0][1]
        baserevid = itm['lastrevid']
        edittoken = itm['edittoken']

        args = {
            'action': 'wbcreateclaim',
            'bot': 1,
            'entity': entity,
            'property': prop,
            'snaktype': 'value',
            'value': json.dumps(value),
            'token': edittoken,
            'baserevid': baserevid
        }

        logger.info('  %s: Adding claim %s = %s', entity, prop, value)
        time.sleep(2)
        response = self.raw_api_call(args)
        return response['claim']

    def create_claim_if_not_exists(self, entity, prop, value):

        claims = self.get_claims(entity, prop)

        if claims:
            current_value = claims[0]['mainsnak']['datavalue']['value']
            if value == current_value:
                logger.info('  Claim %s already exists with the same value %s', prop, value)
                return claims[0]
            else:
                logger.warn('  Claim %s already exists. Existing value: %s, new value: %s', prop, current_value, value)
            return None

        return self.create_claim(entity, prop, value)

    def set_description(self, entity, lang, value, summary=None):

        response = self.pageinfo(entity)
        itm = response['query']['pages'].items()[0][1]
        baserevid = itm['lastrevid']
        edittoken = itm['edittoken']

        args = {
            'action': 'wbsetdescription',
            'bot': 1,
            'id': entity,
            'language': lang,
            'value': value,
            'token': edittoken
        }

        if summary:
            args['summary'] = summary

        logger.info(args)

        logger.info('  Setting description')
        time.sleep(2)

        response = self.raw_api_call(args)
        return response

    def set_label(self, entity, lang, value, summary=None):

        response = self.pageinfo(entity)
        itm = response['query']['pages'].items()[0][1]
        baserevid = itm['lastrevid']
        edittoken = itm['edittoken']

        args = {
            'action': 'wbsetlabel',
            'bot': 1,
            'id': entity,
            'language': lang,
            'value': value,
            'token': edittoken
        }

        if summary:
            args['summary'] = summary

        logger.info(args)

        logger.info('  Setting label')
        time.sleep(2)

        response = self.raw_api_call(args)
        return response

    def add_entity(self, site, lang, title):
        args = {
            'new': 'item',
            'data': {
                'sitelinks': {site: {'site': site, 'title': title}},
                'labels': {lang: {'language': lang, 'value': title}}
            }
        }

        logger.info('  Adding entity for %s:%s', site, title)
        time.sleep(3)

        return self.edit_entity(**args)

    def edit_entity(self, data={}, site=None, title=None, new=None, summary=None):

        response = self.pageinfo('DUMMY')
        itm = response['query']['pages'].items()[0][1]
        edittoken = itm['edittoken']

        args = {
            'action': 'wbeditentity',
            'bot': 1,
            'data': json.dumps(data),
            'token': edittoken
        }
        if site:
            args['site'] = site

        if title:
            args['title'] = title

        if new:
            args['new'] = new

        if summary:
            args['summary'] = summary

        response = self.raw_api_call(args)
        return response
