from unittest import TestCase

from hummingbot.connector.exchange.archax import archax_constants as CONSTANTS, archax_web_utils as web_utils


class WebUtilsTests(TestCase):
    def test_rest_url(self):
        url = web_utils.rest_url(path_url=CONSTANTS.EXCHANGE_INFO_PATH_URL, domain=CONSTANTS.DEFAULT_DOMAIN)
        self.assertEqual('https://ace-auth.archax.com/api/2.0/rest-gateway/reporting/instruments', url)
        url = web_utils.rest_url(path_url=CONSTANTS.EXCHANGE_INFO_PATH_URL, domain='archax_testnet')
        self.assertEqual('https://ace-auth.qat.archax.com/api/2.0/rest-gateway/reporting/instruments', url)
