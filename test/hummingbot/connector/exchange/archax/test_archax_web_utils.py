from unittest import TestCase

from hummingbot.connector.exchange.archax import archax_constants as CONSTANTS, archax_web_utils as web_utils


class WebUtilsTests(TestCase):
    def test_rest_url(self):
        url = web_utils.rest_url(path_url=CONSTANTS.LAST_TRADED_PRICE_PATH, domain=CONSTANTS.DEFAULT_DOMAIN)
        self.assertEqual('https://api.archax.com/spot/quote/v1/ticker/price', url)
        url = web_utils.rest_url(path_url=CONSTANTS.LAST_TRADED_PRICE_PATH, domain='archax_testnet')
        self.assertEqual('https://api-testnet.archax.com/spot/quote/v1/ticker/price', url)
