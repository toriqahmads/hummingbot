import unittest

from hummingbot.connector.exchange.huobi import huobi_constants as CONSTANTS, huobi_utils as web_utils


class HuobiUtilsTestCases(unittest.TestCase):

    def test_public_rest_url(self):
        path_url = CONSTANTS.SERVER_TIME_URL
        expected_url = CONSTANTS.REST_URL + path_url
        self.assertEqual(expected_url, web_utils.public_rest_url(path_url))

    def test_private_rest_url(self):
        path_url = CONSTANTS.SERVER_TIME_URL
        expected_url = CONSTANTS.REST_URL + path_url
        self.assertEqual(expected_url, web_utils.private_rest_url(path_url))

    def test_is_exchange_information_valid(self):
        invalid_info = {
            "tags": "",
            "state": "online",
            "wr": "1.5",
            "sc": "coinalphahbot",
            "p": [
                {
                    "id": 9,
                    "name": "Grayscale",
                    "weight": 91
                }
            ],
            "bcdn": "COINALPHA",
            "qcdn": "HBOT",
            "elr": None,
            "tpp": 2,
            "tap": 4,
            "fp": 8,
            "smlr": None,
            "flr": None,
            "whe": None,
            "cd": None,
            "te": False,
            "sp": "main",
            "d": None,
            "bc": "COINALPHA",
            "qc": "HBOT",
            "toa": 1514779200000,
            "ttp": 8,
            "w": 999400000,
            "lr": 5,
            "dn": "ETH/USDT"
        }

        self.assertFalse(web_utils.is_exchange_information_valid(invalid_info))

        valid_info = {
            "tags": "",
            "state": "online",
            "wr": "1.5",
            "sc": "coinalphahbot",
            "p": [
                {
                    "id": 9,
                    "name": "Grayscale",
                    "weight": 91
                }
            ],
            "bcdn": "COINALPHA",
            "qcdn": "HBOT",
            "elr": None,
            "tpp": 2,
            "tap": 4,
            "fp": 8,
            "smlr": None,
            "flr": None,
            "whe": None,
            "cd": None,
            "te": True,
            "sp": "main",
            "d": None,
            "bc": "COINALPHA",
            "qc": "HBOT",
            "toa": 1514779200000,
            "ttp": 8,
            "w": 999400000,
            "lr": 5,
            "dn": "ETH/USDT"
        }
        self.assertTrue(web_utils.is_exchange_information_valid(valid_info))
