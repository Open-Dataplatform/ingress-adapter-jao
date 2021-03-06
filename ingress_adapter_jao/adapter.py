"""
JAO Adapter for Ingress.
"""
import argparse
import json
from configparser import ConfigParser
from datetime import datetime
import logging
import logging.config
import requests

from dateutil.relativedelta import relativedelta
from osiris.apis.ingress import Ingress
from osiris.core.azure_client_authorization import ClientAuthorization
from osiris.adapters.ingress_adapter import IngressAdapter

logger = logging.getLogger(__file__)


class JaoClient:
    """
    JAO Client - Connects to the JAO API and provides the calls.
    """
    def __init__(self, jao_url, auth_api_key, horizon):
        self.jao_url = jao_url
        self.auth_api_key = auth_api_key
        self.horizon = horizon

    def get_horizons(self):
        """
        Returns all the Horizon names in JSON format.
        """
        response = requests.get(
            url=f'{self.jao_url}/gethorizons',
            headers={'AUTH_API_KEY': self.auth_api_key}
        )
        return json.loads(response.content)

    def get_corridors(self):
        """
        Returns all the corridors in JSON format.
        """
        response = requests.get(
            url=f'{self.jao_url}/getcorridors',
            headers={'AUTH_API_KEY': self.auth_api_key}
        )
        return json.loads(response.content)

    def get_auctions(self, corridor, from_date, to_date=None):
        """
        Get all the auctions for the given corridor from from_date to to_date (optional).
        The call uses horizon Monthly.
        The to_date is optional - but can maximum be 31 days from from_date.
        :param corridor: The corridor to get data from.
        :param from_date: The date starting from.
        :param to_date: The end date (optional).
        :return: Returns the actions as JSON.
        """
        if to_date:
            raise Exception("to_date argument not used")

        response = requests.get(
            url=f'{self.jao_url}/getauctions',
            params={'corridor': corridor, 'fromdate': from_date, 'horizon': self.horizon},
            headers={'AUTH_API_KEY': self.auth_api_key}
        )
        if response.status_code == 200:
            return json.loads(response.content)
        # The challenge is, that bad request can be: no data available.
        # Bad response
        # - we ignore bad response, as it might not be a bad request, but just no data available
        return None

    def get_curtailment(self, corridor, from_date, to_date=None):
        """
        This request lists all curtailments in the system, with the option of filtering for corridors,
        curtailment period start and curtailment period end.
        :param corridor: The corridor to get data from.
        :param from_date: The date starting from.
        :param to_date: The end date (optional).
        :return: Returns the curtailments as JSON.
        """
        if to_date:
            raise Exception("to_date argument not used")

        response = requests.get(
            url=f'{self.jao_url}/getcurtailment',
            params={'corridor': corridor, 'fromdate': from_date},
            headers={'AUTH_API_KEY': self.auth_api_key}
        )
        return json.loads(response.content)

    def get_bids(self, auction_id):
        """
        This request will pull up all of the information about an auction that can be retrieved using a ???getauctions???
        request, plus all of the information about the bids linked to that auction.
        :param auction_id:
        :return:
        """
        response = requests.get(
            url=f'{self.jao_url}/getbids',
            params={'auctionid': auction_id},
            headers={'AUTH_API_KEY': self.auth_api_key}
        )
        return json.loads(response.content)


class CorridorState:
    """
    Corridor State is used to get the state, store the state, and update the state.
    The state saves the LastSuccessfulMonthlyDate for each corridor.
    """
    def __init__(self, ingress, horizon, default_date):
        if horizon not in ['Yearly', 'Monthly']:
            raise ValueError('Horizon not valid value: Valid values are Yearly and Monthly')
        self.json_content = ingress.retrieve_state()
        self.state = self.json_content[horizon]

        self.ingress = ingress
        self.horizon = horizon
        self.default_value = default_date

    def __str__(self):
        return '\n'.join([str(item) for item in self.state])

    def get_last_successful_monthly_date(self, corridor):
        """
        Returns the LastSuccessfulMonthlyDate or default value.
        :param corridor:
        :return:
        """
        monthly_date = self.default_value
        for item in self.state:
            if item['Corridor'] == corridor:
                monthly_date = item['LastSuccessfulMonthlyDate']
                return monthly_date.split('T')[0]
        return monthly_date.split('T')[0]

    def set_last_successful_monthly_date(self, corridor, monthly_date):
        """
        Sets the LastSuccessfulMonthlyDate for the corridor.
        :param corridor:
        :param monthly_date:
        :return:
        """
        for item in self.state:
            if item['Corridor'] == corridor:
                item['LastSuccessfulMonthlyDate'] = monthly_date
                return
        self.state.append({'Corridor': corridor, 'LastSuccessfulMonthlyDate': monthly_date})

    def save_state(self):
        """
        Saves the state.
        :return:
        """
        self.json_content[self.horizon] = self.state
        self.ingress.save_state(self.json_content)


class JaoAdapter(IngressAdapter):
    """
    The JAO Adapter.
    Implements the retrieve_data method.
    """
    def __init__(self, ingress_url: str,  # pylint: disable=too-many-arguments
                 tenant_id: str,
                 client_id: str,
                 client_secret: str,
                 dataset_guid: str,
                 jao_server_url: str,
                 jao_auth_api_key: str,
                 default_value: str,
                 horizon: str):
        client_auth = ClientAuthorization(tenant_id, client_id, client_secret)
        super().__init__(client_auth=client_auth, ingress_url=ingress_url, dataset_guid=dataset_guid)

        client_auth = ClientAuthorization(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
        ingress = Ingress(client_auth,
                          ingress_url,
                          dataset_guid)

        self.state = CorridorState(ingress, horizon, default_value)

        self.client = JaoClient(jao_server_url, jao_auth_api_key, horizon)

    def retrieve_data(self) -> bytes:
        """
        Retrives the data from JAO based on the state and returns it.
        :return:
        """
        logger.debug('Running the JAO Ingress Adapter')

        current_date = datetime.utcnow()

        corridors_all = self.client.get_corridors()

        corridors_all = [corridor['value'] for corridor in corridors_all]
        corridors = self.__filter_corridors(corridors_all, ['DK', 'D1', 'D2'])

        all_corridor_actions = []
        for corridor in corridors:
            monthly_date = self.state.get_last_successful_monthly_date(corridor)
            monthly_datetime_obj = datetime.strptime(monthly_date, '%Y-%m-%d')

            while monthly_datetime_obj < current_date:
                monthly_datetime_str = monthly_datetime_obj.strftime("%Y-%m-%d")
                logger.debug('Retrieving corridor: %s for %s', corridor, monthly_datetime_str)

                auctions = self.client.get_auctions(corridor, monthly_datetime_str)
                if auctions:
                    all_corridor_actions.append({'corridor': corridor, 'from_date': monthly_datetime_str,
                                                 'response': auctions})
                    self.state.set_last_successful_monthly_date(corridor, monthly_datetime_str)

                monthly_datetime_obj += relativedelta(months=+1)

        logger.debug('Save state and return response data')
        return json.dumps(all_corridor_actions).encode('utf_8')

    def save_state(self):
        self.state.save_state()

    @staticmethod
    def get_filename() -> str:
        return datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ') + '.json'

    def get_event_time(self) -> str:
        pass

    @staticmethod
    def __filter_corridors(corridors, filters):
        """
        Helper function used to filter the needed corridors.
        :param corridors:
        :param filters:
        :return:
        """
        result = []
        for corridor in corridors:
            for filter_item in filters:
                if filter_item in corridor:
                    result.append(corridor)
                    break
        return result


def __init_argparse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Ingress adapter for JAO')

    parser.add_argument('--conf',
                        nargs='+',
                        default=['conf.ini', '/etc/osiris/conf.ini'],
                        help='setting the configuration file')
    parser.add_argument('--credentials',
                        nargs='+',
                        default=['credentials.ini', '/vault/secrets/credentials.ini'],
                        help='setting the credential file')

    return parser


def ingest_jao_auctions_data():
    """
    Setups the adapter and runs it.
    """
    arg_parser = __init_argparse()
    args, _ = arg_parser.parse_known_args()

    config = ConfigParser()
    config.read(args.conf)
    credentials_config = ConfigParser()
    credentials_config.read(args.credentials)

    logging.config.fileConfig(fname=config['Logging']['configuration_file'],  # type: ignore
                              disable_existing_loggers=False)

    # To disable azure INFO logging from Azure
    if config.has_option('Logging', 'disable_logger_labels'):
        disable_logger_labels = config['Logging']['disable_logger_labels'].splitlines()
        for logger_label in disable_logger_labels:
            logging.getLogger(logger_label).setLevel(logging.WARNING)

    logger.info('Starting adapter')
    adapter = JaoAdapter(config['Azure Storage']['ingress_url'],
                         credentials_config['Authorization']['tenant_id'],
                         credentials_config['Authorization']['client_id'],
                         credentials_config['Authorization']['client_secret'],
                         config['Datasets']['source'],
                         config['JAO Server']['server_url'],
                         credentials_config['JAO Server']['auth_api_key'],
                         config['JAO Values']['default_date'],
                         config['JAO Values']['horizon'])

    adapter.upload_json_data(False)

    logger.info('Uploaded data complete')


if __name__ == "__main__":
    ingest_jao_auctions_data()
