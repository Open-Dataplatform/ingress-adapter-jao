"""
JAO Adapter for Ingress.
"""
import configparser
import json
from datetime import datetime
import requests

from dateutil.relativedelta import relativedelta
from osiris.adapters.ingress_adapter import IngressAdapter
from osiris.apis.egress import Egress
from osiris.apis.ingress import Ingress


class JaoClient:
    """
    JAO Client - Connects to the JAO API and provides the calls.
    """
    def __init__(self, jao_url, auth_api_key):
        self.jao_url = jao_url
        self.auth_api_key = auth_api_key

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
        response = requests.get(
            url=f'{self.jao_url}/getauctions',
            params={'corridor': corridor, 'fromdate': from_date, 'horizon': 'Monthly'},
            headers={'AUTH_API_KEY': self.auth_api_key}
        )
        return json.loads(response.content)

    def get_curtailment(self, corridor, from_date, to_date=None):
        """
        This request lists all curtailments in the system, with the option of filtering for corridors,
        curtailment period start and curtailment period end.
        :param corridor: The corridor to get data from.
        :param from_date: The date starting from.
        :param to_date: The end date (optional).
        :return: Returns the curtailments as JSON.
        """
        response = requests.get(
            url=f'{self.jao_url}/getcurtailment',
            params={'corridor': corridor, 'fromdate': from_date},
            headers={'AUTH_API_KEY': self.auth_api_key}
        )
        return json.loads(response.content)

    def get_bids(self, auction_id):
        """
        This request will pull up all of the information about an auction that can be retrieved using a “getauctions”
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
    def __init__(self, ingress, egress, default_date):
        json_content = egress.retrieve_state()
        self.state = json_content['LastUpdates']

        self.ingress = ingress
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
        state = {"LastUpdates": self.state}
        self.ingress.save_state(json.dumps(state))


def filter_corridors(corridors, filters):
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


class ConfigurationParser:
    """
    Class used to only parse the configuration file once.
    """
    def __init__(self):
        config = configparser.ConfigParser()
        config.read(['../conf.ini', '/etc/osiris/conf.ini', '/etc/transform-ingress2event-time-conf.ini'])
        self.jao_url = config['JAO Server']['server_url']
        self.auth_api_key = config['JAO Server']['auth_api_key']

        self.egress_url = config['Azure Storage']['egress_url']
        self.ingress_url = config['Azure Storage']['ingress_url']
        self.tenant_id = config['Authorization']['tenant_id']
        self.client_id = config['Authorization']['client_id']
        self.client_secret = config['Authorization']['client_secret']
        self.dataset_guid = config['Datasets']['source']
        self.default_value = config['JAO Values']['default_date']

        self.egress = Egress(self.egress_url, self.tenant_id, self.client_id, self.client_secret, self.dataset_guid)
        self.ingress = Ingress(self.ingress_url, self.tenant_id, self.client_id, self.client_secret, self.dataset_guid)

    def get_egress(self):
        """
        Returns the Egress
        :return:
        """
        return self.egress

    def get_ingress(self):
        """
        Returns the Ingress
        :return:
        """
        return self.ingress


class JaoAdapter(IngressAdapter):
    """
    The JAO Adapter.
    Implements the retrieve_data method.
    """
    def retrieve_data(self) -> bytes:
        """
        Retrives the data from JAO based on the state and returns it.
        :return:
        """
        current_date = datetime.utcnow()

        config = ConfigurationParser()

        state = CorridorState(config.get_ingress(), config.get_egress(), config.default_value)
        print(state)

        client = JaoClient(config.jao_url, config.auth_api_key)

        corridors = client.get_corridors()
        corridors = [corridor['value'] for corridor in corridors]
        corridors = filter_corridors(corridors, ['DK', 'D1', 'D2'])

        # for testing
        # corridors = ['GB-NI', 'GB-IE', 'IF2-FR-GB', 'BE-DE', 'NL-NO', 'CH-FR']

        responses = []
        for corridor in corridors:
            monthly_date = state.get_last_successful_monthly_date(corridor)
            monthly_datetime_obj = datetime.strptime(monthly_date, '%Y-%m-%d')

            while monthly_datetime_obj < current_date:

                print("Fetching", corridor, monthly_datetime_obj.strftime("%Y-%m-%d"))

                response = client.get_auctions(corridor, monthly_datetime_obj.strftime("%Y-%m-%d"))
                if isinstance(response, dict):
                    # Bad response
                    print(response['status'], response['message'])
                    # Log error retrieve - but continue - maybe next dataset is fine
                else:
                    # Good response
                    print(response)
                    responses.append({'corridor': corridor, 'from_date': monthly_datetime_obj.strftime("%Y-%m-%d"),'response': response})
                    state.set_last_successful_monthly_date(corridor, monthly_datetime_obj.strftime("%Y-%m-%d"))

                monthly_datetime_obj += relativedelta(months=+1)

        print(state)
        print(responses)
        state.save_state()
        return json.dumps(responses).encode('utf_8')


def ingest_jao_auctions_data():
    """
    Setups the adapter and runs it.
    """
    config = ConfigurationParser()

    adapter = JaoAdapter(config.ingress_url,
                         config.tenant_id,
                         config.client_id,
                         config.client_secret,
                         config.dataset_guid)

    adapter.upload_json_data(False)


if __name__ == "__main__":

    ingest_jao_auctions_data()
