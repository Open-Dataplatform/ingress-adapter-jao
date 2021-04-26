"""
Docstring
"""
import configparser
import json
from datetime import datetime
import requests

from dateutil.relativedelta import relativedelta
from osiris.apis.egress import Egress
from osiris.apis.ingress import Ingress


class JaoClient:
    """
    JAO Client
    """
    def __init__(self, jao_url, auth_api_key):
        self.jao_url = jao_url
        self.auth_api_key = auth_api_key

    def get_horizons(self):
        """

        :return:
        """
        response = requests.get(
            url=f'{self.jao_url}gethorizons',
            headers={'AUTH_API_KEY': self.auth_api_key}
        )
        return json.loads(response.content)

    def get_corridors(self):
        """

        :return:
        """
        response = requests.get(
            url=f'{self.jao_url}getcorridors',
            headers={'AUTH_API_KEY': self.auth_api_key}
        )
        return json.loads(response.content)

    # More arguments available - from_date to to_date is max of 31 days.
    def get_auctions(self, corridor, from_date, to_date=None):
        """

        :param corridor:
        :param from_date:
        :param to_date:
        :return:
        """
        response = requests.get(
            url=f'{self.jao_url}getauctions',
            params={'corridor': corridor, 'fromdate': from_date, 'horizon': 'Monthly'},
            headers={'AUTH_API_KEY': self.auth_api_key}
        )
        return json.loads(response.content)

    def get_curtailment(self, corridor, from_date, to_date=None):
        """

        :param corridor:
        :param from_date:
        :param to_date:
        :return:
        """
        response = requests.get(
            url=f'{self.jao_url}getcurtailment',
            params={'corridor': corridor, 'fromdate': from_date},
            headers={'AUTH_API_KEY': self.auth_api_key}
        )
        return json.loads(response.content)

    def get_bids(self, auction_id):
        """

        :param auction_id:
        :return:
        """
        response = requests.get(
            url=f'{self.jao_url}getbids',
            params={'auctionid': auction_id},
            headers={'AUTH_API_KEY': self.auth_api_key}
        )
        return json.loads(response.content)


class CorridorState:
    """
    Corridor State
    """
    def __init__(self, config):
        egress_url = config['Azure Storage']['egress_url']
        self.ingress_url = config['Azure Storage']['ingress_url']
        self.tenant_id = config['Authorization']['tenant_id']
        self.client_id = config['Authorization']['client_id']
        self.client_secret = config['Authorization']['client_secret']
        self.dataset_guid = config['Datasets']['source']

        egress = Egress(egress_url, self.tenant_id, self.client_id, self.client_secret, self.dataset_guid)

        json_content = egress.retrieve_state()
        self.state = json_content['LastUpdates']

        self.default_value = config['JAO Values']['default_date']

    def __str__(self):
        return '\n'.join([str(item) for item in self.state])

    def get_last_successful_monthly_date(self, corridor):
        """

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
        Docstring
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

        :return:
        """
        state = {"LastUpdates": self.state}
        ingress = Ingress(self.ingress_url, self.tenant_id, self.client_id, self.client_secret, self.dataset_guid)
        ingress.save_state(json.dumps(state))


def filter_corridors(corridors, filters):
    """

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


def retrieve_auctions():
    """

    :return:
    """
    current_date = datetime.utcnow()

    config = configparser.ConfigParser()
    config.read(['../conf.ini', '/etc/osiris/conf.ini', '/etc/transform-ingress2event-time-conf.ini'])
    jao_url = config['JAO Server']['server_url']
    auth_api_key = config['JAO Server']['auth_api_key']

    state = CorridorState(config)
    print(state)

    client = JaoClient(jao_url, auth_api_key)

    corridors = client.get_corridors()
    corridors = [corridor['value'] for corridor in corridors]
    corridors = filter_corridors(corridors, ['DK', 'D1', 'D2'])

    # for testing
    # corridors = ['GB-NI', 'GB-IE', 'IF2-FR-GB', 'BE-DE', 'NL-NO', 'CH-FR']

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

                state.set_last_successful_monthly_date(corridor, monthly_datetime_obj.strftime("%Y-%m-%d"))

            monthly_datetime_obj += relativedelta(months=+1)

    print(state)
    state.save_state()


if __name__ == "__main__":

    retrieve_auctions()
