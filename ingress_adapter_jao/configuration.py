"""
Contains the configuration.
"""
import configparser
from logging import Logger
import logging.config
from osiris.apis.ingress import Ingress


class Configuration:
    """
    Class used to only parse the configuration file once.
    """
    def __init__(self, name: str):
        self.name = name
        self.config = configparser.ConfigParser()
        self.config.read(['conf.ini', '/etc/osiris/conf.ini', '/etc/ingress-adapter-jao-conf.ini'])
        self.jao_url = self.config['JAO Server']['server_url']
        self.auth_api_key = self.config['JAO Server']['auth_api_key']

        ingress_url = self.config['Azure Storage']['ingress_url']
        tenant_id = self.config['Authorization']['tenant_id']
        client_id = self.config['Authorization']['client_id']
        client_secret = self.config['Authorization']['client_secret']
        dataset_guid = self.config['Datasets']['source']
        self.default_value = self.config['JAO Values']['default_date']

        self.ingress = Ingress(ingress_url, tenant_id, client_id, client_secret, dataset_guid)
        logging.config.fileConfig(fname=self.config['Logging']['configuration_file'], disable_existing_loggers=False)

    def get_ingress(self):
        """
        Returns the Ingress
        :return:
        """
        return self.ingress

    def get_logger(self) -> Logger:
        """
        A customized logger.
        """
        return logging.getLogger(self.name)

    def get_config(self):
        """
        The configuration for the application.
        """
        return self.config
