[![Pylint](https://github.com/Open-Dataplatform/ingress-adapter-jao/actions/workflows/pylint.yml/badge.svg)](https://github.com/Open-Dataplatform/ingress-adapter-jao/actions/workflows/pylint.yml)
[![Bandit](https://github.com/Open-Dataplatform/ingress-adapter-jao/actions/workflows/bandit.yml/badge.svg)](https://github.com/Open-Dataplatform/ingress-adapter-jao/actions/workflows/bandit.yml)
[![Flake8](https://github.com/Open-Dataplatform/ingress-adapter-jao/actions/workflows/flake8.yml/badge.svg)](https://github.com/Open-Dataplatform/ingress-adapter-jao/actions/workflows/flake8.yml)
[![Mypy](https://github.com/Open-Dataplatform/ingress-adapter-jao/actions/workflows/mypy.yml/badge.svg)](https://github.com/Open-Dataplatform/ingress-adapter-jao/actions/workflows/mypy.yml)
[![Pytest](https://github.com/Open-Dataplatform/ingress-adapter-jao/actions/workflows/pytest.yml/badge.svg)](https://github.com/Open-Dataplatform/ingress-adapter-jao/actions/workflows/pytest.yml)

# ingress-adapter-jao
- [Introduction](#introduction)
- [Configuration](#configuration)
  - [Logging](#logging)
- [Development](#development)
  - [tox](#tox)
  - [Commands](#commands)
    - [Linting](#linting)
    - [Tests](#tests)

## Introduction
This adapter calls the JAO end point and ingest it in an "ingress" dataset in regard to their ingested time.

It keeps a state of all the last successful retrieved corridors.

### Usage
```sh
$ python -m ingress_adapter_jao.adapter
```

## Configuration

The application needs a configuration file `conf.ini` (see `conf.example.ini`).
The configuration file must be placed in the root of the project or in the locations `/etc/osiris/conf.ini` or
`/etc/ingress-adapter-jao-conf.ini`.

```
[Logging]
configuration_file = log.conf

[Authorization]
tenant_id = <tenant_id>
client_id = <client_id>
client_secret = <client_secret>

[Azure Storage]
ingress_url = https://<storage_account>.dfs.core.windows.net
filesystem_name = <container_name>

[Datasets]
source = <source_guid>

[JAO Server]
server_url = https://api.jao.eu/OWSMP
auth_api_key = <auth_api_key>

[JAO Values]
default_date = 2020-01-01
```

### Logging
Logging can be controlled by defining handlers and formatters using [Logging Configuration](https://docs.python.org/3/library/logging.config.html) and specifically the [config fileformat](https://docs.python.org/3/library/logging.config.html#logging-config-fileformat).
The location of the log configuration file (`Logging.configuration_file`) must be defined in the configuration file of the application as mentioned above.

Here is an example configuration:
```
[loggers]
keys=root

[handlers]
keys=consoleHandler,fileHandler

[formatters]
keys=fileFormatter,consoleFormatter

[logger_root]
level=ERROR
handlers=consoleHandler

[handler_consoleHandler]
class=StreamHandler
formatter=consoleFormatter
args=(sys.stdout,)

[handler_fileHandler]
class=FileHandler
formatter=fileFormatter
args=('logfile.log',)

[formatter_fileFormatter]
format=%(asctime)s - %(name)s - %(levelname)s - %(message)s
datefmt=

[formatter_consoleFormatter]
format=%(levelname)s: %(name)s - %(message)s
```

#### Grant access to the dataset
The application must be granted read access to the ingress dataset and write-access to the egress dataset on
[the Data Platform](https://dataplatform.energinet.dk/).

Add the application you created earlier, using the `<YOUR APP NAME>` name, to the read- and write-access lists.


## Development

### tox

Development for this project relies on [tox](https://tox.readthedocs.io/).

Make sure to have it installed.

### Commands

If you want to run all commands in tox.ini

```sh
$ tox
```

#### Linting

You can also run a single linter specified in tox.ini. For example:

```sh
$ tox -e flake8
```


#### Tests

(No test at the moment)

Run unit tests.

```sh
$ tox -e py3
```

Run a specific testcase.

```sh
$ tox -e py3 -- -x tests/test_main.py
```