import yaml
import toml
import psutil
import multiprocessing
from pathlib import Path


def keys_exists(element, keys):
    """
    Check if *keys (nested) exists in `element` (dict).
    """
    _element = element
    for key in keys:
        try:
            _element = _element[key]
        except KeyError:
            return False
    return True


# load project initial settings file
try:
    with open(Path(__file__).parent / "settings.toml") as fd:
        _settings = fd.read()
    _settings = toml.loads(_settings)
except FileNotFoundError:
    print(
        """
        please create a file 'settings.toml' in the yclib root folder with the following structure:

        [pipeline]
        PIPELINE_NAME= {{project name}} <- optional default: new_project
        CONCURRENCY_LIMIT= {{number of cpu will be used in multiprocessing}} <- optional default: maximum cpu


        [postgres]
        USER= {{user}}
        PASSWORD= {{password}}
        HOST= {{host}}
        PORT= {{port}}


        [mainflow]
        INGEST_SOURCE_DATA = {{yaml path}}
        RAW_DATASETS = {{yaml path}}
        EXCEPTION_TESTS = {{yaml path}}
        META_DATASETS = {{yaml path}}

        # optional
        [subflow]
            [subflow.victoria]
            EXTRACT= {{yaml path}}
            TRANSFORM= {{yaml path}}
            LOAD= {{yaml path}}
            
        """
    )

# yamls
if keys_exists(_settings, ["mainflow"]):
    for key, val in _settings["mainflow"].items():
        try:
            with open(val) as f:
                locals().update({key: yaml.load(f, Loader=yaml.CLoader)})
        except KeyError:
            raise KeyError(f"cannot find yaml file for {key}, please check {val}")

# project name
if keys_exists(_settings, ["pipeline", "PROJECT_NAME"]):
    PROJECT_NAME = _settings["pipeline"]["PROJECT_NAME"]
else:
    PROJECT_NAME = "default_project"
    print(
        "PROJECT_NAME is default to 'default_project'. Set a pipeline->PROJECT_NAME if you'd like to name your project"
    )

try:
    # postgres creds
    POSTGRES_CREDENTIAL = {
        k: v
        for k, v in {
            "user": _settings["postgres"]["USER"],
            "password": _settings["postgres"]["PASSWORD"],
            "host": _settings["postgres"]["HOST"],
            "port": _settings["postgres"]["PORT"],
            "dbname": "postgres",
        }.items()
        if v is not None
    }
except KeyError:
    print(
        """
        please set postgres config in 'setttings.toml' with the following format if you'd like to use postgres:

        [postgres]
        USER= {{user}}
        PASSWORD= {{password}}
        HOST= {{host}}
        PORT= {{port}}
    """
    )

# memory usage
try:
    AVAILABLE_MEMORY = dict(psutil.virtual_memory()._asdict())["free"]
except KeyError:
    AVAILABLE_MEMORY = 400000000
    print(f"cannot get free AVAILABLE_MEMORY. AVAILABLE_MEMORY is set to default 4GB")


# CONCURRENCY_LIMIT
if keys_exists(_settings, ["pipeline", "CONCURRENCY_LIMIT"]):
    if _settings["pipeline"]["CONCURRENCY_LIMIT"] <= multiprocessing.cpu_count():
        CONCURRENCY_LIMIT = _settings["pipeline"]["CONCURRENCY_LIMIT"]
    else:
        CONCURRENCY_LIMIT = multiprocessing.cpu_count()
        print(
            f"CONCURRENCY_LIMIT is set to maximum {multiprocessing.cpu_count()} as the value set exceeds the maximum core"
        )
else:
    CONCURRENCY_LIMIT = multiprocessing.cpu_count()
    print(
        """
        CONCURRENCY_LIMIT is set to maximum {cpu}.
        Please include the following settings in settings.toml if you'd like to have a lower concurrency limit

        [cpu]
        core = {{number of cpu will be used in multiprocessing}}
        """.format(
            cpu=multiprocessing.cpu_count()
        )
    )

if keys_exists(_settings, ["subflow"]):
    for key, val in _settings["subflow"].items():
        try:
            with open(val) as f:
                locals().update({key: yaml.load(f, Loader=yaml.CLoader)})
        except KeyError:
            raise KeyError(f"cannot find yaml file for {key}, please check {val}")
