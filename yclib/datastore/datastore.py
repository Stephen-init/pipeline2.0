from abc import abstractmethod, abstractproperty
from collections.abc import Iterator
from contextlib import contextmanager
from enum import Enum
import pandas as pd


class ConnectionStatus(Enum):
    CONNECTED = 1
    DISCONNECTED = 0


class NotConnectedError(Exception):
    """Raised when attempting to perform an action that required a connection,
    but was disconnected"""


class DataStore:
    """Protocol for connecting to a generic data store"""

    @abstractmethod
    @contextmanager
    def connect(self) -> Iterator[None]:
        """Establish a context-managed connection - disconnecting after leaving
        the context"""
        ...

    @abstractmethod
    def get_query_results(self, query: str) -> pd.DataFrame:
        """Runs the query, returning results as a dataframe"""
        ...

    @abstractproperty
    def status(self) -> ConnectionStatus:
        """Returns the status of the connection"""
        ...
