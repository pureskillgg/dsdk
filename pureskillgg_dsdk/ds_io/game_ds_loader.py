from typing import TypedDict, List, Dict, Optional, Any, Callable, Union
from collections.abc import Iterable
import structlog

import pandas as pd

from .reader_fs import DsReaderFs
from .reader_s3 import DsReaderS3


class ChannelInstruction(TypedDict):
    channel: str
    columns: Optional[List[str]]


class GameDsLoader:
    def __init__(self, *, reader: Union[DsReaderFs, DsReaderS3], log: object = None):
        self._reader = reader
        self._log = log if log is not None else structlog.get_logger()
        self._metadata = None
        self._manifest = None

    @property
    def manifest(self):
        """Root channel data"""
        self._load()
        return self._manifest

    @property
    def metadata(self):
        """Metadata"""
        self._load()
        return self._metadata

    def _load(self) -> None:
        is_loaded = self._manifest is not None and self._metadata is not None
        if is_loaded:
            return
        self._manifest = self._reader.read_manifest()
        self._metadata = self._reader.read_metadata()

    def get_channels(
        self, channel_instructions: Optional[List[ChannelInstruction]] = None
    ) -> Dict[str, pd.DataFrame]:
        """Read in data from a ds object

        Args:
            channel_instructions (dict, optional): List that contains instructions
                on how to read the channel. Each element must contain a key
                called `channel` and optionally have a key named `columns`
                which is a List of columns to be read in. Not including the
                `columns` key implies you want to read in all columns.
                Omitting this keyword reads all channels and columns


        Returns:
            Data (dict): dictionary where each key is the channel name and the value
                is a Pandas data frame.


        Examples:
            loader = GameDsLoader(reader=reader, log=log)

            # read all channels
            data = loader.get_channels()

            # read one specific channel
            data = loader.get_channels([{"channel":"ch1"}])

            # read two channels
            data = loader.get_channels([
                {"channel":"ch1"},{"channel":"ch2"}
            ])

            # read channels with specific columns
            data = loader.get_channels([
                {"channel":"ch1","columns":["col1","col3"]}
            ])
        """
        self._load()

        instructions = self._normalize_channel_instructions(channel_instructions)

        output = {}
        for instruction in instructions:
            dataframe = self.get_channel(instruction)
            output[instruction["channel"]] = dataframe
        return output

    def get_channel(self, instruction: ChannelInstruction) -> (str, pd.DataFrame):
        """Read in one channel from a ds object

        Args:
            instruction (dict): dictionary containing a key called
                `channel` with the value is the name of the channel
                to be read in. optionally have a key named `columns`
                which is a List of columns to be read in.


        Returns:
            Pandas data frame containing the data you requested and the channel name.

        """
        self._load()

        channel = self._find_channel_by_name(instruction["channel"])

        content_type = channel["contentType"]
        if content_type == "application/x-parquet":
            dataframe = self._reader.read_parquet_channel(
                channel, columns=instruction.get("columns")
            )
        else:
            raise Exception(f"Unsupported content type {content_type}")

        return dataframe

    def _find_channel_by_name(self, channel_name: str, /) -> Dict:
        channel = first_true(
            self._manifest["channels"],
            None,
            lambda channel: channel["channel"] == channel_name,
        )
        if channel is None:
            raise Exception(f"Channel {channel_name} not found in replay.")
        return channel

    def _normalize_channel_instructions(
        self, channel_instructions: List[ChannelInstruction]
    ) -> List[ChannelInstruction]:
        if channel_instructions is not None:
            return channel_instructions
        instructions = []
        for channel in self._manifest["channels"]:
            single_instruction: ChannelInstruction = {"channel": channel["channel"]}
            instructions.append(single_instruction)
        return instructions


def first_true(
    iterable: Iterable, /, default: Any = False, pred: Optional[Callable] = None
) -> Any:
    return next(filter(pred, iterable), default)
