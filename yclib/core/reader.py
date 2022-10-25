import pandas as pd
import numpy as np
import pathlib
from typing import Optional
import os
import io
import msoffcrypto


class FileFilter:
    """methods under this class should only return a list of file paths

    Returns
    -------
    _type_
        _description_
    """

    @staticmethod
    def get_filepath_list(folder_path: str) -> list[pathlib.Path]:
        """get a list of excel files

        Parameters
        ----------
        folder_path : str
            a folder path that the function will walk through

        Returns
        -------
        list[pathlib.Path]
            a list of excel file paths
        """
        folder_path = pathlib.Path(folder_path)
        return [
            pathlib.Path(os.path.join(root, file))
            for root, dirs, files in os.walk(folder_path)
            for file in files
            if pathlib.Path(file).suffix.lower()
            in [".xlsx", ".xls", ".xlsm", ".txt", ".csv", ".xlsb", ".pkl"]
        ]

    @staticmethod
    def filelist_filter(
        filepath_list: list[pathlib.Path] or list[str],
        filename_include: Optional[list[str]] = None,
        filename_exclude: Optional[list[str]] = None,
        filepath_exclude: list[str] = ["archive", "delete"],
    ) -> list[pathlib.Path]:
        """filter file name and path in a list of file paths

        Parameters
        ----------
        filepath_list : list[pathlib.Path] or list[str]
            a list of file paths in either pathlib.Path or str
        filename_include : Optional[list[str]], optional
            a list of included keywords in file name, by default None
        filename_exclude : Optional[list[str]], optional
            a list of excluded keywords in file name, by default None
        filepath_exclude : list[str], optional
            a list of excluded keywords in file path, by default ["archive", "delete"]

        Returns
        -------
        list[pathlib.Path]
            a list of file paths after filtering
        """
        filepath_list = [
            filepath if isinstance(filepath, pathlib.Path) else pathlib.Path(filepath)
            for filepath in filepath_list
        ]

        def _is_included(filepath: pathlib.Path, kws: Optional[str] = None) -> bool:
            return all(kw in filepath.name for kw in kws if kw) if kws else True

        def _is_not_excluded(filepath: pathlib.Path, kws: Optional[str] = None) -> bool:
            return all(kw not in filepath.name for kw in kws if kw) if kws else True

        def _is_not_ignored(filepath: pathlib.Path, kws: Optional[str] = None) -> bool:
            return (
                all(kw.lower() not in str(filepath.parent).lower() for kw in kws)
                if kws
                else True
            )

        return [
            filepath
            for filepath in filepath_list
            if (
                _is_included(filepath, filename_include)
                and _is_not_excluded(filepath, filename_exclude)
                and _is_not_ignored(filepath, filepath_exclude)
            )
        ]

    @staticmethod
    def absolute_filepath_list(
        file_path_list: list[str],
    ) -> list[pathlib.Path]:
        """a simple function for the yaml file for the case when a absolute list of paths should be used in the import stage

        Parameters
        ----------
        file_path_list : list[str]
            a list of file path with the format of string format

        Returns
        -------
        list[pathlib.Path]
            a list of file path with the format of pathlib.Path format
        """
        return [pathlib.Path(filepath) for filepath in file_path_list]


class ExcelFileHandler:
    @staticmethod
    def read_file(filepath: pathlib.Path, pandas_attributes) -> dict[str, pd.DataFrame]:
        """read a file path to a dict with file name as key and pandas dataframe as value

        Parameters
        ----------
        filepath : pathlib.Path
            file path
        **kwargs : variables
            pandas attributes
        Returns
        -------
        dict[str, pd.DataFrame]
            without sheet=None: file name as key and pandas dataframe as value
            excel with multiple sheets(with sheet=None): file name+'_sheet:'+sheet name as key and pandas dataframe as value
        """
        filepath = (
            filepath if isinstance(filepath, pathlib.Path) else pathlib.Path(filepath)
        )
        pd_filetype_mapper = {
            "excel": [".xlsx", ".xls", ".xlsm", ".xlsb"],
            "csv": [".txt", ".csv"],
            "pkl": ["pkl"],
        }
        pd_func_mapper = {
            "excel": pd.read_excel,
            "csv": pd.read_csv,
            "pkl": pd.read_pickle,
        }

        df = [
            pd_func_mapper[key](filepath, **pandas_attributes)
            for key, val in pd_filetype_mapper.items()
            if filepath.suffix.lower() in val
        ][0]

        if isinstance(df, pd.DataFrame):
            df["Source"] = (
                filepath.stem
                + ":row:"
                + (
                    df.index
                    + 2
                    + int(pandas_attributes.get("skiprows", 0))
                    + int(pandas_attributes.get("header", 0))
                ).astype("string")
            )

            df_dict = {filepath.stem: df}
        else:
            for sheet in df:
                df[sheet]["Source"] = (
                    filepath.stem
                    + sheet
                    + ":row:"
                    + (
                        df[sheet].index
                        + 2
                        + int(pandas_attributes.get("skiprows", 0))
                        + int(pandas_attributes.get("header", 0))
                    ).astype("string")
                )
            df_dict = {"_".join([filepath.stem, key]): val for key, val in df.items()}
        return df_dict

    @staticmethod
    def save_dict_to_folder(target: dict[str, pd.DataFrame], folder_name: pathlib.Path):
        folder_name = (
            folder_name
            if isinstance(folder_name, pathlib.Path)
            else pathlib.Path(folder_name)
        )

        for key, val in target.items():
            filename = pathlib.Path(pathlib.Path(key).stem + ".pkl")
            val.to_pickle(pathlib.PurePath(folder_name, filename))

    @staticmethod
    def define_datatype(
        df: pd.DataFrame, column_dtype_dict: Optional[dict[str, str]] = None
    ) -> pd.DataFrame:
        if column_dtype_dict is not None:
            df = df.astype(column_dtype_dict)
            if df.dtypes.astype(str).to_dict() != column_dtype_dict:
                set1 = set(df.dtypes.astype(str).to_dict().items())
                set2 = set(column_dtype_dict.items())
                raise ValueError(
                    f"Columns {str(set1 ^ set2)} failed to change datatypes"
                )
        return df

    @property
    def inspect_datatype(df: pd.DataFrame) -> dict[str, str]:
        return df.dtypes.astype("str").to_dict()

    @staticmethod
    def decrypted_file(file_path: pathlib.Path, password: str or int) -> io.BytesIO:
        decrypted_workbook = io.BytesIO()
        with open(
            file_path,
            "rb",
        ) as file:
            office_file = msoffcrypto.OfficeFile(file)
            office_file.load_key(password=password)
            office_file.decrypt(decrypted_workbook)
        return decrypted_workbook
