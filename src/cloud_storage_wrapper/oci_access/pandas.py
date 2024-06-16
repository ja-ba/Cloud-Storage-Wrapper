import pandas as pd
from cloud_storage_wrapper.oci_access.config import OCI_Config_Base
from cloud_storage_wrapper.oci_access.config import OCI_Connection


class PandasOCI(OCI_Connection):
    """A class providing pandas functionality with an OCI filesystem. Inherits from OCI_Connection to create a connection to OCI."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def retrieve_df(
        self, path: str, df_format: str, columns: list = []
    ) -> pd.DataFrame:
        """Retries a data file from the OCI filesystem and loads it as a parquet file.

        Args:
            path (str): The path to the data file.
            df_format (str): The format of the data file.
            columns (list[str], optional): A list of columns to load. Defaults to [].

        Raises:
            ValueError: If a df_format other than 'ftr', 'parquet' or 'csv' is provided.

        Returns:
            pd.DataFrame: The loaded pandas dataframe.
        """

        ref_path = f"oci://{self.bucket_name}@{self.namespace}/{path}"

        if df_format == "ftr":
            if columns:
                return pd.read_feather(
                    ref_path,
                    columns=columns,
                    storage_options={"config": self.config},
                    dtype_backend="pyarrow",
                )
            else:
                return pd.read_feather(
                    ref_path,
                    storage_options={"config": self.config},
                    dtype_backend="pyarrow",
                )

        elif df_format == "parquet":
            if columns:
                return pd.read_parquet(
                    ref_path,
                    columns=columns,
                    storage_options={"config": self.config},
                    dtype_backend="pyarrow",
                    engine="pyarrow",
                )
            else:
                return pd.read_parquet(
                    ref_path,
                    storage_options={"config": self.config},
                    dtype_backend="pyarrow",
                    engine="pyarrow",
                )

        elif df_format == "csv":
            if columns:
                return pd.read_csv(
                    ref_path,
                    usecols=columns,
                    storage_options={"config": self.config},
                    dtype_backend="pyarrow",
                    engine="pyarrow",
                )
            else:
                return pd.read_csv(
                    ref_path,
                    storage_options={"config": self.config},
                    dtype_backend="pyarrow",
                    engine="pyarrow",
                )

        else:
            raise ValueError(
                f"{df_format} is an invalid df_format, please use one of the following: 'csv', 'parquet' or 'feather'"
            )

    def write_df(
        self, df: pd.DataFrame, path: str, df_format: str, chunk_size: int = 5000
    ) -> None:
        """Writes a pandas dataframe to a file on the OCI filesystem.

        Args:
            df (pd.DataFrame): The pandas DataFrame to write to the cloud storage.
            path (str): The path to which the data file should be written.
            df_format (str): The data format in which the data file should be written.
            chunk_size (int, optional): A chunk size by which to write feather files. Defaults to 5000.


        Raises:
            ValueError: If the path is empty.
            ValueError: If a df_format other than 'ftr', 'parquet' or 'csv' is provided.
            ValueError: If an empty df is passed.
        """
        if not path:
            raise ValueError("Please specify a path")
        if len(df) == 0:
            raise ValueError(f"df which should be written to {path} is empty")

        ref_path = f"oci://{self.bucket_name}@{self.namespace}/{path}"

        if df_format == "ftr":
            df.to_feather(
                ref_path, storage_options={"config": self.config}, chunksize=chunk_size
            )
        elif df_format == "parquet":
            df.to_parquet(
                ref_path,
                storage_options={"config": self.config},
                engine="pyarrow",
                compression="snappy",
            )
        elif df_format == "csv":
            df.to_csv(
                ref_path,
                storage_options={"config": self.config},
                index=False,
            )
        else:
            raise ValueError("df_format must be one of 'ftr', 'parquet' or 'csv'")


def create_PandasOCI_from_dict(configDict: dict) -> PandasOCI:
    """A function creating an OCI_Connection object based on a config dict

    Args:
        configDict (dict): a config dict containing the

    Returns:
        OCI_Connection: Returns the OCI Connection object
    """
    if "oci_config" not in configDict:
        raise ValueError("passed dictionary does not contain the key 'oci_config'")

    # Validate against OCI_Config_Base
    configDict_Base = OCI_Config_Base(**configDict["oci_config"]).model_dump()

    # Return the created config
    return PandasOCI(**configDict_Base)
