import pandas as pd
from cloud_storage_wrapper.oci_access.config import OCI_Connection


class PandasOCI(OCI_Connection):
    """A class providing pandas functionality with an OCI filesystem. Inherits from OCI_Connection to create a connection to OCI."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def retrieve_df(self, file_name: str, type: str) -> pd.DataFrame:
        """Retrieves files in the

        Args:
            file_name (str): _description_
            type (str): _description_

        Raises:
            ValueError: _description_

        Returns:
            pd.DataFrame: _description_
        """
        if type == "ftr":
            return pd.read_feather(
                f"oci://{self.bucket_name}@{self.namespace}/{file_name}",
                storage_options={"config": self.config},
            )
        elif type == "parquet":
            return pd.read_parquet(
                f"oci://{self.bucket_name}@{self.namespace}/{file_name}",
                storage_options={"config": self.config},
            )
        elif type == "csv":
            return pd.read_csv(
                f"oci://{self.bucket_name}@{self.namespace}/{file_name}",
                storage_options={"config": self.config},
            )
        else:
            raise ValueError("Type must be one of 'ftr', 'parquet' or 'csv'")

    def write_df(self, df: pd.DataFrame, file_name: str, type: str) -> None:
        if not file_name:
            raise ValueError("Please specify a file_name")

        if type == "ftr":
            df.to_feather(
                f"oci://{self.bucket_name}@{self.namespace}/{file_name}",
                storage_options={"config": self.config},
            )
        elif type == "parquet":
            df.to_parquet(
                f"oci://{self.bucket_name}@{self.namespace}/{file_name}",
                storage_options={"config": self.config},
            )
        elif type == "csv":
            df.to_csv(
                f"oci://{self.bucket_name}@{self.namespace}/{file_name}",
                storage_options={"config": self.config},
                index=False,
            )
        else:
            raise ValueError("Type must be one of 'ftr', 'parquet' or 'csv'")
