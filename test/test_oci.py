import os
from pathlib import Path

import pandas as pd
import pytest
from cloud_storage_wrapper.oci_access.config import OCI_Connection

os.environ["SAMPLE_KEY"] = Path("test/example_key.txt").read_text()
my_key_string = Path("test/example_key.txt").read_text()


class Test_config:
    def test_config_key_file(self):
        testConfig = OCI_Connection(
            user="ocid1.user.oc1..aaaaaaaabn5z4iq7l3j2vpohtwrew2vo46txvh6jxmppkm7l47j6rwqzjvnq",
            fingerprint="6b:29:11:28:b4:01:14:58:4d:5f:41:b9:de:92:43:54",
            tenancy="ocid1.tenancy.oc1..aaaaaaaavqyu7qyfu4ysj4g47phqalghd6vupj6gqckxcuf3cmxqe6dr2wpa",
            region="eu-frankfurt-1",
            bucket_name="my_test_bucket",
            compartment_id="jakobsdataprojects",
            key_file="test/example_key.pem",
        )

        assert testConfig

    def test_config_key_str(self):
        testConfig = OCI_Connection(
            user="ocid1.user.oc1..aaaaaaaabn5z4iq7l3j2vpohtwrew2vo46txvh6jxmppkm7l47j6rwqzjvnq",
            fingerprint="6b:29:11:28:b4:01:14:58:4d:5f:41:b9:de:92:43:54",
            tenancy="ocid1.tenancy.oc1..aaaaaaaavqyu7qyfu4ysj4g47phqalghd6vupj6gqckxcuf3cmxqe6dr2wpa",
            region="eu-frankfurt-1",
            bucket_name="my_test_bucket",
            compartment_id="jakobsdataprojects",
            direct_key=my_key_string,
        )

        assert testConfig

    def test_config_key_env(self):
        testConfig = OCI_Connection(
            user="ocid1.user.oc1..aaaaaaaabn5z4iq7l3j2vpohtwrew2vo46txvh6jxmppkm7l47j6rwqzjvnq",
            fingerprint="6b:29:11:28:b4:01:14:58:4d:5f:41:b9:de:92:43:54",
            tenancy="ocid1.tenancy.oc1..aaaaaaaavqyu7qyfu4ysj4g47phqalghd6vupj6gqckxcuf3cmxqe6dr2wpa",
            region="eu-frankfurt-1",
            bucket_name="my_test_bucket",
            compartment_id="jakobsdataprojects",
            env_key="SAMPLE_KEY",
        )

        assert testConfig


test_file_name = "my_tests/test_df"


class Test_PandasOCI:
    def test_pandas_upload_csv(self, load_sampled_iris, create_PandasOCI):
        temp_df = load_sampled_iris
        myPandasOCI = create_PandasOCI
        format = "csv"

        myPandasOCI.write_df(
            df=temp_df, path=f"{test_file_name}.{format}", df_format=format
        )

    def test_pandas_download_csv(self, load_sampled_iris, create_PandasOCI):
        temp_df = load_sampled_iris
        myPandasOCI = create_PandasOCI
        format = "csv"

        temp_df_download = myPandasOCI.retrieve_df(
            path=f"{test_file_name}.{format}", df_format=format
        )
        temp_df = temp_df.astype(temp_df_download.dtypes)
        assert temp_df.equals(temp_df_download)

    def test_pandas_download_csv_columns(self, load_sampled_iris, create_PandasOCI):
        temp_df = load_sampled_iris
        cols = list(temp_df.columns)[:3]
        temp_df = temp_df[cols]
        myPandasOCI = create_PandasOCI
        format = "csv"

        temp_df_download = myPandasOCI.retrieve_df(
            path=f"{test_file_name}.{format}", df_format=format, columns=cols
        )
        temp_df = temp_df.astype(temp_df_download.dtypes)

        assert temp_df.equals(temp_df_download)

    def test_pandas_upload_parquet(self, load_sampled_iris, create_PandasOCI):
        temp_df = load_sampled_iris
        myPandasOCI = create_PandasOCI
        format = "parquet"

        myPandasOCI.write_df(
            df=temp_df, path=f"{test_file_name}.{format}", df_format=format
        )

    def test_pandas_download_parquet(self, load_sampled_iris, create_PandasOCI):
        temp_df = load_sampled_iris
        myPandasOCI = create_PandasOCI
        format = "parquet"

        temp_df_download = myPandasOCI.retrieve_df(
            path=f"{test_file_name}.{format}", df_format=format
        )
        temp_df = temp_df.astype(temp_df_download.dtypes)

        assert temp_df.equals(temp_df_download)

    def test_pandas_download_parquet_columns(self, load_sampled_iris, create_PandasOCI):
        temp_df = load_sampled_iris
        cols = list(temp_df.columns)[:3]
        temp_df = temp_df[cols]
        myPandasOCI = create_PandasOCI
        format = "parquet"

        temp_df_download = myPandasOCI.retrieve_df(
            path=f"{test_file_name}.{format}", df_format=format, columns=cols
        )
        temp_df = temp_df.astype(temp_df_download.dtypes)

        assert temp_df[cols].equals(temp_df_download)

    def test_pandas_upload_feather(self, load_sampled_iris, create_PandasOCI):
        temp_df = load_sampled_iris
        myPandasOCI = create_PandasOCI
        format = "ftr"

        myPandasOCI.write_df(
            df=temp_df, path=f"{test_file_name}.{format}", df_format=format
        )

    def test_pandas_download_feather(self, load_sampled_iris, create_PandasOCI):
        temp_df = load_sampled_iris
        myPandasOCI = create_PandasOCI
        format = "ftr"

        temp_df_download = myPandasOCI.retrieve_df(
            path=f"{test_file_name}.{format}", df_format=format
        )
        temp_df = temp_df.astype(temp_df_download.dtypes)

        assert temp_df.equals(temp_df_download)

    def test_pandas_download_feather_columns(self, load_sampled_iris, create_PandasOCI):
        temp_df = load_sampled_iris
        cols = list(temp_df.columns)[:3]
        temp_df = temp_df[cols]
        myPandasOCI = create_PandasOCI
        format = "ftr"

        temp_df_download = myPandasOCI.retrieve_df(
            path=f"{test_file_name}.{format}", df_format=format, columns=cols
        )
        temp_df = temp_df.astype(temp_df_download.dtypes)

        assert temp_df[cols].equals(temp_df_download)

    def test_wrong_formats(self, create_PandasOCI, load_sampled_iris):
        with pytest.raises(ValueError):
            format = "foo"
            create_PandasOCI.retrieve_df(
                path=f"{test_file_name}.{format}", df_format=format
            )
        with pytest.raises(ValueError):
            create_PandasOCI.write_df(
                df=load_sampled_iris, path=f"{test_file_name}.{format}", df_format="foo"
            )

    def test_empty_df_error(self, create_PandasOCI):
        format = "csv"
        with pytest.raises(ValueError):
            create_PandasOCI.write_df(
                df=pd.DataFrame(), path=f"{test_file_name}.{format}", df_format=format
            )


class TestOCI_Pandas_manual:
    def test_pandas_upload_csv(self, load_sampled_iris, create_OCI_config):
        temp_df = load_sampled_iris
        test_config = create_OCI_config

        temp_df.to_csv(
            f"oci://{test_config.bucket_name}@{test_config.namespace}/{test_file_name}.csv",
            storage_options={"config": test_config.config},
            index=False,
        )

    def test_pandas_download_csv(self, load_sampled_iris, create_OCI_config):
        temp_df = load_sampled_iris
        test_config = create_OCI_config

        temp_df_download = pd.read_csv(
            f"oci://{test_config.bucket_name}@{test_config.namespace}/{test_file_name}.csv",
            storage_options={"config": test_config.config},
        )

        assert temp_df.equals(temp_df_download)

    def test_pandas_upload_parquet(self, load_sampled_iris, create_OCI_config):
        temp_df = load_sampled_iris
        test_config = create_OCI_config

        temp_df.to_parquet(
            f"oci://{test_config.bucket_name}@{test_config.namespace}/{test_file_name}.parquet",
            storage_options={"config": test_config.config},
        )

    def test_pandas_download_parquet(self, load_sampled_iris, create_OCI_config):
        temp_df = load_sampled_iris
        test_config = create_OCI_config

        temp_df_download = pd.read_parquet(
            f"oci://{test_config.bucket_name}@{test_config.namespace}/{test_file_name}.parquet",
            storage_options={"config": test_config.config},
        )

        assert temp_df.equals(temp_df_download)

    def test_pandas_upload_feather(self, load_sampled_iris, create_OCI_config):
        temp_df = load_sampled_iris
        test_config = create_OCI_config

        temp_df.to_feather(
            f"oci://{test_config.bucket_name}@{test_config.namespace}/{test_file_name}.ftr",
            storage_options={"config": test_config.config},
        )

    def test_pandas_download_feather(self, load_sampled_iris, create_OCI_config):
        temp_df = load_sampled_iris
        test_config = create_OCI_config

        temp_df_download = pd.read_feather(
            f"oci://{test_config.bucket_name}@{test_config.namespace}/{test_file_name}.ftr",
            storage_options={"config": test_config.config},
        )

        assert temp_df.equals(temp_df_download)


class TestOCI_Files:
    def test_list_files(self, create_OCI_config):
        test_config = create_OCI_config

        existing_files_list = test_config.list_files(prefix="my_tests/")

        assert len(existing_files_list) == 4

    def test_delete_files(self, create_OCI_config):
        test_config = create_OCI_config
        existing_files_list = test_config.list_files(prefix="my_tests/")

        for file in existing_files_list:
            test_config.delete_files(file.name)

        existing_files_list_after_del = test_config.list_files(prefix="my_tests/")
        assert len(existing_files_list_after_del) == 0

    def test_upload_file(self, create_OCI_config, delete_all):
        test_config = create_OCI_config

        # Call delete_all
        _dummy = delete_all

        test_config.upload_file(
            file_to_upload="test/example_file.txt",
            file_name="my_tests/example_file.txt",
        )

        existing_files_list = test_config.list_files(prefix="my_tests/")

        assert existing_files_list[0].name == "my_tests/example_file.txt"

    def test_retrieve_file(self, create_OCI_config):
        test_config = create_OCI_config

        content_original = test_config.retrieve_file_content(
            file_name="my_tests/example_file.txt", decode=False
        )
        content_decoded = test_config.retrieve_file_content(
            file_name="my_tests/example_file.txt", decode=True
        )

        assert content_original.data.content.decode() == "XXX-Jakob-XXX"
        assert content_decoded == "XXX-Jakob-XXX"

    def test_download_file(self, create_OCI_config):
        test_config = create_OCI_config

        save_path = "test/example_file_downloaded.txt"
        test_config.download_file(
            file_name="my_tests/example_file.txt", save_as=save_path
        )

        file_content = Path(save_path).read_text()
        Path(save_path).unlink()
        assert file_content == "XXX-Jakob-XXX"
