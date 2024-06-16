import pytest
import seaborn as sns
from cloud_storage_wrapper.oci_access.config import create_OCI_Connection_from_dict
from cloud_storage_wrapper.oci_access.config import OCI_Connection
from cloud_storage_wrapper.oci_access.pandas import create_PandasOCI_from_dict
from cloud_storage_wrapper.oci_access.pandas import PandasOCI

auth_dict = {
    "oci_config": {
        "user": "ocid1.user.oc1..aaaaaaaabn5z4iq7l3j2vpohtwrew2vo46txvh6jxmppkm7l47j6rwqzjvnq",
        "fingerprint": "6b:29:11:28:b4:01:14:58:4d:5f:41:b9:de:92:43:54",
        "tenancy": "ocid1.tenancy.oc1..aaaaaaaavqyu7qyfu4ysj4g47phqalghd6vupj6gqckxcuf3cmxqe6dr2wpa",
        "region": "eu-frankfurt-1",
        "bucket_name": "my_test_bucket",
        "compartment_id": "jakobsdataprojects",
        "key_file": "test/example_key.pem",
    }
}


@pytest.fixture(scope="session")
def load_sampled_iris():
    iris_data = sns.load_dataset("iris")
    iris_data_sampled = iris_data.sample(n=10, random_state=1).reset_index(drop=True)
    return iris_data_sampled


@pytest.fixture(scope="session")
def create_PandasOCI() -> PandasOCI:
    return create_PandasOCI_from_dict(configDict=auth_dict)


@pytest.fixture(scope="session")
def create_OCI_config() -> OCI_Connection:
    return create_OCI_Connection_from_dict(configDict=auth_dict)


@pytest.fixture
def delete_all(create_OCI_config):
    test_config = create_OCI_config
    existing_files_list = test_config.list_files(prefix="my_tests/")

    for file in existing_files_list:
        test_config.delete_files(file.name)
