import os
from pathlib import Path

import oci
from ocifs import OCIFileSystem


class OCI_Connection:
    """This is a base class creating a connection to OCI file system based on a key provided either via an environment variable,
    a key file on disk or a key which is directly passed to the class.

    The class is a wrapper for easier handling of the OCI SDK: https://pypi.org/project/oci/
    """

    def __init__(
        self,
        user: str,
        fingerprint: str,
        tenancy: str,
        region: str,
        bucket_name: str,
        compartment_id: str,
        env_key: str = "",
        key_file: str = "",
        direct_key: str = "",
    ):
        """Initialized a OCI_Connection object

        Args:
            user (str): A string specifying the user
            fingerprint (str): A string specifying the fingerprint
            tenancy (str): A string specifying the tenancy
            region (str): A string specifying the region
            bucket_name (str): A string specifying the name of the bucket to be used for access and storage
            compartment_id (str): A string specifying the compartment_id
            env_key (str, optional): The name of an accessible environment variable containing the key. Defaults to ''.
            key_file (str, optional): The path to a key file containing the key. Defaults to ''
            direct_key (str, optional): The key directly passed in string format. Defaults to ''

        Raises:
            ValueError: if no key is provided or the key file doesn't exist/has the wrong format

        """
        self.oci_key: str = ""
        self.bucket_name = bucket_name
        self.compartment_id = compartment_id

        if not env_key and not key_file and not direct_key:
            raise ValueError(
                "No key was passed to instaniateOCI(), please provide one of the \
                    arguments env_key, key_file or direct_key"
            )

        if env_key:
            self.oci_key = os.environ.get(env_key, "")

        elif key_file:
            if ".pem" not in key_file:
                raise ValueError("The provided key does not seem to be a .pem file")

            elif not Path(key_file).exists():
                raise ValueError("The provided key file doesn't exist")

            else:
                self.oci_key = Path(key_file).read_text()

        elif direct_key:
            self.oci_key = direct_key

        # Config for OCI
        self.config = {
            "user": user,
            "key_content": self.oci_key,
            "fingerprint": fingerprint,
            "tenancy": tenancy,
            "region": region,
        }
        self.object_storage = oci.object_storage.ObjectStorageClient(self.config)
        self.namespace = self.object_storage.get_namespace().data
        self.fs = OCIFileSystem(config=self.config, profile="DEFAULT")

        # Set bucket_name and namespace as environment vars
        # os.environ['BUCKET_NAME'] = bucket_name
        # os.environ['NAMESPACE'] = namespace
        # os.environ['COMPARTMENT_ID'] = compartment_id

    def retrieve_file_content(self, file_name: str, decode: bool):
        """Method to retrieve file content from a file in OCI cloud storage

        Args:
            file_name (str): The path to the file which should be retrieved
            decode (bool): A flag specifying whether to decode the output (True) or return the object directly

        Returns:
            The decoded or raw file
        """
        if decode:
            return self.object_storage.get_object(
                self.namespace, self.bucket_name, file_name
            ).data.content.decode()
        else:
            return self.object_storage.get_object(
                self.namespace, self.bucket_name, file_name
            )

    def download_file(self, file_name: str, save_as: str) -> None:
        """Method to download a file and write it do disk

        Args:
            file_name (str): The path to the file which should be retrieved
            save_as (str): A path specifying where the file should be saved
        """
        with open(save_as, "wb") as file:
            object_data = self.object_storage.get_object(
                self.namespace, self.bucket_name, file_name
            )
            for chunk in object_data.data.raw.stream(1024 * 1024, decode_content=False):
                file.write(chunk)

    def upload_file(self, file_to_upload: str, file_name: str) -> None:
        """Method to upload a file to OCI cloud storage

        Args:
            file_to_upload (str): The path to the file which should be uploaded
            file_name (str): The name under which the file should be saved after the upload. This can also be a path
        """
        with open(file_to_upload, "rb") as file:
            self.object_storage.put_object(
                self.namespace, self.bucket_name, file_name, file
            )

    def delete_files(self, file_name: str) -> bool:
        """Method to delete a file on OCI cloud storage

        Args:
            file_name (str): The name of the file which should be deleted. This can also be a path

        Returns:
            bool: A flag whether the upload was successful
        """
        try:
            self.object_storage.delete_object(
                self.namespace, self.bucket_name, file_name
            )
            return True
        except Exception:
            return False

    def list_files(self, prefix: str = "") -> list:
        """Method to list files on OCI cloud storage

        Args:
            prefix (str, optional): The optional path in which to list a file. Defaults to "" to list all files in the bucket.

        Returns:
            list: A list of objects found
        """
        return self.object_storage.list_objects(
            self.namespace, self.bucket_name, prefix=prefix
        ).data.objects
