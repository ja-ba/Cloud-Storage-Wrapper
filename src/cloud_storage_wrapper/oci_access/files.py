from cloud_storage_wrapper.oci_access.config import OCI_Connection


class FilesOCI(OCI_Connection):
    """A class providing access to files on an OCI filesystem. Inherits from OCI_Connection to create a connection to OCI."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

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
            ).data.content.decode()  # type: ignore
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
            for chunk in object_data.data.raw.stream(1024 * 1024, decode_content=False):  # type: ignore
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
        ).data.objects  # type: ignore
