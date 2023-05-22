import os, uuid
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, generate_account_sas, ResourceTypes, AccountSasPermissions
from datetime import datetime, timedelta


class AzureFile:

  def normalize_email(self, s):
      s = s.replace(".","1")
      s = s.replace("@","2")            
      return s

  def get_blob_service_client(self):
    blob_service_client = blob_service_client = BlobServiceClient.from_connection_string(self.cs)
    return blob_service_client

  def blob_upload_file(self, container_name, local_path, local_file_name, email):
    print(container_name)
    print(local_file_name)
    print(local_path)
    service_client = self.get_blob_service_client() 
    container_client = service_client.get_container_client(container_name)
    if not container_client.exists():
      container_client.create_container()
    blob_client = container_client.get_blob_client(local_file_name)
    upload_file_path = os.path.join(local_path, local_file_name)
    metadata = {'uploaded_by':'test', 'status':'UPLOADED'}
    metadata["uploaded_by"] = email
    print(metadata)
    with open(file=upload_file_path, mode="rb") as data:
      result = blob_client.upload_blob(data, overwrite=True, metadata=metadata)
    os.remove(upload_file_path)
    return result

  def blob_download_file(self, container_name, local_path, blob_file_name):
    service_client = self.get_blob_service_client()
    blob_client = service_client.get_blob_client(container=container_name, blob=blob_file_name)
    if not os.path.exists(local_path):
      os.mkdir(local_path)
    with open(file=os.path.join(local_path, blob_file_name), mode="wb") as sample_blob:
      download_stream = blob_client.download_blob()
      sample_blob.write(download_stream.readall())
    return os.path.join(local_path, blob_file_name)

  def blob_list_container(self, container_name):
    service_client = self.get_blob_service_client() 
    container_client = service_client.get_container_client(container_name)
    return container_client.list_blobs(include='metadata')
  
  def blob_delete_file(self, container_name, blob_file_name):
    service_client = self.get_blob_service_client()
    blob_client = service_client.get_blob_client(container=container_name, blob=blob_file_name)
    blob_client.delete_blob(delete_snapshots="include")

  def change_metadata(self, container_name, file_name, key, value):
    service_client = self.get_blob_service_client()
    blob_client = service_client.get_blob_client(container_name, file_name)
    metadata = blob_client.get_blob_properties().metadata
    metadata[key] = value
    blob_client.set_blob_metadata(metadata)

  def delete_local_file(file):  
    os.remove(file)

  def __init__(self, connection_string) -> None:
    self.cs = connection_string
