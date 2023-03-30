import os, uuid
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, generate_account_sas, ResourceTypes, AccountSasPermissions
from datetime import datetime, timedelta

def normalize_email(s):
    s = s.replace(".","1")
    s = s.replace("@","2")            
    return s

def get_blob_service_client():
  connect_str = os.getenv("AZURE_TABLE_CS")
  blob_service_client = blob_service_client = BlobServiceClient.from_connection_string(connect_str)
  return blob_service_client

def blob_upload_file(container_name, local_path, local_file_name, email):
  print(container_name)
  print(local_file_name)
  print(local_path)
  service_client = get_blob_service_client() 
  container_client = service_client.get_container_client('climate')
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

def blob_download_file(container_name, local_path, blob_file_name):
  service_client = get_blob_service_client()
  blob_client = service_client.get_blob_client(container=container_name, blob=blob_file_name)
  if not os.path.exists(local_path):
    os.mkdir(local_path)
  with open(file=os.path.join(local_path, blob_file_name), mode="wb") as sample_blob:
    download_stream = blob_client.download_blob()
    sample_blob.write(download_stream.readall())
  return os.path.join(local_path, blob_file_name)

def blob_list_container(container_name):
  service_client = get_blob_service_client() 
  container_client = service_client.get_container_client(container_name)
  return container_client.list_blobs(include='metadata')

def change_metadata(container_name, file_name, key, value):
  service_client = get_blob_service_client()
  blob_client = service_client.get_blob_client(container_name, file_name)
  metadata = blob_client.get_blob_properties().metadata
  metadata[key] = value
  blob_client.set_blob_metadata(metadata)

def delete_local_file(file):  
  os.remove(file)

if __name__ == "__main__":
  service_client = get_blob_service_client() 
  container_client = service_client.get_container_client('climate')
  print(container_client.exists())  
  #service_client.create_container('climate')