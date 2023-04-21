from azure.storage.queue import QueueServiceClient, QueueClient, QueueMessage, StorageErrorCode
import os, time

class AzureQueue:

  def get_queue_client(self, queue_name):
    try:
      queue_client = QueueClient.from_connection_string(self.cs, queue_name)
      return queue_client
    except Exception as ex:
      print('Exception:')
      print(ex)

  def get_queue(self, queue_name):
    try:
      client = self.get_queue_client(queue_name)
      client.create_queue()
    except Exception as ex:
      if ex.error_code == StorageErrorCode.QUEUE_ALREADY_EXISTS:
        return client
    else: 
      return client

  def send_message(self, queue_name, message):
    queue_client = self.get_queue(queue_name)  
    queue_client.send_message(message)  

  def recieve_message(self, queue_name):
    queue_client = self.get_queue(queue_name)
    return queue_client.receive_message(visibility_timeout=6000)

  def delete_message(self, queue_name, message):
    queue_client = self.get_queue(queue_name)
    queue_client.delete_message(message)

  def __init__(self, connection_string):
    self.cs = connection_string

                    
