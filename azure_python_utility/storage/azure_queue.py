from azure.storage.queue import QueueServiceClient, QueueClient, QueueMessage, StorageErrorCode
import os, time

def get_queue_client(queue_name):
  try:
    connect_str = os.getenv('STORAGE_CONNECTION_STRING')
    print(connect_str)
    queue_client = QueueClient.from_connection_string(connect_str, queue_name)
    return queue_client
  except Exception as ex:
    print('Exception:')
    print(ex)

def get_queue(queue_name):
  try:
    client = get_queue_client(queue_name)
    client.create_queue()
  except Exception as ex:
    if ex.error_code == StorageErrorCode.QUEUE_ALREADY_EXISTS:
      return client
  else: 
    return client

def send_message(queue_name, message):
  queue_client = get_queue(queue_name)
  queue_client.send_message(message)  

def recieve_message(queue_name, message):
  queue_client = get_queue(queue_name)
  return queue_client.receive_message(visibility_timeout=6000)

def delete_message(queue_name, message):
  queue_client = get_queue(queue_name)
  queue_client.delete_message(message)

if __name__ == "__main__":
  queue_client = get_queue('embedding')
  queue_client.send_message('test')
  msg = queue_client.receive_message(visibility_timeout=6000)
  print(msg)
  #queue_client.delete_message(msg)
  time.sleep(2)
  msg2 = queue_client.receive_message(visibility_timeout=6000)
  print(msg2)
                    
