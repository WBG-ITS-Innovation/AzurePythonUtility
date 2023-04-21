from azure.data.tables import TableServiceClient
from azure.data.tables import UpdateMode
import uuid
from datetime import datetime
import os


class AzureTable:


  def get_table_client(self, table_name='Prompts'):    
      table_service_client = TableServiceClient.from_connection_string(conn_str=self.cs)
      table_client = table_service_client.create_table_if_not_exists(table_name)
      print("Table name: {}".format(table_client.table_name))
      return table_client

  def create_prompt_entry(self, file_name, question, ss_result, prompt, gpt_response):
      key = str(uuid.uuid4())
      prompt_entity = {
          'PartitionKey': file_name,
          'RowKey': key,
          'question': question,
          'semanticSearch': ss_result,
          'prompt': prompt,
          'response': gpt_response,
          'timestamp': datetime.now()
      }

      table_client = self.get_table_client()
      table_client.create_entity(entity=prompt_entity)
      return key

  def update_prompt_entry(self, key, file_name, response_grade, comment, email):
      table_client = self.get_table_client()
      prompt = table_client.get_entity(partition_key=file_name, row_key=key)
      prompt['responseGrade'] = response_grade
      prompt['comment'] = comment
      prompt['email'] = email
      table_client.update_entity(mode=UpdateMode.MERGE, entity=prompt)

  def list_prompts(self, file_name):
      result = []
      table_client = self.get_table_client()
      my_filter = "PartitionKey eq '{}'".format(file_name)
      print(my_filter)
      entities = table_client.query_entities(my_filter)
      
      for entity in entities:    
          print(entity)
          result.append(entity)
      return result

  def get_user(self, user_name):    
      try:
          table_client = self.get_table_client('Users')
          return table_client.get_entity(partition_key=self.USERS_PARTITION_KEY, row_key=user_name)
      except Exception as err:
          print(err)
      return None

  def create_user(self, email, password):     
      user_entity = {
          'PartitionKey': self.USERS_PARTITION_KEY,
          'RowKey': email,
          'password': password
      }

      table_client = self.get_table_client(self.USERS_TABLE)
      table_client.upsert_entity(mode=UpdateMode.REPLACE, entity=user_entity)
  
  def __init__(self, connection_string) -> None:
      self.cs = connection_string
      self.USERS_PARTITION_KEY = "Users"
      self.USERS_TABLE = "Users"