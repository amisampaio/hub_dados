###################################
# put your debug keys



###################################
#-*- coding: utf-8 -*-




#############################################################################
def isnotebook():
  # return True
  try:
      shell = get_ipython().__class__.__name__
      if shell == 'ZMQInteractiveShell':
          return True   # Jupyter notebook or qtconsole
      elif shell == 'TerminalInteractiveShell':
          return False  # Terminal running IPython
      elif shell == 'TerminalInteractiveShell':
          return False
      elif shell == 'PyDevTerminalInteractiveShell':
          return False
      else:
          if get_ipython().__class__.__module__.count('colab')>0:
            return True
          else:
            return False # Other type (?)
  except NameError:
      return False # Probably standard Python interpreter

#########################################################################################################################
_locals = locals()
import traceback
import warnings
warnings.filterwarnings('ignore')

global thread_list_global
thread_list_global = {}
global running_block_thread
running_block_thread = {}
global thread_running_simples_download
thread_running_simples_download = {}
global thread_bar_close
thread_bar_close = {}

import random

###########################################################################################################################

def versao():
  print("0.0.1")

###########################################################################################################################

import requests
import json
import calendar
from datetime import timedelta
from dateutil.relativedelta import *
from os import listdir
from os.path import isfile, join
from datetime import datetime
from dateutil.relativedelta import relativedelta
from io import StringIO
from pytz import timezone
from time import sleep
import glob
import io
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import unicodedata
import urllib
import urllib.request
import zipfile

###########################################################################################################################
import pandas as pd
def my_formatter(x):
    if x < 0:
        return '({:,.0f})'.format(-x)
    return '{:,.0f}'.format(x)
pd.options.display.float_format = my_formatter
pd.set_option('display.expand_frame_repr', False)
###########################################################################################################################
def install(package):
    try:
      subprocess.check_call([sys.executable, "-m", "pip", "install", package])
    except Exception as e:
      print("Erro 0:",str(e))
      #print(traceback.print_exc())
      pass
###########################################################################################################################

global session


def login_gitlab(gitlab_user, gitlab_key,gitlab_url="https://gitlab.dru.decode.buzz/crawlers"):
    global config
    global c
    config.gitlab_user = gitlab_user
    config.gitlab_key = gitlab_key
    config.gitlab_url = gitlab_url
    c = crawlers_hub()
    return c


def login_aws(aws_key, aws_region="",aws_bucket="",aws_output_athena="",aws_database_athena='', ambiente_config ='dev'):

    global config
    global t

    if aws_region == '':
        aws_region = 'us-east-1'

    if aws_bucket == '':
        aws_bucket = 'dru-raw-zone-dev'

    if aws_output_athena == '':
        aws_output_athena = "s3://prod-dru-logs/athena/"

    if aws_database_athena == '':
        aws_database_athena = "raw_public_data_dev"


    if aws_key.count("\n") > 0:
        for s in aws_key.split("\n"):
            if s[:3] == "aws":
                exec(s.split("=")[0].rstrip().lstrip().lower() + "='" + s.split("=")[1].rstrip().lstrip() + "'")
            if s[:3] == "exp":
                s = s.replace("export ", "")
                quote = ''
                quote = "\"" if s.split("=")[1].count('"') == 1 else ""
                #print(s.split("=")[0].rstrip().lstrip().lower() + "=" + s.split("=")[1].rstrip().lstrip() + quote)
                if s.split("=")[0].rstrip().lstrip().lower() == "aws_secret_access_key":
                    aws_secret_access_key = s.split("=")[1].rstrip().lstrip() + quote
                    aws_secret_access_key = aws_secret_access_key.replace('"', '').replace(' ', '')
                if s.split("=")[0].rstrip().lstrip().lower() == "aws_access_key_id":
                    aws_access_key_id = s.split("=")[1].rstrip().lstrip() + quote
                    aws_access_key_id = aws_access_key_id.replace('"','').replace(' ', '')
                if s.split("=")[0].rstrip().lstrip().lower() == "aws_session_token":
                    aws_session_token = s.split("=")[1].rstrip().lstrip() + quote
                    aws_session_token = aws_session_token.replace('"', '').replace(' ', '')

    else:
        for s in aws_key.split("export "):
            variavel = s.split("=")
            if variavel[0] != "":
                quote = "\"" if variavel[1].count('"') == 1 else ""
                #print(variavel[0].rstrip().lstrip().lower() + "=" + variavel[1] + quote)
                if variavel[0].rstrip().lstrip().lower() == "aws_secret_access_key":
                    aws_secret_access_key = variavel[1] + quote
                    aws_secret_access_key = aws_secret_access_key.replace('"', '').replace(' ', '')
                if variavel[0].rstrip().lstrip().lower() == "aws_access_key_id":
                    aws_access_key_id = variavel[1] + quote
                    aws_access_key_id = aws_access_key_id.replace('"', '').replace(' ', '')
                if variavel[0].rstrip().lstrip().lower() == "aws_session_token":
                    aws_session_token = variavel[1] + quote
                    aws_session_token = aws_session_token.replace('"', '').replace(' ', '')

    import os
    os.system("pip install boto3")
    import boto3
    if aws_secret_access_key != "":
        try:
            if (aws_secret_access_key != ""):
                session = boto3.Session(region_name=aws_region, aws_access_key_id=aws_access_key_id,
                                        aws_secret_access_key=aws_secret_access_key,
                                        aws_session_token=aws_session_token)

                config.session = session
                config.aws_access_key_id = aws_access_key_id
                config.aws_secret_access_key = aws_secret_access_key
                config.aws_session_token = aws_session_token
                config.aws_region = aws_region

                config.aws_bucket = aws_bucket
                config.aws_output_athena = aws_output_athena
                config.aws_database_athena = aws_database_athena

                config.ambiente_config = ambiente_config


                config.s3_resource = session.resource('s3')
                config.s3_client = session.client('s3')
                config.athena_client = session.client('athena')
                buckets = []
                try:
                    for bucket in config.s3_resource.buckets.all():
                        buckets.append(bucket.name)
                    config.buckets = buckets
                except Exception as e:
                    config.buckets = ["Erro5:" + str(e)]
                    pass

                catalogs = []
                try:
                    responseGetDatabases = config.athena_client.list_data_catalogs()
                    for catalog in responseGetDatabases['DataCatalogsSummary']:
                        databases = []
                        for database in config.athena_client.list_databases(CatalogName=catalog['CatalogName'])[
                            'DatabaseList']:
                            databases.append(database['Name'])
                        catalogs.append([catalog['CatalogName'], databases])
                    config.catalogs = catalogs
                    config.athena_read = True
                except Exception as e:
                    config.catalogs = ["Erro6:" + str(e)]
                    config.athena_read = False
                    pass

            else:
                raise
        except:
            try:
                session = boto3.Session(region_name=aws_region, aws_access_key_id=aws_access_key_id,
                                        aws_secret_access_key=aws_secret_access_key)
                config.session = session
                config.aws_access_key_id = aws_access_key_id
                config.aws_secret_access_key = aws_secret_access_key
                config.aws_region = aws_region

                config.aws_bucket = aws_bucket
                config.aws_output_athena = aws_output_athena
                config.aws_database_athena = aws_database_athena

                config.ambiente_config = ambiente_config

                config.s3_resource = session.resource('s3')
                config.s3_client = session.client('s3')
                config.athena_client = session.client('athena')


                buckets = []
                try:
                    for bucket in config.s3_resource.buckets.all():
                        buckets.append(bucket.name)
                    config.buckets = buckets
                except Exception as e:
                    config.buckets = ["Erro5:" + str(e)]
                    pass

                catalogs = []
                try:
                    responseGetDatabases = config.athena_client.list_data_catalogs()
                    for catalog in responseGetDatabases['DataCatalogsSummary']:
                        databases = []
                        for database in config.athena_client.list_databases(CatalogName=catalog['CatalogName'])[
                            'DatabaseList']:
                            databases.append(database['Name'])
                        catalogs.append([catalog['CatalogName'], databases])
                    config.catalogs = catalogs
                    config.athena_read = True
                except Exception as e:
                    config.catalogs = ["Erro6:" + str(e)]
                    config.athena_read = False
                    pass



            except Exception as e:
                print(e)
    try:
        s3_resource, buckets = session.resource("s3"), []
        x: [buckets.append(bucket.name) for bucket in s3_resource.buckets.all()]
        print("AWS session work")
    except Exception as e:
        print("AWS erro:", str(e))

    t = tabelas_hub()

    return session, t

####################################################################

try:
  import boto3
except:
    try:
        install("boto3")
        import boto3
    except:pass

try:
  import gitlab
except:
    try:
        install("python-gitlab")
        import gitlab
    except:pass

####################################################################
try:
    import seaborn as sns
except:
    try:
        install("seaborn")
        import seaborn as sns
    except:
        pass

try:
    import matplotlib.pyplot as plt
except:
    try:
        install("matplotlib")
        import matplotlib.pyplot as plt
    except:
        pass


#import pandas as pd
#import seaborn as sns
#import matplotlib.pyplot as plt

# df = pd.DataFrame({
#     'Factor': ['Growth', 'Value'],
#     'Weight': [0.10, 0.20],
#     'Variance': [0.15, 0.35]
# })
#
# plt.show()
# fig, ax1 = plt.subplots(figsize=(10, 10))
# tidy = df.melt(id_vars='Factor').rename(columns=str.title)
# sns.barplot(x='Factor', y='Value', hue='Variable', data=tidy, ax=ax1)
# sns.despine(fig)
# plt.show()

###########################################################################################################################
try:
  session = boto3.Session()
except:
  session=''
  pass

try:
  os.chdir("/content/")
except:
  pass
try:
  os.makedirs("./dados/")
except:
  pass

try:
  os.makedirs("./consultas_athena/")
except:
  pass

####################################################################
# try:
#     from tqdm.auto import tqdm, trange
# except:
#     try:
#         install("tqdm")
#         from tqdm.auto import tqdm, trange
#     except:
#         pass
#********************************************************************************************
if isnotebook():
    from tqdm.auto import tqdm, trange
else:
    from tqdm import tqdm, trange

from time import sleep
# ********************************************************************************************
# Bar functions 1
global bar_process_dates
bar_process_dates = {}
####################################################################
def update_bar_process_dates(endpoint, msg, perc_sucesso=1, bloco_id=0, total_blocos=0):
    endpoint_ = endpoint[:20].ljust(20).replace(" ", "_") if len(endpoint) > 20 else endpoint.ljust(20).replace(" ","_")
    global bar_process_dates
    if isnotebook():
        desc = endpoint_ + " | Bloco " + str(bloco_id + 1) + "/" + str(total_blocos + 1)
    else:
        desc = endpoint_ + " | "
    bar_process_dates[endpoint].set_description(desc)
    if msg != '':
        bar_process_dates[endpoint].set_postfix({'Log': msg})
    bar_process_dates[endpoint].update(perc_sucesso)
####################################################################
def plot_bar_process_dates(qnt_datas, endpoint):
    endpoint_ = endpoint[:20].ljust(20).replace(" ", "_") if len(endpoint) > 20 else endpoint.ljust(20).replace(" ","_")
    global bar_process_dates
    if isnotebook():
        formato = "{l_bar}{r_bar}"
    else:
        formato = "{l_bar}{bar}{r_bar}"
    bar_process_dates[endpoint] = trange(qnt_datas, unit="base", desc=endpoint_ + " | ", leave=True, bar_format=formato)
    bar_process_dates[endpoint].reset()
    time.sleep(0.7)
####################################################################
def plot_bar_process_dates_close(endpoint, size):
    global bar_process_dates
    bar_process_dates[endpoint].reset()

    if isnotebook():

        try:
            if len(bar_process_dates[endpoint].postfix) > 0:
                sucesso = True
        except:
            sucesso = False

        if sucesso:
            bar_process_dates[endpoint].update(bar_process_dates[endpoint].total)
        else:
            bar_process_dates[endpoint].update(bar_process_dates[endpoint].total - bar_process_dates[endpoint].total/2)
            print("Nenhum dado encontrado no endpoint : ", endpoint)
        time.sleep(0.1)
    else:
        bar_process_dates[endpoint].update(size)

    bar_process_dates[endpoint].close()
    time.sleep(0.7)
    global thread_bar_close
    thread_bar_close[endpoint] = 1
####################################################################
def func_size_checkpoint(total_jobs):
    return 1 / total_jobs
# ********************************************************************************************
global bar_process_athena
# Bar functions 2
def update_bar_process_athena(msg):
    global bar_process_athena
    if msg != '':
        bar_process_athena.set_postfix({'Log': msg})
    bar_process_athena.update()
    bar_process_athena.refresh()
def plot_bar_process_athena(qnt_tabelas):
    global bar_process_athena
    if isnotebook():
        formato = "{l_bar}{r_bar}"
    else:
        formato = "{l_bar}{bar}{r_bar}"
    bar_process_athena = trange(qnt_tabelas, unit="tabela", desc= "", leave=True, bar_format=formato)
    bar_process_athena.reset()
    return True
def plot_bar_process_athena_close():
    global bar_process_athena
    bar_process_athena.close()
# ********************************************************************************************



###########################################################################################################################

class endpoint:
    def __init__(self, endpoints = [] ):
      for endpoint in sorted(endpoints):
        exec('self.' + endpoint + ' = "' + endpoint + '"') # , globals(), _locals)
      self.get_all = sorted(endpoints)

###########################################################################################################################

class enpoint_crawler(object):
   def __init__(self,lista = []):
     #pass
     #lista_endpoints =[]
     for item in sorted(lista):
      crawler_name = item[0]
      endpoints = item[1]
      #print(crawler_name, endpoints)
      exec('self.' + crawler_name + ' = endpoint(endpoints)')
      #lista_endpoints.append(item[1])
     #self.all = lista_endpoints

###########################################################################################################################

class tabelas_hub():
   def __init__(self):
    try:
      catalogo = config.athena_client.list_table_metadata(CatalogName="AwsDataCatalog", DatabaseName = config.aws_database_athena)
      lista_tabelas = []
      for dado in catalogo["TableMetadataList"]:
        exec('self.' + dado["Name"] + ' = "' + dado["Name"] + '"')
        lista_tabelas.append(dado["Name"])
      self.all = lista_tabelas
    except Exception as e:
      if str(e).count("'str' object has no attribute 'list_table_metadata'") > 0:
        print("\n", "AWS : Vc precisa fazer login para acessar os dados")
      else:
        print("Erro4:",str(e))
        print(traceback.print_exc())
      pass

###########################################################################################################################

class ambiente_hub:

   def __init__(self):
     
      global ambiente_config
      global aws_access_key_id
      global aws_secret_access_key
      global aws_session_token
      global session
      global gitlab_url
      global gitlab_user
      global gitlab_key
      global api_key

      try:
        self.session = session
      except Exception as e:
        print("Erro1:"+str(e))
        print(traceback.print_exc())
        pass

      try:
          ambiente_config = ambiente_config
      except:
          ambiente_config = 'dev'
      try:
          aws_access_key_id = aws_access_key_id
      except:
          aws_access_key_id = ''
      try:
          aws_secret_access_key = aws_secret_access_key
      except:
          aws_secret_access_key = ''
      try:
          aws_session_token = aws_session_token
      except:
          aws_session_token = ''
      try:
          gitlab_url = gitlab_url
      except:
          gitlab_url = ''
      try:
          gitlab_user = gitlab_user
      except:
          gitlab_user = ''
      try:
          gitlab_key = gitlab_key
      except:
          gitlab_key = ''
      try:
          api_key = api_key
      except:
          api_key = ''

      #************************************************************************
      if ambiente_config =='':
        ambiente_config ='default'

      #************************************************************************
      if ambiente_config =='default':
        ambiente_config ='dev'

      #************************************************************************
      if ambiente_config =='pessoal':
        aws_bucket = ''
        aws_region = ''
        aws_output_athena = ''
        aws_database_athena = "raw_public_data_dev"

      #************************************************************************
      if ambiente_config =='dev':
        aws_bucket = 'dru-raw-zone-dev'
        aws_region = 'us-east-1'
        aws_output_athena = "s3://prod-dru-logs/athena/"
        aws_database_athena = "raw_public_data_dev"
        
      #************************************************************************
      if ambiente_config =='prod':
        aws_bucket = 'prod-dru-raw-zone'
        aws_region = 'us-east-1'
        aws_output_athena = "s3://prod-dru-logs/athena/"
        aws_database_athena = "raw_public_data_prod"

      # for debug #############################
      if (aws_bucket == '') & (ambiente_config == 'pessoal'):
          try:
              global aws_bucket_debug
              aws_bucket = aws_bucket_debug
          except:pass
          try:
              global aws_output_athena_debug
              aws_output_athena = aws_output_athena_debug
          except:pass
          try:
              global aws_region_debug
              aws_region = aws_region_debug
          except:pass
          try:
              global gitlab_url_debug
              gitlab_url = gitlab_url_debug
          except:pass
          try:
              global gitlab_user_debug
              gitlab_user = gitlab_user_debug
          except:pass
          try:
              global gitlab_key_debug
              gitlab_key = gitlab_key_debug
          except:pass
          try:
              global api_key_debug
              api_key = api_key_debug
          except:pass

      #########################################

      self.ambiente_config = ambiente_config
      self.aws_access_key_id = aws_access_key_id
      self.aws_secret_access_key = aws_secret_access_key
      self.aws_session_token = aws_session_token
      self.aws_bucket = aws_bucket
      self.aws_region = aws_region
      self.aws_output_athena = aws_output_athena
      self.aws_database_athena = aws_database_athena
      self.root_inicial = os.getcwd()
      self.api_key = api_key

      self.gitlab_url = gitlab_url
      self.gitlab_user = gitlab_user
      self.gitlab_key = gitlab_key

      try:
          if str(session)==str(boto3.Session()):
            try:
              if aws_access_key_id !="":
                if aws_session_token=="":
                  self.session = boto3.Session(region_name = aws_region, aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)
                else:
                  self.session = boto3.Session(region_name = aws_region, aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key,aws_session_token = aws_session_token)
            except Exception as e:
              self.session = boto3.Session()
              print("Erro2:"+str(e))
              print(traceback.print_exc())
              pass
          else:
            self.session = session
      except:
          self.session = ''

      try:
        self.s3_resource = self.session.resource('s3')
      except Exception as e:
        if os.getenv('install_libs', '') != 'ignore':
            print("Erro s3_resource:"+str(e))
            print(traceback.print_exc())
        self.s3_resource = ""
        pass

      try:
        self.s3_client = self.session.client('s3')
      except Exception as e:
        if os.getenv('install_libs', '') != 'ignore':
            print("Erro s3_client:"+str(e))
            print(traceback.print_exc())
        self.s3_client = ""
        pass
        
      try:
        self.athena_client = self.session.client('athena')
      except Exception as e:
        #print("Erro athena_client:"+str(e))
        self.athena_client = ""
        pass

      buckets = []
      try:
        for bucket in self.s3_resource.buckets.all():
          buckets.append(bucket.name)
        self.buckets = buckets
      except Exception as e:
        self.buckets = ["Erro5:"+str(e)]
        pass

      catalogs = []
      try:
        responseGetDatabases = self.athena_client.list_data_catalogs()
        for catalog in responseGetDatabases['DataCatalogsSummary']:
          databases = []
          for database in self.athena_client.list_databases(CatalogName=catalog['CatalogName'])['DatabaseList']:
            databases.append(database['Name'])
          catalogs.append([catalog['CatalogName'],databases])
        self.catalogs = catalogs
        self.athena_read = True
      except Exception as e:
        self.catalogs = ["Erro6:"+str(e)]
        self.athena_read = False
        pass

global config
config = ambiente_hub()


def config_hub_login():
    global config
    config = ambiente_hub()

###########################################################################################################################

class crawlers_hub():
   def __init__(self):
       try:
           lista_crawlers = []
           gl = gitlab.Gitlab('https://' + config.gitlab_url.split("//")[1].split('/')[0], private_token=config.gitlab_key,  api_version=4)
           gl.auth()
           projects = gl.projects.list(owned=True)
           for project in projects:
               if project.namespace['web_url'].replace("/groups/","/") == config.gitlab_url:
                   if project.name.count('crawler_') > 0:
                       # print(project.name, project.http_url_to_repo, project.id )
                       exec('self.' + project.name + ' = "' + project.name + '"')
                       lista_crawlers.append(project.name)
                       # get(crawler_name)
           self.get_all = lista_crawlers
       except Exception as e:
           if os.getenv('install_libs', '') != 'ignore':
               print("Erro 8:", str(e))
               print("try: pip install python-gitlab")
               # print(traceback.print_exc())
           pass

try:
  if config.gitlab_user != '':
    c = crawlers_hub()
except:pass

###########################################################################################################################

#print(datetime.now())
try:
    if config.athena_read:
        t = tabelas_hub()
except:pass
#print(datetime.now())
#print(1)

###########################################################################################################################

def info():
  print("------------------------------------------")
  print("ambiente:",config.ambiente_config)
  print("root_inicial:",config.root_inicial)
  print("------------------------------------------")
  print("aws_access_key_id:",config.aws_access_key_id)
  print("aws_secret_access_key:",config.aws_secret_access_key)
  print("aws_session_token:",config.aws_session_token)
  print("aws_region:",config.aws_region)
  print("------------------------------------------")
  print("aws_bucket:",config.aws_bucket)
  print("------------------------------------------")
  print("session:",config.session)
  print("s3_client:",config.s3_client)
  print("athena_resource:",config.athena_client)
  print("s3_resource:",config.s3_resource)
  print("------------------------------------------")
  print("aws_output_athena:",config.aws_output_athena)
  print("aws_database_athena:",config.aws_database_athena)
  print("------------------------------------------")
  print("buckets:",config.buckets)
  print("catalogs:",config.catalogs)
  print("------------------------------------------")
  print("api_key:", config.api_key)
  print("------------------------------------------")
  print("gitlab_url:", config.gitlab_url)
  print("gitlab_user:", config.gitlab_user)
  print("gitlab_key:", config.gitlab_key)


# info()

###########################################################################################################################

def zipar(path_thread):
  import zipfile as zipf
  import os
  import glob
  import time
  teste = ''
  try:
    arqs = glob.glob(path_thread + '*.csv')
    teste == arqs[0].replace("\\","/")
    with zipf.ZipFile(arqs[0].replace('.csv', '') + '.zip', 'w', zipf.ZIP_DEFLATED) as z:
        for arq in arqs:
            if (os.path.isfile(arq)):  # se for ficheiro
                file_name = arq
                fim_path = file_name.rfind('/')
                if fim_path == -1:
                    fim_path = 0
                else:
                    fim_path = fim_path + 1
                file_name = file_name[fim_path: len(file_name)]
                z.write(arq, file_name)
            else:  # se for diretorio
                for root, dirs, files in os.walk(arq):
                    for f in files:
                        z.write(os.path.join(root, f))

  except Exception as e:
    #print("Não existe nenhum csv para fazer upload para o hub =| ",str(e))
    pass


###########################################################################################################################

##################################################################################################
# Essa função detecta o encoding do arquivo
##################################################################################################
def get_encoding(path_original):
    import chardet
    f = open(path_original, 'rb')
    rawdata = b''.join([f.readline() for _ in range(10000)])
    return chardet.detect(rawdata)['encoding']

##################################################################################################
# Essa função detecta o separador do arquivo
##################################################################################################
def get_separator(path_original, _encoding):
    import chardet
    import pandas as pd
    reader = pd.read_csv(path_original, sep = None, iterator = True, encoding=_encoding)
    return reader._engine.data.dialect.delimiter

##################################################################################################

def normalizar_csv_to_df(csv_name, database, crawler,base, amostras, csv_encoding, csv_sep, csv_doublequote, data_extracao, url, path_s3, bucket, regiao_aws, path_s3_raw,  path_log="", padrao_s3 = "", endpoint= "",insert_header=False,layout_header=[]):

  #Upload do arquivo csv para dataframe

  try:
    _encoding = get_encoding(csv_name)
  except:
    _encoding = csv_encoding

  try:
    _sep = get_separator(csv_name,_encoding)
  except:
    _sep = csv_sep


  if amostras == -1:

    #log_hub(path_log,"importando arquivo no pandas")
    if insert_header:
        df=pd.read_csv(csv_name, encoding=_encoding, sep=_sep, doublequote=True, low_memory=False, error_bad_lines=False, header=None)
        df.columns = layout_header
    else:
        df=pd.read_csv(csv_name, encoding=_encoding, sep=_sep, doublequote=True, low_memory=False, error_bad_lines=False)

  else:
    log_hub(path_log,"importando amostra no pandas")
    if insert_header:
        df=pd.read_csv(csv_name, encoding=_encoding, sep=_sep, doublequote=True, low_memory=False, error_bad_lines=False, header=None, nrows=amostras)
        df.columns = layout_header
    else:
        df=pd.read_csv(csv_name, encoding=_encoding, sep=_sep, doublequote=True, low_memory=False, error_bad_lines=False, nrows=amostras)


  #log_hub(path_log,"Adicionando colunas: data_base, data_extracao e url")
  path_csv_s3 = "https://s3.console.aws.amazon.com/s3/object/"+ bucket + "?prefix=" + path_s3 + "&region=" + regiao_aws
  path_parquet_s3 = "https://s3.console.aws.amazon.com/s3/object/"+ bucket + "?prefix=" + "parquet/" + path_s3.replace(".csv",".parquet") + "&region=" + regiao_aws
  path_s3_raw_ = "https://s3.console.aws.amazon.com/s3/object/"+ bucket + "?prefix=" + path_s3_raw + "&region=" + regiao_aws
  df['hub_data_extracao'] = data_extracao
  df['hub_url_extracao'] = url
  df['hub_nome_crawler'] = crawler
  df['hub_base'] = base
  df['hub_data_base'] = database
  df['hub_file_raw_s3'] = path_s3_raw_
  df['hub_file_equalizado_s3'] = path_csv_s3
  df['hub_file_comprimido_s3'] = path_parquet_s3
  df['hub_padrao_s3'] = padrao_s3
  df['hub_endpoint'] = endpoint
  df = df_normalize_head(df,crawler,base,path_log)
  return df

###########################################################################################################################

def log_hub(path_log,text_log):
  try:
    with open(path_log + '_log.txt', 'a') as f:
        print(text_log, file=f)
  except:
    pass

###########################################################################################################################

def status_hub(path_log,text_log,append = "w",console = False):
  time.sleep(0.1)
  try:
    with open(path_log + '_status.txt', append) as f:
        print(text_log, file=f)
  except:
    pass
  if console:
    #print("\n",text_log, "\n")
    pass

###########################################################################################################################

###########################################################################################################################

def get_time_now():
  return datetime.utcnow().strftime("%d/%m/%Y, %H:%M:%S")
  #return (datetime.now(timezone('America/Sao_Paulo') ) - relativedelta(hours=0)  ).strftime("%d/%m/%Y, %H:%M:%S")

###########################################################################################################################

def normalizar_string(texto):
  texto = unicodedata.normalize('NFD', texto.lower()).encode('ascii', 'ignore').decode("utf-8").strip().replace(" ", "_")
  texto = texto.replace("-","_").replace("(*)","").replace("/","_").replace("(r$)","brl").replace("(u$)","usd")
  texto = texto.replace('__', '_')
  texto = texto.replace('(', '_')
  texto = texto.replace(')', '_')
  if texto[len(texto)-1:len(texto)] == "_":
    texto = texto[0:len(texto)-1]
  return texto

###########################################################################################################################

def retorna_tipo(df, nome_campo):
    # https://docs.aws.amazon.com/athena/latest/ug/data-types.html
    tipo_campo = str(df[nome_campo].convert_dtypes().dtypes).lower()
    tipo_campo = 'int' if 'int' in tipo_campo else tipo_campo 
    tipo_campo = 'string' if 'str' in tipo_campo else tipo_campo 
    tipo_campo = 'decimal' if 'dec' in tipo_campo else tipo_campo 
    tipo_campo = 'boolean' if 'boo' in tipo_campo else tipo_campo 
    tipo_campo = 'date' if 'date' in tipo_campo else tipo_campo 
    tipo_campo = 'timestamp' if 'time' in tipo_campo else tipo_campo 
    tipo_campo = 'double' if 'dou' in tipo_campo else tipo_campo 
    tipo_campo = 'float' if 'flo' in tipo_campo else tipo_campo 
    return tipo_campo

###########################################################################################################################

def df_upload_file_to_s3(file_path, s3_resource, bucket, path_s3, regiao_aws, path_log=""):
  if bucket != '':
    s3_resource.Bucket(bucket).upload_file(file_path, path_s3)
    log_hub(path_log, "Upload AWS1 : " + bucket + "/" + path_s3)
    #+ file_path + " | "
  else:
    log_hub(path_log,"csv sem upload AWS variavel bucket está vazia : " + file_path)
    #print("CSV Salvo localmente : " , file_path )
###########################################################################################################################

def df_upload_csv_to_s3(df, s3_resource, bucket, path_s3, regiao_aws, path_log="", Upload_AWS = True):
  if Upload_AWS:
      if bucket != '':
          export_encoding = 'utf-8'
          export_sep=';'

          file_buffer = StringIO()
          df.to_csv(file_buffer, index=False , encoding=export_encoding , sep=export_sep, doublequote=True )
          s3_resource.Object(bucket, path_s3).put(Body=file_buffer.getvalue())
          log_hub(path_log, "Upload AWS 2 : " + bucket + '/' + path_s3)
          buffer_parquet = io.BytesIO()
          df = df.astype(str)
          df.to_parquet(buffer_parquet, engine='pyarrow')
          s3_resource.Object(bucket, "parquet/" + path_s3.replace(".csv",".parquet")).put(Body=buffer_parquet.getvalue())
          log_hub(path_log, "Upload AWS 3 : " + bucket + '/' + "parquet/" + path_s3.replace(".csv",".parquet"))
          return df
      else:
        log_hub(path_log, "Parquet sem upload AWS variavel bucket está vazia" )
        #print("Parquet sem upload AWS, variavel bucket está vazia : ", file_path )



###########################################################################################################################

def df_head(df):
  head = []
  head_original = []
  k = 0
  for i in df.columns: 
    head.append(i)
  return head

###########################################################################################################################

def df_normalize_head(df,crawler,base, path_log=""):
  #log_hub(path_log,"Normalizando nome das colunas")
  head = []
  head_original = []
  k = 0
  for i in df.columns: 
    nome_campo = normalizar_string(i) 
    df.rename(columns={ df.columns[k]: nome_campo }, inplace = True)
    head_original.append(i)
    head.append(nome_campo)
    #cadastro de metadados no hub de crawlers
    # try:
    #     params = {}
    #     params['crawler_code'] = crawler
    #     params['base_code'] = base
    #     params['name'] = nome_campo
    #     params['type'] = retorna_tipo(df, nome_campo)
    #     params['description'] = 'campo preenchido automaticamente'
    #     #print(params)
    #     result = requests.post(f'{url_hub}/api/variable/autoadd', data=params)
    #     if result.text != "{}":
    #       log_hub(path_log,"")
    #       log_hub(path_log,"************")
    #       log_hub(path_log,"API HUB")
    #       log_hub(path_log,"")
    #       log_hub(path_log,result.text)
    #       print("Resposta Hub:", params, result.text)
    # except Exception as e: 
    #     log_hub(path_log,str(e))
    #     pass
    k = k + 1
  log_hub(path_log,head_original)
  log_hub(path_log,head)
  return df

###########################################################################################################################

def extrator_url_zip(url, zip_name, path, path_log=""):
  status = True
  try:

    log_hub(path_log,"baixando : " + url)
    #print("\n","baixando : ", url,"\n")
    max_retries = 2
    tentativas = 0
    status_htttp = False
    exit_loop = 0
    while exit_loop == 0:
        try:
            r = requests.head(url, timeout=30)
            if r.status_code == 200:
                status_htttp = True
                file_size = r.headers.get("Content-Length")
                log_hub(path_log, "Encontramos  : " + str(file_size) + " bytes de dados a serem baixados")

                exit_loop = 1
            else:
                tentativas = tentativas + 1
        except:
            tentativas = tentativas + 1
            pass

        if tentativas > max_retries:
            exit_loop = 1
        time.sleep(random.uniform(0, 0.5))



    if status_htttp:

        urllib.request.urlretrieve(url, path + zip_name,)

        log_hub(path_log,"retirando do zip" + path + zip_name)
        with zipfile.ZipFile(path + zip_name, 'r') as zip_ref:
              zip_ref.extractall(path)

        #####################################################
        # Transformar .txt em .csv
        import glob
        import os
        for file in glob.glob(path + '*.txt'):
            file = file.replace("\\","/")
            if (file.count('_log.txt') + file.count('_status.txt')) == 0:
                try:
                    try:
                        os.remove(file.replace(".txt", ".csv"))
                    except:pass
                    os.rename(file, file.replace(".txt", ".csv"))
                except Exception as e:
                    print(e)
                    pass

    else:
        log_hub(path_log, "Dados nao encontrados na url : " + url)
        status = False
        pass


  except Exception as e:
    log_hub(path_log,"erro" + str(e))
    status = False
    pass
  return status

###########################################################################################################################

def run_querry_athena(Query, Output_Local_Path, Output_File_Name, Export_Local, Export_Frame):

  import os
  cwd = os.getcwd()
  del os 

  try:

    athena = config.athena_client # cria client do athena
    s3 = config.s3_client # cria client do S3
    Output_S3_bucket_path = config.aws_output_athena.replace("s3://"+config.aws_output_athena.split("/")[2],"")[1:len(config.aws_output_athena.replace("s3://"+config.aws_output_athena.split("/")[2],""))]

    temp = athena.start_query_execution(QueryString=Query, # query a ser executada
                                        QueryExecutionContext={'Database': config.aws_database_athena}, # nome do banco de dados no athena
                                        ResultConfiguration={'OutputLocation': config.aws_output_athena})  # bucket onde o arquivo de resuktado da query sera gravado

    ID_QUERY = temp['QueryExecutionId'] # nome do arquivo gera como resultado da query 

    #*****************************************************************************
    #   Espera o fim da query do Athena
    #*****************************************************************************

    STATUS = "RUNNING"
    STATUS_LIST = {"QUEUED","RUNNING"};

    i = 0
    while STATUS in STATUS_LIST:
        status_query = athena.get_query_execution(QueryExecutionId = ID_QUERY)
        STATUS = status_query['QueryExecution']['Status']['State']

        if STATUS == "RUNNING":
            i = i + 1
            #print(STATUS, i, " segundos.")
            sleep(0.1)

    if STATUS != "SUCCEEDED":
        print(STATUS)
        print(temp)
        print(status_query)
        print(Query)
        raise TypeError("Erro")

        
    if Export_Local == True:

        #*****************************************************************************
        #   Muda o diretorio para onde sera salvo a query
        #*****************************************************************************
        import os
        os.chdir(Output_Local_Path)
        del os 
        import os

        #*****************************************************************************
        #   Salva o resultado da query no diretório definido
        #*****************************************************************************
        s3.download_file(config.aws_output_athena.split("/")[2],Output_S3_bucket_path + '{}.csv'.format(ID_QUERY),'' + '{}.csv'.format(Output_File_Name))
        
        #*****************************************************************************
        #   Volta o diretorio original
        #*****************************************************************************
        import os
        os.chdir(cwd)
        del os 
        import os        
        
        return "ok"

    if Export_Frame == True:
        #*****************************************************************************
        #   Carrega a saida em memoria formato pandas
        #*****************************************************************************
        SAIDA = s3.get_object(Bucket=config.aws_output_athena.split("/")[2], Key=Output_S3_bucket_path + '{}.csv'.format(ID_QUERY))
        return pd.read_csv(SAIDA['Body'],low_memory=False) #.head(1)

  except:
        
    import os
    os.chdir(cwd)
    del os 
    import os

###########################################################################################################################

def download_athena(Query, Output_Local_Path="", Output_File_Name=""):
    #config = ambiente_hub() if config == "" else config
    Output_Local_Path = config.root_inicial if Output_Local_Path == "" else Output_Local_Path
    Output_File_Name = "output_athena" if Output_File_Name == "" else Output_File_Name
    return run_querry_athena(Query, Output_Local_Path, Output_File_Name, True, False)

###########################################################################################################################
    
def frame_athena(Query):
    #config = ambiente_hub() if config == "" else config
    return run_querry_athena( Query, "", "", False, True)

###########################################################################################################################

def get_data_frame(table,limit="5",where="",info="put all on limit to bring all"):
  #config = ambiente_hub() if config == "" else config
  LIMITE = "LIMIT " + limit
  if limit == "all":
    LIMITE = ""
  Query = 'select * FROM ' + config.aws_database_athena + '.' + table + ' ' + where + LIMITE
  return frame_athena(Query)

###########################################################################################################################

def get_data_frame_sql(Query):
  #config = ambiente_hub() if config == "" else config
  return frame_athena(Query)

###########################################################################################################################

def get_data_file(table,limit="5",info="put all on limit to bring all"):
  #config = ambiente_hub() if config == "" else config
  LIMITE = "LIMIT " + limit
  if limit == "all":
    LIMITE = ""
  Query = 'select * FROM ' + config.aws_database_athena + '.' + table + ' ' + LIMITE
  return download_athena(Query,"./consultas_athena/", table)
  
###########################################################################################################################

def create_table_athena_parquet(athena_resource, banco_dados, tabela, head, source, output, path_log=""):

  #-----------------------------------------------------------------------------------------------------------------
  br = """

  """
  #-----------------------------------------------------------------------------------------------------------------
  query = "CREATE DATABASE IF NOT EXISTS " + banco_dados
  temp = athena_resource.start_query_execution( QueryString = query, ResultConfiguration = {'OutputLocation': output}  )
  ID_QUERY = temp['QueryExecutionId']
  STATUS = "RUNNING"
  STATUS_LIST = {"QUEUED","RUNNING"};
  i = 0
  while STATUS in STATUS_LIST:
      status_query = athena_resource.get_query_execution(QueryExecutionId = ID_QUERY)
      STATUS = status_query['QueryExecution']['Status']['State']
      if STATUS == "RUNNING":
          i = i + 1
          #print(STATUS, i, " segundos.")
          sleep(1)
  log_hub(path_log,"CREATE DATABASE" + STATUS)
  #-----------------------------------------------------------------------------------------------------------------
  query = "DROP TABLE " + tabela
  temp = athena_resource.start_query_execution( QueryString = query  ,   ResultConfiguration = {'OutputLocation': output}  )
  ID_QUERY = temp['QueryExecutionId']
  STATUS = "RUNNING"
  STATUS_LIST = {"QUEUED","RUNNING"};
  i = 0
  while STATUS in STATUS_LIST:
      status_query = athena_resource.get_query_execution(QueryExecutionId = ID_QUERY)
      STATUS = status_query['QueryExecution']['Status']['State']
      if STATUS == "RUNNING":
          i = i + 1
          #print(STATUS, i, " segundos.")
          sleep(1)
  log_hub(path_log,"DROP TABLE" + STATUS)
  #-----------------------------------------------------------------------------------------------------------------
  query = ""
  query = query + " CREATE EXTERNAL TABLE IF NOT EXISTS " + tabela + " (" + br

  for i in range(0,len(head)):
    quota = ","
    if i == len(head)-1:
      quota = ""
    tipo = "string"
    query = query + "`" + head[i] + "` "+ tipo + quota + br

  query = query + " ) " + br
  query = query + " ROW FORMAT SERDE  " + br
  query = query + " 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'  " + br
  query = query + " WITH SERDEPROPERTIES (  " + br
  query = query + " 'serialization.format' = '1'  " + br
  query = query + " )   " + br
  query = query + " LOCATION '" + source + "' "  + br
  query = query + " TBLPROPERTIES ( " + br
  query = query + " 'has_encrypted_data'='false' " + br
  query = query + " );  " + br
  #print(query)
  #-----------------------------------------------------------------------------------------------------------------
  temp = athena_resource.start_query_execution( QueryString = query ,  ResultConfiguration = {'OutputLocation': output}  )
  ID_QUERY = temp['QueryExecutionId']
  STATUS = "RUNNING"
  STATUS_LIST = {"QUEUED","RUNNING"};
  i = 0
  while STATUS in STATUS_LIST:
      status_query = athena_resource.get_query_execution(QueryExecutionId = ID_QUERY)
      STATUS = status_query['QueryExecution']['Status']['State']
      if STATUS == "RUNNING":
          i = i + 1
          #print(STATUS, i, " segundos.")
          sleep(1)
  log_hub(path_log,"CREATE EXTERNAL TABLE" + STATUS)
  return True #("tabela:", tabela)

###########################################################################################################################

def create_table_athena(athena_resource, banco_dados, tabela, head, source, output, path_log=""):
  export_sep=';'
  #-----------------------------------------------------------------------------------------------------------------
  br = """

  """
  #-----------------------------------------------------------------------------------------------------------------
  query = "CREATE DATABASE IF NOT EXISTS " + banco_dados
  temp = athena_resource.start_query_execution( QueryString = query  ,    ResultConfiguration = {'OutputLocation': output}  )
  ID_QUERY = temp['QueryExecutionId']
  STATUS = "RUNNING"
  STATUS_LIST = {"QUEUED","RUNNING"};
  i = 0
  while STATUS in STATUS_LIST:
      status_query = athena_resource.get_query_execution(QueryExecutionId = ID_QUERY)
      STATUS = status_query['QueryExecution']['Status']['State']
      if STATUS == "RUNNING":
          i = i + 1
          #print(STATUS, i, " segundos.")
          sleep(1)
  log_hub(path_log,"CREATE DATABASE" + STATUS)
  #-----------------------------------------------------------------------------------------------------------------
  query = "DROP TABLE " + tabela
  temp = athena_resource.start_query_execution( QueryString = query  ,  ResultConfiguration = {'OutputLocation': output}  )
  ID_QUERY = temp['QueryExecutionId']
  STATUS = "RUNNING"
  STATUS_LIST = {"QUEUED","RUNNING"};
  i = 0
  while STATUS in STATUS_LIST:
      status_query = athena_resource.get_query_execution(QueryExecutionId = ID_QUERY)
      STATUS = status_query['QueryExecution']['Status']['State']
      if STATUS == "RUNNING":
          i = i + 1
          #print(STATUS, i, " segundos.")
          sleep(1)
  log_hub(path_log,"DROP TABLE" + STATUS)
  #-----------------------------------------------------------------------------------------------------------------
  query = ""
  query = query + " CREATE EXTERNAL TABLE IF NOT EXISTS " + tabela + " (" + br

  for i in range(0,len(head)):
    quota = ","
    if i == len(head)-1:
      quota = ""
    tipo = "string"
    query = query + "`" + head[i] + "` "+ tipo + quota + br

  query = query + " ) " + br
  query = query + " ROW FORMAT SERDE  " + br
  query = query + " 'org.apache.hadoop.hive.serde2.OpenCSVSerde'  " + br
  query = query + " WITH SERDEPROPERTIES (  " + br
  query = query + " 'escapeChar'='\\\\',  " + br
  query = query + " 'serialization.encoding'='iso-8859-1',  " + br
  query = query + " 'separatorChar'='\\" + export_sep + "')   " + br
  query = query + " LOCATION '" + source + "' "  + br
  query = query + " TBLPROPERTIES ( " + br
  query = query + " 'has_encrypted_data'='false', " + br
  query = query + " 'store.charset'='iso-8859-1' , " + br
  query = query + " 'retrieve.charset'='iso-8859-1' , " + br
  query = query + " 'skip.header.line.count'='1' " + br
  query = query + " );  " + br
  #print(query)
  #-----------------------------------------------------------------------------------------------------------------
  temp = athena_resource.start_query_execution( QueryString = query ,  ResultConfiguration = {'OutputLocation': output}  )
  ID_QUERY = temp['QueryExecutionId']
  STATUS = "RUNNING"
  STATUS_LIST = {"QUEUED","RUNNING"};
  i = 0
  while STATUS in STATUS_LIST:
      status_query = athena_resource.get_query_execution(QueryExecutionId = ID_QUERY)
      STATUS = status_query['QueryExecution']['Status']['State']
      if STATUS == "RUNNING":
          i = i + 1
          #print(STATUS, i, " segundos.")
          sleep(1)
  log_hub(path_log,"CREATE EXTERNAL TABLE" + STATUS)
  return("tabela:", tabela)

################################################################################

def retorna_tempo(tempo_processo):
  unidade = "segundos"
  if tempo_processo >= 60 and tempo_processo < 60*60:
    tempo_processo = tempo_processo / 60.00
    unidade = "minutos"
  if tempo_processo >= 60*60 and tempo_processo < 60*60*60:
    tempo_processo = tempo_processo / 3600.00
    unidade = "horas"
  return str(round(tempo_processo,1)) + " " + unidade  

################################################################################

def criar_diretorio_thread(thread_name, path_log = ""):
  
  try:
    
    if  thread_name[len(thread_name) -2 : len(thread_name)] == "//":
      thread_name = thread_name[0 :len(thread_name) -2 ]

    if thread_name[len(thread_name) -1 : len(thread_name)] == "/":
      thread_name =  thread_name[0 :len(thread_name) -1 ]

    if thread_name[0:1] == "/":
      thread_name =  thread_name[1 :len(thread_name) ]

    path_thread =  thread_name +"/"
    #print(path_thread)
    os.makedirs(path_thread)

    #log_hub(path_log,"Criando diretorio : " + path_thread)



  except Exception as e:
  #   print(e)
     #log_hub(path_thread,"Warning: criar_diretorio_thread : " + str(e))

     pass

################################################################################

def limpar_thread(path, path_log = "" ):
  if os.getcwd() != path:
      log_hub(path_log,"Limpando o diretorio : " + path)
      try:
        shutil.rmtree(path , ignore_errors=True)
      except:
        pass

################################################################################

def nome_base_from_file(file_name):

  fim_path = file_name.rfind('/') 
  if fim_path == -1 :
    fim_path = 0
  else:
    fim_path = fim_path + 1 
  file_name = file_name[ fim_path : len(file_name)]

  inicio = file_name.find('_', 1) 
  if inicio == -1 :
    inicio = 0
  else:
    inicio = inicio + 1 
  
  fim = file_name.find('.', 1) 
  if fim == -1 :
    fim = len(file_name)

  base = file_name[inicio : fim].lower()
  base = normalizar_string(base)

  return base

################################################################################

def return_qnt_datas(data_inicio,data_fim,step):
  if step == "anual":
    return relativedelta(data_inicio,data_fim).years  
  if step == "mensal":
    return relativedelta(data_inicio,data_fim).years*12 + relativedelta(data_inicio,data_fim ).months  
  if step == "diario":
    return relativedelta(data_inicio,data_fim).years*360 + relativedelta(data_inicio,data_fim ).months*30 + relativedelta(data_inicio,data_fim).days

def return_dt_fim(data_inicio,steps,tipo_step):
    if tipo_step == "anual":
        return data_inicio - relativedelta(years=steps)
    if tipo_step == "mensal":
        return data_inicio - relativedelta(months=steps)
    if tipo_step == "diario":
        return data_inicio - relativedelta(days=steps)


###################################################################################################################################################

def simples_download(setup_crawler,crawler_lib,endpoint, ano, mes, dia, download, tratamento, upload_s3, path_thread, limpar, criar_athena, path_log = "", crawler =  "" , bloco_id=1,total_blocos=1, Extracao = True, Upload_AWS = True):

  log_hub(path_log, "\n----------INICIO DO PROCESSO -------------------\n")
  global thread_running_simples_download
  thread_running_simples_download[endpoint] = 1

  status = False

  inicio_processo = time.time()
  
  criar_diretorio_thread(path_thread, path_log)
  url = setup_crawler.setup(endpoint,ano,mes,dia).get_config()["url_endpoint"]  
  
  _extracao = datetime.utcnow()
  dia_extracao, mes_extracao,ano_extracao  = _extracao.strftime("%d"), _extracao.strftime("%m"), _extracao.strftime("%Y")
  
  regiao_aws = config.aws_region
  bucket = config.aws_bucket
  
  zip_name = setup_crawler.setup(endpoint,ano,mes,dia).get_config()["zip_name"]


  path_s3_raw = "raw/" + crawler + "/" + endpoint + "/" + ano_extracao + "/" + mes_extracao + "/"+ dia_extracao + "/" + zip_name
  tipo_crawler = setup_crawler.setup().get_config()["tipo_crawler"]

  
  try:
    id_extracao = str(endpoint)  + " , " + str(ano) + " , "  +  str(mes) + " , "  + str(dia) + " , "  + str(download) + " , "  + str(tratamento) + " , "  + str(path_thread) 
    #log_hub(path_log,"************************************************************")
    #log_hub(path_log,"Inicio Download: " + id_extracao )
    #log_hub(path_log,"************************************************************")

    if download == True:

      if tipo_crawler == 'extrator_url':
        if Extracao == True:
            status = extrator_url_zip(url, zip_name, path_thread, path_log)
        else:
            log_hub(path_log, "** Sem efetuar o request de download, opcao do ususario **")
            status = True #check if exist local will be nice
      
      if tipo_crawler == 'extrator_python':
        try:
          if Extracao == True:
              status = crawler_lib.start_hub(path_thread,ano,mes,dia,endpoint)
              zipar(path_thread)
          else:
              log_hub(path_log, "** Sem efetuar o request de download, opcao do ususario **")
              status = True #check if existis will be nice
          
        except Exception as e:
          log_hub(path_log,"ERRO! "+str(e))
          print("Erro no crawler :",str(e))
          print(traceback.print_exc())
          pass 

      if status == True:
        try:
            if Upload_AWS:
                df_upload_file_to_s3(path_thread+zip_name, config.s3_resource, bucket, path_s3_raw, regiao_aws, path_log)
        except Exception as e:
            log_hub(path_log, "ERRO | Arquivo nao disponível : " + str(e))
            status = False
            pass


    else:
      status = True


    if tratamento == True:
      if status :
        data_extracao = get_time_now()
        #print(setup_crawler,crawler_lib,-1, upload_s3, False, data_extracao, url, endpoint , path_thread, path_s3_raw, path_log, crawler)
        try:
            upload_all_csvs(setup_crawler,crawler_lib,-1, upload_s3, False, data_extracao, url, endpoint , path_thread, path_s3_raw, path_log, crawler,Upload_AWS)
        except Exception as e:
            log_hub(path_log, "ERRO | Arquivo nao disponível : " + str(e))
            status = False
            pass
      else:
        status_hub(path_log,"ERRO | Site indisponivel : " + url + " | " +  id_extracao  )

    if (criar_athena == True):
      if status :
        data_extracao = get_time_now()
        log_hub(path_log,"Start geração Athena")
        upload_all_csvs(setup_crawler,crawler_lib,1000, False, True, data_extracao, url, endpoint , path_thread, path_s3_raw, path_log, crawler,Upload_AWS)
      else:
        status_hub(path_log,"ERRO | Site indisponivel : " + url + " | " +  id_extracao  + " | " + crawler   )

    if limpar == True:
      limpar_thread(path_thread, path_log)

  except Exception as e:

    if (str(e).count('Unable to locate credentials') > 0) | (str(e).count('Failed to upload') > 0) | (str(e).count('InvalidClientTokenId') > 0):
      log_hub(path_log, "ERRO | AWS indisponivel : AccessDenied | " + id_extracao + " | ")
      print("\n","AWS : Failed to upload / AccessDenied " )
      status = True
    else:
      exc_type, exc_obj, exc_tb = sys.exc_info()
      fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
      print(exc_type, fname, exc_tb.tb_lineno)
      print("Erro 5:",str(e))
      log_hub(path_log,"ERRO! "+str(e))
      print(traceback.print_exc())
      status = False
      

    pass

  log_hub(path_log,"")
  log_hub(path_log,"----------FIM DO PROCESSO -------------------")

  if status:
    status_hub(path_log,"CONCLUIDO | " + crawler + " | " +  id_extracao   )

  # coloquei para nao logar o athena ( variavel criar_athena )
  status_hub("./" + path_thread.split("/")[0] + "/" , ano + mes + dia + " | Status : " +  str(status) + " | Endpoint : " + endpoint + " | Duração : " + retorna_tempo(time.time() - inicio_processo) + " | Inicio : " + str(get_time_now()) + " | " + crawler  ,'a' , not(criar_athena) )

  ##################################################
  # bar of progress when finish
  if criar_athena == False:
    pass
    if status:
        msg = ano + mes + dia  #+ " | Encontrado | Duração : " + retorna_tempo(time.time() - inicio_processo)
    else:
        msg = ''

    ##########################################################
    # Bar Config
    total_jobs = 1
    size_checkpoint = func_size_checkpoint(total_jobs)
    ##########################################################

    if isnotebook():
        update_bar_process_dates(endpoint, msg, size_checkpoint, bloco_id, total_blocos)
    else:
        update_bar_process_dates(crawler.replace("crawler_",""), msg, size_checkpoint, bloco_id, total_blocos)
  ##################################################

  thread_running_simples_download[endpoint] = 0
  return status

###################################################################################################################################################

def upload_all_csvs(setup_crawler,crawler_lib,amostras, upload_s3, gerar_athena,data_extracao, url, endpoint, path_thread, path_s3_raw, path_log = "", crawler ="", Upload_AWS = True):

  files_csv_download = glob.glob( path_thread + '*.csv' )

  #log_hub(path_log,"")
  #log_hub(path_log,"||||||||||||||||||||||||||||||||||||||||||||||")
  #log_hub(path_log,"upload_all_csvs : " + str(files_csv_download) )

  # fazer isso ficar multithread

  for file_csv in files_csv_download:
    file_csv = file_csv.replace("\\","/")
    log_hub(path_log,"")
    #log_hub(path_log,"||||||||||||||||||||||||||||||||||||||||||||||")
    log_hub(path_log,"Normalizando arquivo : " + file_csv)

    #####################################################################
    # Config default

    if 'endpoint' not in locals():
        endpoint = ''
    if 'ano' not in locals():
        ano = ''
    if 'mes' not in locals():
        mes = ''
    if 'dia' not in locals():
        dia = ''
    if 'file_csv' not in locals():
        file_csv = ''


    path_aws = setup_crawler.setup(endpoint, ano, mes, dia, file_csv).get_config()["path_aws"]
    csv_encoding = setup_crawler.setup(endpoint, ano, mes, dia, file_csv).get_config()["csv_encoding"]
    csv_sep = setup_crawler.setup(endpoint, ano, mes, dia, file_csv).get_config()["csv_sep"]
    csv_doublequote = setup_crawler.setup(endpoint, ano, mes, dia, file_csv).get_config()["csv_doublequote"]
    base = setup_crawler.setup(endpoint, ano, mes, dia, file_csv).get_config()["base"]
    padrao_s3 = setup_crawler.setup(endpoint, ano, mes, dia, file_csv).get_config()["padrao_s3"]
    regiao_aws = config.aws_region
    bucket = config.aws_bucket
    s3_resource = config.s3_resource

    s = file_csv[file_csv.rfind('/') + 1 : len(file_csv)]

    if padrao_s3 == "diario":

      pattern_regex = '\d{4}\d{2}\d{2}'
      ano = re.findall(pattern_regex, s)[0][0:4]
      mes = re.findall(pattern_regex, s)[0][4:6]
      dia = re.findall(pattern_regex, s)[0][6:8]
      database = dia + "/" + mes +"/" + ano
      path_s3 = 'crawler' + "/" + crawler + "/" + base + "/" + ano + '/'  + mes + '/'+ dia + '/'  +  ano + mes + dia +  "_" + base + ".csv"

    if padrao_s3 == "mensal":

      pattern_regex = '\d{4}\d{2}'
      ano = re.findall(pattern_regex, s)[0][0:4]
      mes = re.findall(pattern_regex, s)[0][4:6]
      dia = ""
      database = "01/" + mes +"/" + ano
      path_s3 = 'crawler' + "/" + crawler + "/" + base + "/" + ano + '/'  +  mes + '/' + ano + mes + "_" + base + ".csv"
    
    if padrao_s3 == "anual":

      pattern_regex = '\d{4}'
      ano = re.findall(pattern_regex, s)[0]
      mes = ""
      dia = ""
      database = "01/01/" + ano
      path_s3 = 'crawler' + "/" + crawler + "/" + base + "/" + ano + '/'  +  ano +  "_" + base + ".csv"
    
    log_hub(path_log,"Nome da Base : " + base)
    status_hub(path_log,"Base Gerada : " + base + " | Database : " + ano + mes + dia + " | " + path_aws,"w", False)


    ### colocar para fazer essa operação multi-thread
    try:

        insert_header = setup_crawler.setup(endpoint, ano, mes, dia, file_csv).get_config()["insert_header"]
        layout_header = setup_crawler.setup(endpoint, ano, mes, dia, file_csv).get_config()["layout_header"]

        if insert_header:
            df = normalizar_csv_to_df(file_csv, database, path_aws, base, amostras, csv_encoding, csv_sep, csv_doublequote, data_extracao, url, path_s3, bucket, regiao_aws, path_s3_raw, path_log, padrao_s3, endpoint, insert_header, layout_header)
        else:
            df = normalizar_csv_to_df(file_csv, database, path_aws, base, amostras, csv_encoding, csv_sep,csv_doublequote, data_extracao, url, path_s3, bucket, regiao_aws, path_s3_raw,path_log, padrao_s3, endpoint)
    except:
        df = normalizar_csv_to_df(file_csv, database, path_aws, base, amostras, csv_encoding, csv_sep, csv_doublequote, data_extracao, url, path_s3, bucket, regiao_aws, path_s3_raw, path_log, padrao_s3, endpoint)

    # Salvando parquet
    #df.to_parquet(file_csv.replace(".csv",".parquet"), engine='pyarrow')


    if upload_s3 == True:

      # possibilidade de fazer o upload multithread
      df = df_upload_csv_to_s3(df, s3_resource, bucket, path_s3, regiao_aws, path_log, Upload_AWS )
      
    if gerar_athena:
        log_hub(path_log,"")
        #log_hub(path_log,"************************************************************")
        log_hub(path_log,"gerar athena:" + str(gerar_athena))
        #log_hub(path_log,"************************************************************")
        log_hub(path_log,"")

    if gerar_athena == True:
      
      banco_dados = config.aws_database_athena
      output_athena = config.aws_output_athena
      #tabela = "`" + banco_dados + "`.`hub_v1_" + crawler + "_" + base + "`"
      tabela_parquet = "`" + banco_dados + "`.`" + crawler + "_" + base + "`"
      source = "s3://" + bucket + "/" + 'crawler' + "/" + crawler + "/" + base + "/" 
      source_parquet = "s3://" + bucket + "/parquet/" + 'crawler' + "/" + crawler + "/" + base + "/"

      log_hub(path_log,"Criando tabela Athena" + tabela_parquet)
      #hub.create_table_athena(athena_resource, banco_dados, tabela, hub.df_head(df) , source, output_athena, path_log )
      create_table_athena_parquet(config.athena_client, banco_dados, tabela_parquet, df_head(df) , source_parquet, output_athena, path_log )

###################################################################################################################################################

def loop_extracao_multi_thread (setup_crawler,crawler_lib,crawler, endpoint, abrir_threads, data_inicio, qnt_datas, step_up, tipo_step, find_break, path_thread, limpar, bloco_id=1,total_blocos=1, Extracao = True, Upload_AWS = True):

  global thread_list_global
  thread_list_global[endpoint] = []

  #if qnt_datas == 0:
  #  qnt_datas = 1

  for i in range(0,qnt_datas):

    status = False

    if tipo_step == "anual":
      today = data_inicio + relativedelta(years=-i) + relativedelta(years=step_up) 
      ano = str(today.year).zfill(4) 
      mes = ""
      dia = ""

    if tipo_step == "mensal":
      today = data_inicio + relativedelta(months=-i) + relativedelta(months=step_up) 
      ano = str(today.year).zfill(4) 
      mes = str(today.month).zfill(2) 
      dia = ""

    if tipo_step == "diario":
      today = data_inicio + relativedelta(days=-i) + relativedelta(days=step_up)
      ano = str(today.year).zfill(4) 
      mes = str(today.month).zfill(2) 
      dia = str(today.day).zfill(2)  

    path_thread_data = path_thread + ano + mes + dia + "/"

    ##########print(path_thread_data)

    path_log = path_thread_data

    criar_diretorio_thread(path_thread_data,path_log)
    

    criar_athena = False # desligando o athena, fazer em loop depois

    #problema da primeira database vir sem nada, como pegar a primeira que contem dados
    #if i == 0:
    #  criar_athena = True
    #else:
    #  criar_athena = False

    global thread_running_simples_download
    thread_running_simples_download[endpoint] = -1

    if abrir_threads == True:
      t = threading.Thread(target=simples_download, args=(setup_crawler,crawler_lib,endpoint, ano, mes, dia, True , True, True, path_thread_data, limpar,criar_athena, path_log, crawler , bloco_id,total_blocos, Extracao, Upload_AWS ))
      t.name = endpoint
      thread_list_global[endpoint].append(t)
    
    else:

      status = simples_download(setup_crawler,crawler_lib,endpoint, ano, mes, dia, True , True, True, path_thread_data, limpar,criar_athena, path_log, crawler , bloco_id,total_blocos, Extracao , Upload_AWS )

      if status == True:
        if find_break == True:
          log_hub(path_log,"Dados encontrados, parando o processo")
          break

  if abrir_threads == True:

    # Starts threads
    for thread in thread_list_global[endpoint]:
        thread.start()

    # This blocks the calling thread until the thread whose join() method is called is terminated.
    for thread in thread_list_global[endpoint]:
        thread.join()


  
  return status

###################################################################################################################################################

def start_block_multi_thread (setup_crawler,crawler_lib,step_pagina, crawler, endpoint, abrir_threads, data_inicio, qnt_datas, step_up, tipo_step, find_break, path_thread, limpar, Extracao = True, Upload_AWS = True):

  global running_block_thread
  running_block_thread[endpoint] = 1

  if step_pagina == -1:
    step_pagina = qnt_datas 

  paginas = int(qnt_datas/step_pagina)

  if qnt_datas == step_pagina:
    paginas = paginas - 1

  for pagina in range(0,paginas +1 ):
    inferior = step_pagina*(pagina)
    superior = step_pagina*(pagina+1)

    for i in range(inferior, min(qnt_datas +1,superior+1)  ):

      if tipo_step == "anual":
        data = data_inicio + relativedelta(years=-i) + relativedelta(years=step_up) 
        padrao_data = "%Y"

      if tipo_step == "mensal":
        data = data_inicio + relativedelta(months=-i) + relativedelta(months=step_up) 
        padrao_data = "%Y%m"

      if tipo_step == "diario":
        data = data_inicio + relativedelta(days=-i) + relativedelta(days=step_up)
        padrao_data = "%Y%m%d"

      if i == inferior:
        inicio = data

    fim = data

    if abrir_threads == True:
      tipo_thread = "Multi-thread"
    else:
      tipo_thread = "Uni-thread"

    #print("")
    #print("---------------------------------------------------------------------------------------")
    #print("")
    #print("Processando :", crawler , endpoint, tipo_thread,  "de : ", fim.strftime(padrao_data)   , " até ",  inicio.strftime(padrao_data) , " - ",   max(return_qnt_datas(inicio, fim, tipo_step),1) , " Databases" )
    #print("")

    bloco_id = pagina

    if int(qnt_datas/step_pagina) == qnt_datas/step_pagina:
        total_blocos = paginas - 1
    else:
        total_blocos = paginas

    resposta_loop = loop_extracao_multi_thread(setup_crawler, crawler_lib, crawler, endpoint, abrir_threads, inicio,
                               max(return_qnt_datas(inicio, fim, tipo_step),1), 0, tipo_step, find_break, path_thread, limpar, bloco_id,total_blocos, Extracao , Upload_AWS )

    if (find_break == True) & (resposta_loop == True):
      break

  running_block_thread[endpoint] = 0

###################################################################################################################################################

def loop_processo(setup_crawler,crawler_lib,processo,historico, Extracao = True, Upload_AWS = True):

  thread_list = []
  #limpar_dados = False
  #data_inicio = datetime.today()

  # Thread control
  global bar_process_dates
  global running_block_thread
  global thread_running_simples_download
  global thread_bar_close




  bar_process_dates = {}
  running_block_thread = {}
  thread_running_simples_download = {}
  thread_bar_close = {}


  for i in range(0, len(processo)):
    #print("\n",processo[i])

    if processo[i][10] == True:
      limpar_thread(processo[i][1])

    criar_diretorio_thread(processo[i][1])

    if historico:
      processo[i][5] = return_qnt_datas(processo[i][4],processo[i][11], setup_crawler.setup(processo[i][2]).get_config()['padrao_s3'])+1 + processo[i][6]

    # False unithred por database (Mais lento, mas pega apeanas a data base mais recente no range), coloque True para ser multithread por database baixando todas as informações
    t = threading.Thread(target=start_block_multi_thread, args=(setup_crawler,crawler_lib,processo[i][0], processo[i][1], processo[i][2] , processo[i][3],processo[i][4], processo[i][5], processo[i][6], processo[i][7], processo[i][8], processo[i][9], processo[i][10], Extracao, Upload_AWS))
    #t.name = str(processo[i][1]) + "_" + str(processo[i][2])
    t.name = str(processo[i][2])
    running_block_thread[str(processo[i][2])] = -1
    if isnotebook():
        thread_bar_close[str(processo[i][2])] = -1




    thread_list.append(t)
    #print(t)

    ####################################################
    # barra de processo
    if isnotebook():
        plotar = plot_bar_process_dates(processo[i][5], processo[i][2])
    ####################################################

  # barra de processo: compatibilidade com terminais
  qnt_data = 0
  if not(isnotebook()):
       for i in range(0, len(processo)):
           qnt_data = qnt_data + processo[i][5]
           crawler = str(processo[i][1])

       thread_bar_close[crawler.replace("crawler_","")] = -1
       plotar = plot_bar_process_dates(qnt_data,crawler.replace("crawler_",""))
       #time.sleep(0.7)
  ####################################################

  # Starts threads
  #print(thread_list)
  for thread in thread_list:
      #time.sleep(0.05)
      thread.start()
      
  # This blocks the calling thread until the thread whose join() method is called is terminated.
  endpoints = []
  for i in range(0, len(processo)):
    endpoints.append(processo[i][2])

  # variaveis de controle de bloco
  count_endpoint, status_endpoint = {}, {}
  for endpoint in endpoints:
      count_endpoint[endpoint] = 0
      status_endpoint[endpoint] = 'Iniciado'

  global thread_list_global
  exit_loop = 0
  while (exit_loop == 0):
      time.sleep(0.1)
      # pode ser especificado um timeout do endpoint aqui
      # pode ser feito um count de qnts bases foram encontrada
      # pode ser feito a logica do find break de um bloco em processamento online

      try:
          # Valida se a thread já foi finalizada
          for endpoint in endpoints:
              if status_endpoint[endpoint] != 'OK':

                  # Contabiliza quantas threads do endpoint já finzalizaram no bloco
                  count_endpoint[endpoint] = str(thread_list_global[endpoint]).count(endpoint + ', stopped')
                  #print( endpoint , ':',str(thread_list_global[endpoint]).count(endpoint+ ', stopped'))
                  # Ve se a contagem de threads finalizadas é igual ao total de threads do endpoint
                  if (count_endpoint[endpoint] == int(str(thread_list_global[endpoint]).count(endpoint))) & (running_block_thread[endpoint] == 0 ) & (thread_running_simples_download[endpoint] == 0):
                      time.sleep(1)
                      if not(isnotebook()):
                        print("\n","\n", endpoint, " : finalizou o processo ","\n", "\n")  # ,
                      status_endpoint[endpoint] = 'OK'
                      if isnotebook():
                        plot_bar_process_dates_close(endpoint, qnt_data)

          contagem_threads_finalizadas = 0
          for endpoint in endpoints:
              contagem_threads_finalizadas = contagem_threads_finalizadas + count_endpoint[endpoint]


          if (int(str(thread_list_global[endpoint]).count('Thread')) == contagem_threads_finalizadas) & (running_block_thread[endpoint] == 0 ) :
              if not (isnotebook()):
                  try:
                    plot_bar_process_dates_close(crawler.replace("crawler_", ""),qnt_data)
                    status_endpoint[endpoint] = 'OK'
                  except Exception as e:
                      print(e)

          if not(isnotebook()) :
              if (thread_bar_close[crawler.replace("crawler_", "")] == 1):
                  exit_loop = 1

          if isnotebook():
              #print("\n")
              total_threads_abertas = 0
              total_threads_finalizadas = 0

              for i in thread_bar_close:
                  try:
                      total_threads_abertas = total_threads_abertas + 1
                      total_threads_finalizadas = total_threads_finalizadas + thread_bar_close[i]
                      #print(i, thread_bar_close[i],total_threads_abertas,total_threads_finalizadas)
                  except Exception as e:
                      print(e)
              if total_threads_abertas == total_threads_finalizadas:
                  exit_loop = 1

      except Exception as e:
          print(e)
          pass



  # # Garante display de fim do processo
  # if isnotebook():
  #   for i in range(0, len(processo)):
  #       qnt_data = processo[i][5]
  #       endpoint = str(processo[i][2])
  #       plot_bar_process_dates_close(endpoint, qnt_data)
  #   time.sleep(0.7)
  # else:
  #     qnt_data = 0
  #     for i in range(0, len(processo)):
  #         qnt_data = qnt_data + processo[i][5]
  #         crawler = str(processo[i][1])
  #     plot_bar_process_dates_close(crawler.replace("crawler_", ""), qnt_data)
  # ####################################################



  ###################################################################################################################################################

def setup_run(tipo_processo, data_inicio = "", data_fim = ""):
  from datetime import datetime
  if data_inicio == "":
    data_inicio = datetime.today()
  if tipo_processo == "entre_datas":
    Processar_Historico = False
  if tipo_processo == "historico":
    Processar_Historico = True
    data_fim = ""
  if tipo_processo == "default":
    Processar_Historico = False
    data_fim = ""
  return Processar_Historico, data_inicio, data_fim

###################################################################################################################################################

def get_job(setup_crawler,job):

  try:
    if os.getcwd().find("dados") == -1:
      try:
        os.mkdir("./dados/")
        os.chdir("./dados/")
      except Exception as e:
        print("Erro 6:",e)
        print(traceback.print_exc())
        pass
  except:
    pass
  
  #print("Diretorio de dados:",os.getcwd())
  exec('config_job = ' + job , globals(), _locals)
  #print(config_job)

  processos = setup_crawler.config_processo_default(config_job[1][1])
  lista_crawlers, Processar_Historico, data_inicio, data_fim = config_job[0] , config_job[1][0] , config_job[1][1] , config_job[1][2]
  #print("debug:",lista_crawlers, Processar_Historico, data_inicio, data_fim , processos )
  
  lista_processo = []
  processo = []
  i = 0

  for processo in processos:
    try:
      if config_job[2] == 'entre_datas':
        processo[6] = 0
    except:pass

    try:
      if config_job[2] == 'historico':
        if processo[4] == processo[11]:
            processo[11] = return_dt_fim(processo[4],processo[5],processo[7])
    except:pass
    try:
        if config_job[2] == 'historico':

          config_job[2] == 'entre_datas'
    except:pass


    if [processo[1],processo[2]] in lista_crawlers:
      lista_processo.append(processo)
      i = i + 1
  if (data_fim != ""):
    for i in range(0, len(lista_processo)):
      lista_processo[i][11] = data_fim
    Processar_Historico = True
  if (Processar_Historico == True):
    lista_processo[0][3] = True
    lista_processo[0][8] = False
  #print("lista_processo, Processar_Historico",lista_processo, Processar_Historico)

  if lista_processo == []:
    print("\n", "\n")
    print("Erro, processo nao configurado: " , config_job[0]  , "\n","\n")
    print("segue lista de processos disponiveis:", "\n", "\n")
    for processo in processos:
      print(processo)
    print("\n", "\n")
  
  return lista_processo, Processar_Historico
  
###################################################################################################################################################

#_locals = locals()
#exec('def set_rw(operation, name, exc): \n    import stat \n    os.chmod(name, stat.S_IWRITE) \n    return True \ndef git_clone(url): \n import os,shutil,base64 \n url="git clone " + url.replace("https://","https://" + base64.b64decode("YW1pbGNhci5zYW1wYWlv").decode("utf-8") + ":"+base64.b64decode("Q2dvNWpOaHBYeFZhZnNQb2IyR0o=").decode("utf-8")+"@") \n try: \n   try: \n    shutil.rmtree(os.getcwd().replace("\\\\","/") + "/" + url.split(".git")[0].split("/")[-1] + "/" , ignore_errors=False , onerror=set_rw) \n   except:pass \n   x = os.system(url) \n   if x != 0: \n    try: \n     shutil.rmtree(os.getcwd().replace("\\\\", "/") + "/" + url.split(".git")[0].split("/")[-1] + "/", ignore_errors=False, onerror=set_rw) \n    except:pass \n    os.system(url) \n except Exception as e: \n  print(e)', globals(), _locals)

def set_rw(operation, name, exc):
    import stat
    os.chmod(name, stat.S_IWRITE)
    return True

###################################################################################################################################################


def git_clone(url, gitlab_user="", gitlab_key=""):

 if gitlab_user == "":
     gitlab_user = config.gitlab_user

 if gitlab_key == "":
     gitlab_key = config.gitlab_key

 import os,shutil
 url = "git clone " + url.replace("https://", "https://" + gitlab_user + ":" + gitlab_key + "@")
 try:
   try:
    shutil.rmtree((os.getcwd().replace("\\","/") + "/" + url.split(".git")[0].split("/")[-1] + "/").replace("git clone ",""), ignore_errors=False , onerror=set_rw)
   except Exception as e:
    #print("Erro1:",e)
    pass
   #print(url)
   x = os.system(url)
   if x != 0:
    try:
     shutil.rmtree((os.getcwd().replace("\\", "/") + "/" + url.split(".git")[0].split("/")[-1] + "/").replace("git clone ",""), ignore_errors=False, onerror=set_rw)
    except Exception as e:
      #print("Erro2:",e)
      pass
    os.system(url)
 except Exception as e:
  #print("Erro3:",e)
  pass


###################################################################################################################################################

def get(crawler_name,retornar_setup = False):
  global e
  import os
  root_atual = os.getcwd()
  try:
    os.chdir(config.root_inicial)
  except:pass

  str_setup = str(crawler_name.replace('crawler', 'setup'))

  try:

    git_clone(config.gitlab_url + '/' + crawler_name + '.git')

  except:pass

  try:
      exec('del sys.modules["' + crawler_name + '"]')
  except:
      pass
  try:
      exec('del ' + crawler_name + '')
  except:
      pass

  try:
      exec('del sys.modules["' + crawler_name + '.crawler"]')
  except:
      pass
  try:
      exec('del ' + crawler_name + '.crawler')
  except:
      pass

  try:
      exec('del sys.modules["' + crawler_name + '.setup"]')
  except:
      pass
  try:
      exec('del ' + crawler_name + '.setup')
  except:
      pass

  exec('from ' + crawler_name  + ' import crawler as ' + crawler_name, globals(), _locals)
  exec('from ' + crawler_name  + ' import setup as ' + str_setup, globals(), _locals)
  #exec('e_now = endpoint('+ str_setup + '.setup.get_endpoints())', globals(), _locals)

  import glob
  crawlers = glob.glob('crawler_*')
  global lista
  lista = []
  for crawler in crawlers:
      crawler = crawler.replace("\\", "/")
      str_setup = str(crawler.replace('crawler', 'setup'))
      exec('from ' + crawler + ' import crawler as ' + crawler, globals(), _locals)
      exec('from ' + crawler + ' import setup as ' + str_setup, globals(), _locals)
      exec('end_points = ' + crawler.replace('crawler', 'setup') + '.setup.get_endpoints()', globals(), _locals)
      lista.append([crawler, end_points])
  exec('e = enpoint_crawler(lista)', globals(), _locals)

  #print("\n","Crawler: ", crawler,"|", crawler.replace('crawler','setup')," importado" ,"\n" )

  try:
    os.chdir(root_atual)
  except:pass

  if retornar_setup == True:
      import pandas as pd
      global __df
      str_setup = str(crawler_name.replace('crawler', 'setup'))
      exec('__df = pd.DataFrame(' + str_setup + '.config_processo_default(""))', globals(), _locals)
      __df.columns = ["max_threads", "crawler", "endpoint", "multi_thread", "data_inicio", "qnt_datas", "step_up",
                      "tipo_step", "find_break", "path_thread", "limpar", "inicio_historico"]
      return __df

###################################################################################################################################################

def gerar_tabelas_athena(crawler_name="", endpoint_name=""):

  global __endpoint, __ano, __mes, __dia, __path_thread, __path_log, __crawler

  import glob
  import time

  try:
    os.chdir("./dados/")
  except Exception as e:
    #print("Erro 10:",str(e))
    pass

  if config.aws_bucket != '':

      if not(isnotebook()):
        print("")
      print("--------------------------------------------------------------------------------")
      print("")
      print("Gerando tabelas athena...")
      print("")



      if endpoint_name == '':
        qnt_tabelas = len(sorted(glob.glob( crawler_name + '/[!_]*' )))
      else:
        qnt_tabelas = 1

      plot_bar_process_athena(qnt_tabelas)
      time.sleep(0.7)



      file = "_status.txt"
      sair = 0
      for folder in sorted(glob.glob( '*' )):
        #print(folder)

        if folder.count('.') == 0:
          try:
            sair = 0
            inicio_processo = time.time()
            for sub_folder in sorted(glob.glob( folder + '/*' ), reverse=True):

              crawler_atual = sub_folder.replace("\\","/").split("/")[0]
              endpoint_atual = sub_folder.replace("\\","/").split("/")[1]


              if (crawler_name != crawler_atual) & ( crawler_name != ""):
                  break

              #print(sub_folder + '/*')
              resposta = False
              for database in sorted(glob.glob( sub_folder + '/*' ), reverse=True):
                #print('database:',database , '|', crawler,endpoint)


                if database.count('.') == 0:
                    #print(database + "/" + file)


                    database = database.replace("\\","/")
                    if resposta:
                        break

                    with open( database + "/" + file) as f:
                        first_line = f.readline()
                        __status = first_line.split("|")[0].lstrip().rstrip()
                        __crawler = first_line.split("|")[1].lstrip().rstrip()

                    if __status == "CONCLUIDO":
                      k = 0
                      for i in first_line.split("|"):
                        if k==2:
                          __endpoint = i.split(",")[0].lstrip().rstrip()
                          __ano,__mes,__dia = '','',''
                          __ano = i.split(",")[1].lstrip().rstrip()
                          __mes = i.split(",")[2].lstrip().rstrip()
                          __dia = i.split(",")[3].lstrip().rstrip()
                          __path_thread = database + "/"
                          __path_log = __path_thread
                          check = False
                          if (crawler_name == "") | (__crawler == crawler_name):
                              if (endpoint_name == "") | (__endpoint == endpoint_name):
                                  check = True
                          if check:
                              if sair == 0:
                               try:
                                   exec( __crawler.replace("crawler","setup") + '=' + __crawler.replace("crawler","setup"),globals(), _locals)
                               except:
                                   get(__crawler)



                               #print("*******************************************")
                               #print("")
                               #print("Athena | " + __crawler + " | " + __endpoint + " | " + __path_thread, "\n")
                               exec('simples_download(' + __crawler.replace("crawler","setup") + ', ' + __crawler + ', __endpoint, __ano, __mes, __dia, False, False, False, __path_thread, False, True, __path_log, __crawler)',globals(), _locals)
                               resposta = True
                               # print(retorna_tempo(time.time() - inicio_processo))

                               ################################
                               # Update bar_athena
                               update_bar_process_athena(str(retorna_tempo(time.time() - inicio_processo)))
                               ################################



                               if (endpoint_name !=""):
                                    sair = 1
                               break

                        k= k + 1

          except Exception as e:
            print("Erro 3 :",e)
            pass
      plot_bar_process_athena_close()

  else:
    #print('Athena não processado, você não fez login na aplicação do hub')
    pass

###################################################################################################################################################

def run(crawler_name,endpoint="",inicio="",fim="", Athena=True, Extracao = True, Upload_AWS = True):

  # Lista global de variaveis para controle de threads
  global thread_list_global
  thread_list_global = {}
  get(crawler_name)

  print("")
  print("--------------------------------------------------------------------------------")
  print("\n", "Iniciando o download e tratamento dos dados:", "\n")
  
  try:
    # muda para o diretório default de dados
    os.chdir("./dados/")
  except Exception as e:
    #print("Erro 7:",str(e))
    pass

  # valida se o processo é do tipo run(c1,e1,"","") "data nao declarada"
  if (endpoint != "") & (inicio == ""):
    # Valida se o endpoint é um lista de valores
    if type(endpoint) == type([]):
      processo = ''
      k = 0
      for i in endpoint:
          k = k + 1
          quote = ','
          if k == len(endpoint):
              quote = ''
          processo = processo + "['" + crawler_name + "', '" + i + "']" + quote
      # processar o default de datas  de uma lista de endpoints
      job = "([" + processo + "], " + str(setup_run("default")) + ")"
      #print(job)
    else:
      # processar o default de datas de apenas um endpoint
      job = "([['" + crawler_name + "', '" + endpoint + "']], " + str(setup_run("default")) + ")"


    #configura o setup do processo
    job = job.replace('datetime.', '')
    exec('lista_processo, Processar_Historico = get_job(' + crawler_name.replace("crawler","setup") + ',"' + job + '")', globals(), _locals)
  
  else:
    # Processo é do tipo run(c1,e1,t1,t2) , pressupoe que se tem t1 tem t2
    if inicio != '':
      if type(inicio) == str:
        inicio = datetime.strptime(inicio, '%d/%m/%Y')
      if type(fim) == str:
        fim = datetime.strptime(fim, '%d/%m/%Y')
      # valida se o enpoint é uma lista de endpoints
      if type(endpoint) == type([]):
          processo = ''
          k = 0
          for i in endpoint:
              k = k + 1
              quote = ','
              if k == len(endpoint):
                  quote = ''
              processo = processo + "['" + crawler_name + "', '" + i + "']" + quote
          # processo entre datas de uma lista de endpoints
          job = "([" + processo + "], " + str(setup_run('entre_datas', fim, inicio)) + ",'entre_datas')"
          # print(job)
      else:
        # processo entre datas de apenas um endpoint
        job = "([['" + crawler_name + "', '" + endpoint + "']], " + str(setup_run('entre_datas', fim, inicio))  + ",'entre_datas')"

      # configura o setup do processo
      job = job.replace('datetime.', '')
      exec( crawler_name.replace("crawler","setup") + '.config_processo_default(' + 'datetime(' + str(fim.year) + ',' + str(fim.month) + ',' + str(fim.day) + ')' + ')' , globals(), _locals)
      exec('lista_processo, Processar_Historico = get_job(' + crawler_name.replace("crawler","setup") + ',"' + job + '")', globals(), _locals)
      
    else:
      # processo default do crawler
      exec('lista_processo, Processar_Historico = get_job(' + crawler_name.replace("crawler","setup") + ',str(' + crawler_name.replace("crawler","setup") + '.job_default()).replace("datetime.",""))', globals(), _locals)
      
    #print('lista_processo, Processar_Historico :' , lista_processo, Processar_Historico  )
    #print(crawler_name.replace("crawler","setup") + ',' + crawler_name)

  global __Extracao
  __Extracao =Extracao
  global __Upload_AWS
  __Upload_AWS = Upload_AWS

  # aqui é o start do processo apos as devidas configurações do setup
  exec('loop_processo(' + crawler_name.replace("crawler","setup") + ',' + crawler_name  + ',lista_processo,Processar_Historico,__Extracao,__Upload_AWS)', globals(), _locals)

  if Athena:
    if endpoint == '':
        gerar_tabelas_athena(crawler_name)
    else:
        gerar_tabelas_athena(crawler_name,endpoint)

  try:
    os.chdir(config.root_inicial)
  except Exception as e:
    #print("Erro 7:",str(e))
    pass

  print("\n", "--------------------------------------------------------------------------------")
  print("\n", "Fim do processo")
  print("\n", "--------------------------------------------------------------------------------")


###################################################################################################################################################


def run_history(crawler_name, endpoint="", athena=False, Extracao = True, Upload_AWS = True):

    # clona o git e captura o setup default do crawler em pandas
    processo = get(crawler_name, True)

    try:
        os.chdir("./dados/")
    except Exception as e:
        # print("Erro 7:",str(e))
        pass

    data_extracao = pd.to_datetime(get_time_now(), format='%d/%m/%Y, %H:%M:%S').strftime('%d/%m/%Y')

    # processar histórico de um enpoint específico
    if endpoint != "":

        # colocar run histoty de uma lista de endpoints, por enquanto apenas de um especifico

        try:
            data = processo[processo['endpoint'] == endpoint]['inicio_historico'].reset_index(drop=True)[0]
            if str(type(data)) == "<class 'pandas._libs.tslibs.nattype.NaTType'>":
                #print(crawler_name, endpoint)
                run(crawler_name, endpoint,"","",athena, Extracao , Upload_AWS )
            else:
                inicio_historico = processo[processo['endpoint'] == endpoint]['inicio_historico'].reset_index(drop=True)[0].strftime('%d/%m/%Y')
                #print(crawler_name, endpoint, inicio_historico, data_extracao)
                run(crawler_name, endpoint, inicio_historico, data_extracao,athena, Extracao , Upload_AWS )
        except Exception as e:
            print(e)
            pass

    # processar histórico de todos os endpoints do crawler
    else:

        lista_enpoints_default = []
        lista_enpoint_historico = {}
        blocos = {}
        datas = []
        for endpoints in processo['endpoint']:
            try:
                data = processo[processo['endpoint'] == endpoints]['inicio_historico'].reset_index(drop=True)[0]
                if str(type(data)) == "<class 'pandas._libs.tslibs.nattype.NaTType'>":
                    #print(crawler_name, endpoints)
                    lista_enpoints_default.append(endpoints)
                else:
                    inicio_historico = processo[processo['endpoint'] == endpoints]['inicio_historico'].reset_index(drop=True)[0].strftime('%d/%m/%Y')
                    #print(crawler_name, endpoints, inicio_historico, data_extracao)
                    blocos[endpoints] = [inicio_historico, data_extracao]
                    if inicio_historico not in datas:
                        datas.append(inicio_historico)

            except Exception as e:
                print(e)
                pass

        for j in range(0,len(datas)):
            lista_enpoint_historico[j] = []
        for i in blocos:
            lista_enpoint_historico[datas.index(blocos[i][0])].append(i)

        # Processos em que a base atual é a base historica ( so processar a base atual )
        print(crawler_name, lista_enpoints_default, "", "", athena, Extracao, Upload_AWS)
        run(crawler_name, lista_enpoints_default, "", "", athena, Extracao, Upload_AWS)

        # Processos historicos com data de inicio definida quebrando o processamento por blocos de datas
        for k in lista_enpoint_historico:
            # poderia ser feita uma funcao que quebrasse o range de datas comum ex 2014-2016 e 2016-2020 ( assim juntaria o endpoint com historico mais antigo no bloco e depois processaria so as datas faltantes )
            print(crawler_name,lista_enpoint_historico[k],datas[k],data_extracao,athena, Extracao , Upload_AWS )
            run(crawler_name, lista_enpoint_historico[k], datas[k], data_extracao, athena, Extracao, Upload_AWS)


    try:
        os.chdir(config.root_inicial)
    except Exception as e:
        # print("Erro 7:",str(e))
        pass



######################################################################
# APIS hub
######################################################################

class api_hub():
   def __init__(self):
    self.registro_civil = 'https://86qlnoporc.execute-api.us-east-1.amazonaws.com/prod-beta/registro-civil'
    self.patrimonio = 'https://86qlnoporc.execute-api.us-east-1.amazonaws.com/prod-beta/patrimonio'
    self.renda = 'https://86qlnoporc.execute-api.us-east-1.amazonaws.com/prod-beta/renda'
    self.renda_extrato = 'https://86qlnoporc.execute-api.us-east-1.amazonaws.com/prod-beta/renda-extrato'
    self.ocupacao = 'https://86qlnoporc.execute-api.us-east-1.amazonaws.com/prod-beta/ocupacao'
    self.documentos = ''
    self.enderecos = 'https://86qlnoporc.execute-api.us-east-1.amazonaws.com/prod-beta/local-trabalho'

api_hub = api_hub()

def get_json_api(json_,url_api):
    response = requests.post(
        url = url_api,
        headers = {'x-api-key':config.api_key},
        data = json.dumps(json_)
    )
    response = json.dumps(response.json(),indent=2)
    return response

def get_data_api_hub(filtros,api_hub,retorno="json" ):
    import concurrent.futures
    result = []
    futures = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for i in range(0, len(filtros)):
            futures.append(executor.submit(get_json_api, filtros[i],api_hub))
    for future in concurrent.futures.as_completed(futures):
        return_value = future.result()
        #print(return_value)
        if return_value.replace("[", "").replace("]", "") != '':
            result.append(json.loads(return_value.replace("[", "").replace("]", "")))
    if retorno == "pandas":
        import pandas as pd
        return  pd.DataFrame(result)
    if retorno =="json":
        return result

###########################################
# Quality check
def Check_QA_API(api_key):

    # acrescentar teste se o valor da busca aproximada é igual ao da busca exta ( to_do )
    try:
        config.api_key = api_key
        filtros = []
        filtros.append({"nome_completo": "SANDRO OLIVEIRA SIMAS", "primeiro_nome": "", "ultimo_nome": "", "cpf": "***4757****", "tipo_requisicao": "Exato"})
        json_output = get_data_api_hub(filtros,api_hub.registro_civil)
        valor_esperado = "[{'base_origem': 'GEOSAMPA-IPTU', 'cpf': '***4757****', 'data_referencia_informacao': '2020', 'endereco_imovel': 'AV  ALFREDO EGIDIO DE SOUZA ARANHA,255,AP 111,VILA CRUZEIRO,SP', 'hash_id': '7518438281999573379', 'nacionalidade_inferida': 'BRASILEIRA', 'nome_completo': 'SANDRO OLIVEIRA SIMAS', 'numero_do_contribuinte': '0874350138-9', 'primeiro_nome': 'SANDRO', 'sexo_inferido': 'MASCULINO', 'uf_regiao_fisical': 'None', 'ultimo_nome': 'SIMAS'}]"
        if str(json_output) == str(valor_esperado):

            print("\n","*****************************************")
            print("\n","Tudo certo com a API de Registro Civil =D","\n")
            print("*****************************************")
            #print("Encontrado : ",json_output)
            return True
        else:
            print("Valores de QA divergente :")
            print("Esperado   : ", valor_esperado)
            print("Encontrado : ", json_output)
            return False
    except: return False

def Check_QA_Crawler():
    try:
        run("crawler_registro_civil","nascimento_casamento_morte","01/01/2020","31/01/2020",False)
        df = get_data_frame_sql("select distinct hub_data_extracao from crawler_registro_civil_cadastro order by 1;")
        last_update, agora = pd.to_datetime(df['hub_data_extracao'], format='%d/%m/%Y, %H:%M:%S').max() , pd.to_datetime(get_time_now(), format='%d/%m/%Y, %H:%M:%S')
        delay = abs(last_update-agora).seconds
        if delay > 120:
            print(last_update, agora)
            print ("Erro na lib de Crawlers! ultima atualizacao as : ",last_update, "Delay de leitura : " , delay , " segundos")
            return False
        else:
            print("\n","\n","*****************************************")
            print("\n","Tudo certo com a lib de Crawlers! ultima atualizacao as : ",last_update, "Delay de leitura : " , delay , " segundos" ,"\n")

            return True
    except: return False


############################################################################
# Run crawler

#run("crawler_geosampa")

#run("crawler_registro_civil")
#run("crawler_registro_civil","nascimento_casamento_morte","01/01/2015","31/01/2015")

#run("crawler_portal_transparencia")
#run("crawler_portal_transparencia","peti","01/01/2020","13/01/2021")
#run("crawler_portal_transparencia","ceaf","","",False) # Rodar sem gerar a tabela athena

#run("crawler_portal_transparencia","peti")
#run("crawler_portal_transparencia","pep")
#run("crawler_portal_transparencia","ceaf")
#run("crawler_portal_transparencia","acordos_leniencia")
#run("crawler_portal_transparencia","cnpj")
#run_history("crawler_portal_transparencia")

#gerar_tabelas_athena()

############################################################################
# Get Crawler from Gitlab

#get(c.crawler_registro_civil)
#get(c.crawler_portal_transparencia)

############################################################################
## Athena

#gerar_tabelas_athena() # gerar todas as tabelas athena dos dados baixados
#gerar_tabelas_athena("crawler_registro_civil")
#gerar_tabelas_athena("crawler_registro_civil","nascimento_casamento_morte")
#gerar_tabelas_athena("crawler_portal_transparencia")
#gerar_tabelas_athena("crawler_portal_transparencia","ceaf")
#gerar_tabelas_athena("crawler_portal_transparencia","peti")

############################################################################



#run("crawler_tse","bem_candidato","01/01/2010","31/12/2010",False)

###########################################
# Quality check

#run_history("crawler_registro_civil")
#run_history("crawler_tse")
#run_history("crawler_portal_transparencia","ceaf")
#run_history("crawler_portal_transparencia")

#run("crawler_registro_civil")
#gerar_tabelas_athena("crawler_registro_civil")
# run("crawler_portal_transparencia")
# run("crawler_tse","bem_candidato")
# run("crawler_geosampa","iptu")

# Check_QA_Crawler()
# Check_QA_API(config.api_key)


# t = tabelas_hub()
# time.sleep(9999)

# print("fim")

