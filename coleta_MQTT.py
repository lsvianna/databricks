# Databricks notebook source
# MAGIC %md
# MAGIC 1. Coleta MQTT
# MAGIC
# MAGIC Receber mensagens do equipamento e armazená-las como JSON em um volume. O coletor funciona durante 60 segundos e termina.

# COMMAND ----------

# MAGIC %pip install paho-mqtt==2.1.0 --quiet

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração e Volume

# COMMAND ----------

# Cria o volume para recebimento dos dados
catalogo = "senai"
esquema = "streaming"
volume = "medallion_mqtt"
pasta = f"/Volumes/{catalogo}/{esquema}/{volume}/entrada"

# Parâmetros do servidor mqtt
broker = "189.8.205.50"
porta = 1883
topico = "IoT_1"
segundos = 60
usuario = "alunoblu"
senha = "HotDogComTorresmo"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalogo}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalogo}.{esquema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalogo}.{esquema}.{volume}")

import os
os.makedirs(pasta, exist_ok=True)
print("Pasta de entrada:", pasta)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Conectar ao servidor e receber mensagens
# MAGIC
# MAGIC Cada mensagem fica registrada com horário de recebimento, tópico e conteúdo original (`payload`).

# COMMAND ----------

import json
import time
from datetime import datetime, timezone
import paho.mqtt.client as mqtt

mensagens = []

def ao_conectar(cliente, userdata, flags, codigo, propriedades):
    print("Conexão MQTT:", codigo)
    if not codigo.is_failure:
        cliente.subscribe(topico)

def ao_receber(cliente, userdata, mensagem):
    mensagens.append({
        "recebido_em": datetime.now(timezone.utc).isoformat(),
        "topico": mensagem.topic,
        "payload": mensagem.payload.decode("utf-8")
    })

cliente = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
cliente.on_connect = ao_conectar
cliente.on_message = ao_receber
cliente.username_pw_set(usuario, senha)

cliente.connect(broker, porta)
cliente.loop_start()
time.sleep(segundos)
cliente.disconnect()
cliente.loop_stop()

if not mensagens:
    raise ValueError("Nenhuma mensagem recebida. Verifique rede, tópico e autenticação.")

arquivo = f"{pasta}/mqtt_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}.json"
with open(arquivo, "w", encoding="utf-8") as saida:
    for mensagem in mensagens:
        saida.write(json.dumps(mensagem, ensure_ascii=False) + "\n")

print("Mensagens recebidas:", len(mensagens))
print("Arquivo gravado:", arquivo)