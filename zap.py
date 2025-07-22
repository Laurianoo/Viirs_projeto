import pyautogui
import requests
import pyperclip
import time
import random
import logging
import json
import subprocess
from logging.handlers import RotatingFileHandler
import datetime
import os

def setup_logging():
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
    log_file = os.path.join("logs", "execucao.log")
    handlers = [
        RotatingFileHandler(
            log_file,
            maxBytes=5*1024*1024,
            backupCount=3,
            encoding="utf-8"),
        logging.StreamHandler()]
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=handlers)

def mover_mouse():
    t = 0
    while t<10:
        x = random.randint(200, 1600)
        y = random.randint(150, 950)
        pyautogui.moveTo(x, y, duration=0.5)
        time.sleep(1)
        t+=1
    logging.info("Movimentação do mouse concluída.")

def salvar_estado(dia, focos):
    """Salva o dia e as ultimas detecções"""
    caminho_config = os.path.join("config", "mensagens_enviadas.json")
    os.makedirs("config", exist_ok=True)
    with open(caminho_config, "w") as f:
        json.dump({"dia": dia, "ultimos_focos": focos}, f)

def carregar_estado():
    """Carrega o dia e as ultimas detecções"""
    caminho_config = os.path.join("config", "mensagens_enviadas.json")
    try:
        with open(caminho_config, "r") as f:
            dados = json.load(f)
            return dados["dia"], dados["ultimos_focos"]
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        return 0, []

def hora_envio(hora_envio_str):
    hora_atual_str = time.strftime("%H:%M")
    logging.info(f"Hora de envio programada para: {hora_envio_str}")
    logging.info(f"Hora atual: {hora_atual_str}. Aguardando {hora_envio_str}...")
    while True:
        hora_atual_str = time.strftime("%H:%M")
        if hora_atual_str == hora_envio_str:
            logging.info("Hora de enviar a mensagem!")
            break
        time.sleep(60)

def formatar_mensagem(mensagem):
    try:
        pyperclip.copy(mensagem)
        logging.info("Mensagem copiada para a área de transferência!")
    except Exception as e:
        logging.error(f"Erro ao formatar/copiar a mensagem via pyautogui: {e}")


def enviar_mensagem(WHAPI_TOKEN, id_do_grupo, mensagem):
    """
    Envia mensagem para um grupo do WhatsApp usando a API Whapi Cloud
    
    Parâmetros:
    id_do_grupo (str): ID do grupo no formato 5511999999999-1234567890@g.us
    mensagem (str): Texto da mensagem a ser enviada
    """
    url = "https://gate.whapi.cloud/messages/text"
    headers = {
        "Authorization": f"Bearer {WHAPI_TOKEN}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "to": id_do_grupo,
        "body": mensagem
    }
    
    try:
        response = requests.post(url, json=payload, headers=headers)
        
        # Verifica se a mensagem foi entregue com sucesso
        if response.status_code == 200:
            logging.info(f"Mensagem enviada com sucesso para o grupo {id_do_grupo}")
            logging.info(f"ID da mensagem: {response.json().get('id')}")
        else:
            error_info = response.json().get('errors', [{}])[0]
            logging.error(f"Erro na API: {error_info.get('title')} - {error_info.get('details')}")
            logging.error(f"Código de status: {response.status_code}")
        
    except Exception as e:
        logging.error(f"Erro ao enviar mensagem: {e}")
        logging.error(f"Resposta completa: {response.text if 'response' in locals() else 'N/A'}")
