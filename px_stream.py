#!/usr/bin/env python3
import boto3
import os
import json
import time
import http.client
from operator import itemgetter
from datetime import datetime, timedelta
# Duração da busca em minutos
search_range = 5

def enviar_mensagem_slack(acc, events, amount, cents):
    SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK")
    split_webhook = SLACK_WEBHOOK.split("/")
    webhook_url = f"/{'/'.join(split_webhook[-4:])}"
    host = split_webhook[2]
    # Conteúdo da mensagem
    texto_mensagem = f"*ACC*: {acc}\n*Quantidade de eventos*: {events}\n*Montante dos eventos*: ${amount} ({cents})"
    # Payload com layout profissional
    payload = {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "Relatório de Eventos Pix CashOut",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Eventos identificados nos ultimos 3 minutos:*\n" + texto_mensagem
                }
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "plain_text",
                        "text": "Informação gerada automaticamente pelo lambda infosec-alert-pix-prod-cashout.",
                        "emoji": True
                    }
                ]
            }
        ]
    }
    # Configurar conexão HTTP
    conn = http.client.HTTPSConnection(host)
    headers = {'Content-type': 'application/json'}
    # Enviar a mensagem
    conn.request("POST", webhook_url, json.dumps(payload), headers)
    response = conn.getresponse()
    # Fechar conexão
    conn.close()

def write_int_to_s3_file(value):
    bucket_name = 'bucket-name-here'
    file_name = 'lastId.txt'
    # Criar um cliente S3
    s3_client = boto3.client('s3')

    try:
        # Converter o valor para string
        value_str = str(value)
        # Escrever o conteúdo no arquivo S3
        s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=value_str)
    except Exception as e:
        print(f"Erro ao escrever no arquivo: {e}")

def read_last_id_from_s3():
    bucket_name = 'infosec-workloads-info'
    file_name = 'lastId.txt'
    # Criar um cliente S3
    s3_client = boto3.client('s3')
    try:
        # Obter o objeto do S3
        obj = s3_client.get_object(Bucket=bucket_name, Key=file_name)
        # Ler o conteúdo do objeto
        file_content = obj['Body'].read().decode('utf-8')
        # Converter para inteiro e retornar
        return int(file_content.strip())
    except Exception as e:
        print(f"Erro ao ler o arquivo: {e}")
        return None
        
def centavos_para_reais(centavos):
    # Converter centavos para reais
    reais = centavos / 100
    # Formatar o valor para o formato desejado
    valor_formatado = f'{reais:,.2f}'
    return valor_formatado

def timestamp_para_data_legivel(timestamp):
    # Converter milissegundos em segundos
    segundos = timestamp / 1000.0
    # Converter o timestamp para um objeto datetime
    data_hora = datetime.fromtimestamp(segundos)
    # Formatar a data e hora de forma legível
    data_formatada = data_hora.strftime('%d/%m/%Y %H:%M:%S')
    return data_formatada

def get_aws_events(log_group_name):
    client = boto3.client('logs')
    # Calculando intervalo de tempo
    tempo_atual = datetime.now()
    tempo_inicial = tempo_atual - timedelta(minutes=search_range)
    timestamp_inicial = int(time.mktime(tempo_inicial.timetuple()) * 1000)
    timestamp_final = int(time.mktime(tempo_atual.timetuple()) * 1000)
    response = client.filter_log_events(
        logGroupName=log_group_name,
        startTime=timestamp_inicial,
        endTime=timestamp_final
    )
    eventos = response.get('events', [])
    # Adiciona o log group name a cada evento
    for evento in eventos:
        evento['logGroupName'] = log_group_name
    return response.get('events', [])

def process_event(evento):
    # Extraia e processe informações de cada evento
    try:
        message_content = evento['message'].split('\t')[-1].strip()
        apigateway_request_id = evento['message'].split('\t')[1]
        message_json = json.loads(message_content)
    except:
        # Se o JSON não puder ser decodificado, pule este evento
        return None

    # Extraia informações do evento (ajuste conforme a estrutura do seu log)
    log_stream_name = evento['logStreamName']
    timestamp = evento['timestamp']
    ingestion_time = evento['ingestionTime']
    event_id = evento['eventId']
    log_group = evento['logGroupName']
    try:
        session_status = message_json['Session']
    except:
        return None

    if 'Payload' in message_json:
        pretty_event = evento
        pretty_event['message'] = message_json
        pretty_event['apigatewayRequestId'] = apigateway_request_id
        return pretty_event
    else:
        return None

def lambda_handler(event, context):
    # Lista de eventos pix validos (cashin e cashout)
    verified_pix_event_list = []
    pix_cashout_events = []
    target_acc_events = []
    pix_cashout_events_amount = 0
    # Defina os nomes dos log groups
    log_group_list = ["/aws/lambda/loggroup-with-pix-payload"]
    target_acc_amount = 0
    target_acc_name = ["23463567342345", "23463123442346", "23123467342347"]
    last_saved_event = read_last_id_from_s3()
    eventos_combinados = []
    # Obtenha eventos de ambos os log groups
    for log_name in log_group_list:
        eventos_combinados += get_aws_events(log_name)
    
    eventos_combinados.sort(key=lambda x: x['timestamp'])
    # Processar eventos combinados
    for evento in eventos_combinados:
        pix_event = process_event(evento)
        if pix_event != None:
            verified_pix_event_list.append(pix_event)
            # Obter os dados do evento
            log_stream_name = pix_event['logStreamName']
            timestamp = pix_event['timestamp']
            ingestion_time = pix_event['ingestionTime']
            event_id = pix_event['eventId']
            event_msg = pix_event['message']
            log_group = pix_event['logGroupName']
            api_request_id = pix_event['apigatewayRequestId']
            # Obter os dados do payload
            event_payload = event_msg['Payload']['body']
            moment = timestamp_para_data_legivel(timestamp)
            p_key = list(event_payload.keys())[0]
    
            # Fluxo para eventos Pix.
            if p_key == 'transactionId' and log_group in log_group_list:
                event_amount = event_payload.get('amount', 'Amount does not exist')
                transaction_id = event_payload.get('transactionId', 'Transaction ID does not exist')
                receiver_key_value = event_payload.get('receiverKeyValue', 'Receiver key value does not exist')
                qr_code_identifier = event_payload.get('qrCodeIdentifier', 'QR Code does not exist')
                scheduled_date = event_payload.get('scheduledDate', 'Scheduled date does not exist')
                description = event_payload.get('description', 'Description does not exist')
                acc_name = event_msg.get('Session').get('accountName', '')
                session_id = event_msg.get('Session').get('sessionId', '')
    
                pix_cashout_events_amount += event_amount
                pix_cashout_events.append(pix_event)
                # Fluxo para eventos Pix da conta monitorada.
                if acc_name in target_acc_name and int(event_id) > last_saved_event:
                    # adicione o evento a lista de target_acc_events
                    target_acc_events.append(pix_event)
                    target_acc_amount += event_amount
    
    # Contabilizar os dados finais do eventos pix
    cashout_amount = centavos_para_reais(pix_cashout_events_amount) # Valor de todas as transacoes analisadas
    target_amount = centavos_para_reais(target_acc_amount) # Valor das transacoes analisadas da referente a conta monitorada
    last_id = read_last_id_from_s3()

    if target_acc_amount > 0:
        enviar_mensagem_slack(target_acc_name, len(target_acc_events), target_amount, target_acc_amount)
        write_int_to_s3_file(event_id)
        
