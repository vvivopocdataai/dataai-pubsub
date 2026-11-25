#!/usr/bin/env python3
"""
e2e_validation_test.py

Teste de validacao ponta a ponta do broker (Event Hubs + Schema Registry), usando
EXCLUSIVAMENTE autenticacao Entra ID (AzureCliCredential, sessao local do usuario
autenticado) -- sem SAS keys, sem connection strings, sem secrets.

Passos:
  1. Resolve (NAO re-registra) o schema "com.vivo.dataai.arrecadacao.PaymentMethodCreateEvent"
     no schema group "application-schemas" via get_schema_properties() -- confirma que
     o Schema Registry responde sem 401/403 (valida a role "Schema Registry Contributor"/
     "Schema Registry Reader" do principal autenticado).
  2. Monta um payload de teste fiel ao schema (espelho 1:1 de arrecadacao.forma_pgto) e
     codifica em Avro usando o AvroEncoder (embute o schemaId no content_type da EventData).
  3. Envia a mensagem codificada para o topico "evt-arrecadacao" via EventHubProducerClient
     -- valida a role de dados (Data Owner/Data Sender) sem 401/403.
  4. Le de volta via EventHubConsumerClient no consumer group "$Default" (nenhum consumer
     group novo foi criado, conforme solicitado), decodifica com o mesmo AvroEncoder e
     confirma que o payload recebido bate com o que foi enviado -- valida a role de
     recepcao (Data Owner/Data Receiver) sem 401/403.

Uso:
    python3 e2e_validation_test.py
"""
import datetime
import json
import os
import sys
import threading
import time
import uuid

NAMESPACE_FQDN = "eh-dataai-vivopoc.servicebus.windows.net"
SCHEMA_GROUP = "application-schemas"
EVENTHUB_NAME = "evt-arrecadacao"
CONSUMER_GROUP = "$Default"
SCHEMA_FQN = "com.vivo.dataai.arrecadacao.PaymentMethodCreateEvent"
SCHEMA_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "schemas",
                           "arrecadacao", "payment-method-create-event.avsc")

RECEIVE_TIMEOUT_SECONDS = 30


def is_auth_error(exc) -> bool:
    status_code = getattr(exc, "status_code", None)
    if status_code in (401, 403):
        return True
    msg = str(exc)
    return any(tok in msg for tok in ("status code: 401", "status code: 403", "(401)", "(403)", "ClientAuthenticationError", "Unauthorized", "Forbidden"))


def main():
    print(f"=== Teste E2E Broker (Entra ID, sem SAS/secrets) ===")
    print(f"Namespace:     {NAMESPACE_FQDN}")
    print(f"Schema group:  {SCHEMA_GROUP}")
    print(f"Event Hub:     {EVENTHUB_NAME}")
    print(f"Consumer grp:  {CONSUMER_GROUP}")
    print(f"Schema (FQN):  {SCHEMA_FQN}\n")

    from azure.identity import AzureCliCredential
    from azure.schemaregistry import SchemaRegistryClient
    from azure.schemaregistry.encoder.avroencoder import AvroEncoder
    from azure.eventhub import EventHubProducerClient, EventHubConsumerClient, EventData

    credential = AzureCliCredential()

    # ---- Passo 1: resolver o schema ja registrado (sem criar nova versao) ----
    print("[1/4] Resolvendo schema ja registrado no Schema Registry (get_schema_properties)...")
    with open(SCHEMA_FILE, "r", encoding="utf-8") as fh:
        schema_definition = fh.read()

    sr_client = SchemaRegistryClient(fully_qualified_namespace=NAMESPACE_FQDN, credential=credential)
    sr_client.__enter__()
    try:
        props = sr_client.get_schema_properties(
            group_name=SCHEMA_GROUP,
            name=SCHEMA_FQN,
            definition=schema_definition,
            format="Avro",
        )
        print(f"      OK  -> schemaId={props.id}  version={props.version}  group={props.group_name}")
        step1_ok = True
    except Exception as e:
        print(f"      FALHA ao resolver schema: {type(e).__name__}: {e}")
        if is_auth_error(e):
            print("      >>> Indicio de erro 401/403 (autenticacao/autorizacao) no Schema Registry.")
        sr_client.__exit__(None, None, None)
        return 1

    # ---- Monta payload de teste fiel ao schema (espelho de arrecadacao.forma_pgto) ----
    test_marker = str(uuid.uuid4())
    now_dt = datetime.datetime.now(datetime.timezone.utc)
    payload = {
        "eventId": test_marker,
        "eventType": "PaymentMethodCreateEvent",
        "eventTime": now_dt,
        "domain": "Commercial",
        "event": {
            "id_forma": f"TEST-E2E-{test_marker[:8]}",
            "tp_forma": "PIX",
            "nr_final_cartao": None,
            "ds_bandeira": None,
            "fl_preferencial": True,
            "id_conta": "TEST-ACC-0001",
            "dt_atualizacao": now_dt,
        },
    }
    print(f"\nPayload de teste (marcador unico: {test_marker}):")
    print(json.dumps(payload, indent=2, ensure_ascii=False, default=str))

    # ---- Passo 2: codificar em Avro via AvroEncoder (usa o Schema Registry) ----
    print("\n[2/4] Codificando payload em Avro via AvroEncoder...")
    encoder = AvroEncoder(client=sr_client, group_name=SCHEMA_GROUP, auto_register=False)
    try:
        event_data = encoder.encode(payload, schema=schema_definition, message_type=EventData)
        print(f"      OK  -> content_type={event_data.content_type}")
    except Exception as e:
        print(f"      FALHA ao codificar: {type(e).__name__}: {e}")
        if is_auth_error(e):
            print("      >>> Indicio de erro 401/403 no Schema Registry.")
        sr_client.__exit__(None, None, None)
        return 1

    # ---- Passo 3: enviar para o Event Hub ----
    print(f"\n[3/4] Enviando para o topico '{EVENTHUB_NAME}' (EventHubProducerClient, Entra ID)...")
    producer = EventHubProducerClient(fully_qualified_namespace=NAMESPACE_FQDN, eventhub_name=EVENTHUB_NAME, credential=credential)
    try:
        with producer:
            batch = producer.create_batch()
            batch.add(event_data)
            producer.send_batch(batch)
        print("      OK  -> mensagem enviada sem erro (plano de dados aceitou a escrita).")
    except Exception as e:
        print(f"      FALHA ao enviar: {type(e).__name__}: {e}")
        if is_auth_error(e):
            print("      >>> Indicio de erro 401/403 no plano de dados do Event Hubs (producer).")
        return 1

    # ---- Passo 4: ler de volta via consumer group $Default ----
    print(f"\n[4/4] Lendo de volta via consumer group '{CONSUMER_GROUP}' (ate {RECEIVE_TIMEOUT_SECONDS}s)...")
    found = {"ok": False, "decoded": None, "error": None}
    stop_event = threading.Event()

    def on_event(partition_context, event):
        if event is None:
            return
        try:
            decoded = encoder.decode(event)
        except Exception as decode_exc:
            found["error"] = decode_exc
            return
        if decoded.get("eventId") == test_marker:
            found["ok"] = True
            found["decoded"] = decoded
            stop_event.set()

    consumer = EventHubConsumerClient(
        fully_qualified_namespace=NAMESPACE_FQDN,
        eventhub_name=EVENTHUB_NAME,
        consumer_group=CONSUMER_GROUP,
        credential=credential,
    )
    try:
        with consumer:
            t = threading.Thread(
                target=lambda: consumer.receive(on_event=on_event, starting_position="-1", max_wait_time=RECEIVE_TIMEOUT_SECONDS),
                daemon=True,
            )
            t.start()
            stop_event.wait(timeout=RECEIVE_TIMEOUT_SECONDS)
            consumer.close()
            t.join(timeout=5)
    except Exception as e:
        print(f"      FALHA ao consumir: {type(e).__name__}: {e}")
        if is_auth_error(e):
            print("      >>> Indicio de erro 401/403 no plano de dados do Event Hubs (consumer).")
        sr_client.__exit__(None, None, None)
        return 1

    sr_client.__exit__(None, None, None)

    if found["error"] is not None:
        print(f"      FALHA ao decodificar mensagem recebida: {found['error']}")
        return 1

    if not found["ok"]:
        print(f"      FALHA: nao recebeu de volta a mensagem com marcador {test_marker} dentro de {RECEIVE_TIMEOUT_SECONDS}s.")
        return 1

    print(f"      OK  -> mensagem recebida e decodificada, eventId confere: {found['decoded']['eventId']}")
    print("\nPayload decodificado recebido:")
    print(json.dumps(found["decoded"], indent=2, ensure_ascii=False, default=str))

    match = (
        found["decoded"]["event"]["id_forma"] == payload["event"]["id_forma"]
        and found["decoded"]["event"]["tp_forma"] == payload["event"]["tp_forma"]
        and found["decoded"]["event"]["fl_preferencial"] == payload["event"]["fl_preferencial"]
    )

    print("\n=== RESUMO ===")
    print(f"1. Schema Registry (resolver schema):        {'OK' if step1_ok else 'FALHA'}")
    print(f"2. Codificacao Avro:                          OK")
    print(f"3. Envio ao Event Hub (producer, Entra ID):   OK")
    print(f"4. Leitura via consumer group $Default:       OK")
    print(f"5. Conteudo recebido == conteudo enviado:     {'OK' if match else 'DIVERGENTE'}")
    print("6. Nenhum erro 401/403 em nenhuma etapa:       OK")
    return 0 if match else 2


if __name__ == "__main__":
    sys.exit(main())
