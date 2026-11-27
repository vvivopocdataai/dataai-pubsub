#!/usr/bin/env python3
"""
clean_topics.py

"Limpa" (zera o conteudo de) os Event Hubs/topicos do namespace eh-dataai-vivopoc,
usando exclusivamente autenticacao Entra ID (AzureCliCredential / DefaultAzureCredential)
-- sem connection strings, sem SAS keys, sem secrets.

POR QUE DELETE + RECREATE E NAO UM "PURGE":
Azure Event Hubs (como o Kafka) e um log append-only. Nao existe uma API de
"apagar as mensagens X" ou "truncar o topico" -- as mensagens expiram sozinhas
so quando a retencao vence. A UNICA forma confiavel de zerar o conteudo de um
Event Hub e deletar e recriar a entidade com as MESMAS configuracoes (numero de
particoes e retencao), o que e exatamente o que este script faz, preservando
as configuracoes atuais de cada topico antes de apagar.

ATENCAO -- OPERACAO DESTRUTIVA:
- Apaga TODAS as mensagens de cada topico listado (inclusive qualquer backlog
  real de producao, nao so dados de teste).
- Os offsets/posicoes de leitura de qualquer consumidor em andamento ficam
  invalidos (o Event Hub recriado comeca do zero).
- NAO mexe no schema group nem nos schemas do Schema Registry -- so nos topicos
  (Event Hubs) em si.
- Por padrao, NAO inclui o Event Hub "events" (piloto/legado, decisao de
  descomissionamento ainda pendente) -- use --include-events para incluir.

ACHADO IMPORTANTE (verificado antes de este script ser escrito): os 9 topicos
criados para o P2 estao hoje com retencao real de ~28h (retentionDescription.
retentionTimeInHours=28), NAO os 7 dias (168h) configurados na criacao via ARM
-- aparentemente o campo "messageRetentionInDays" enviado na chamada nao foi o
efetivo. Por padrao este script PRESERVA a retencao atual (ainda que incorreta)
ao recriar; use --retention-days 7 para corrigir para 7 dias durante a limpeza.

Pre-requisitos:
    pip install azure-identity azure-mgmt-eventhub

RBAC necessario: Contributor (ou Azure Event Hubs Data Owner nao basta -- e
operacao de CONTROL PLANE/ARM) no namespace ou no resource group.

Uso:
    python3 clean_topics.py --dry-run                 # so mostra o que seria feito
    python3 clean_topics.py                            # pede confirmacao interativa
    python3 clean_topics.py --yes                      # sem confirmacao (ex.: CI)
    python3 clean_topics.py --topics evt-billing,evt-crm  # so os topicos informados
    python3 clean_topics.py --include-events           # inclui tambem o "events"
    python3 clean_topics.py --retention-days 7         # tambem corrige a retencao para 7d
    python3 clean_topics.py --retention-hours 6        # retencao em horas (mais granular que dias)
"""
import argparse
import sys
import time

SUBSCRIPTION_ID = "afee2beb-85dc-47f8-ac49-b62198aefc7b"
RESOURCE_GROUP = "az-rg-vivopoc-dataai"
NAMESPACE = "eh-dataai-vivopoc"

# Os 9 topicos criados para o P2 (ver PLANO_P2_DATA_CONTRACTS_SCHEMA_REGISTRY.md secao 3).
# "events" e o piloto/legado pre-existente -- excluido por padrao de proposito.
DEFAULT_TOPICS = [
    "evt-arrecadacao",
    "evt-billing",
    "evt-crm",
    "evt-notificacao",
    "evt-tmf-productorder",
    "evt-tmf-productinventory",
    "evt-tmf-productcatalog",
    "evt-customer-ops",
    "evt-ops",
]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--topics", default=None,
                         help="Lista separada por virgula dos topicos a limpar. Default: os 9 topicos do P2.")
    parser.add_argument("--include-events", action="store_true",
                         help="Inclui tambem o Event Hub 'events' (piloto/legado). Ignorado se --topics for informado.")
    parser.add_argument("--dry-run", action="store_true", help="So mostra o que seria feito, sem chamar o Azure.")
    parser.add_argument("--yes", action="store_true", help="Pula a confirmacao interativa (ex.: uso em CI).")
    parser.add_argument("--retention-days", type=int, default=None,
                         help="Se informado, corrige a retencao para N dias ao recriar, em vez de preservar o valor atual.")
    parser.add_argument("--retention-hours", type=int, default=None,
                         help="Se informado, corrige a retencao para N horas ao recriar (mais granular que --retention-days; "
                              "tem prioridade se os dois forem informados).")
    args = parser.parse_args()

    if args.retention_days is not None and args.retention_hours is not None:
        print("Aviso: --retention-days e --retention-hours informados juntos -- usando --retention-hours.")

    if args.topics:
        topics = [t.strip() for t in args.topics.split(",") if t.strip()]
    else:
        topics = list(DEFAULT_TOPICS)
        if args.include_events:
            topics.append("events")

    print("=== clean_topics.py -- limpeza de Event Hubs via delete+recreate ===")
    print(f"Namespace:   {NAMESPACE}")
    print(f"Resource grp: {RESOURCE_GROUP}")
    print(f"Topicos ({len(topics)}): {', '.join(topics)}\n")

    if args.dry_run:
        print("[dry-run] Nenhuma chamada sera feita ao Azure. Para cada topico acima, o script iria:")
        print("  1. Ler partitionCount e retencao atuais (GET)")
        print("  2. Apagar o Event Hub (DELETE) -- remove todas as mensagens")
        print("  3. Recriar o Event Hub com as MESMAS configuracoes (PUT)")
        return 0

    if not args.yes:
        resp = input(f"Isso vai APAGAR TODAS AS MENSAGENS de {len(topics)} topico(s): "
                     f"{', '.join(topics)}.\nTem certeza? Digite 'sim' para confirmar: ")
        if resp.strip().lower() not in ("sim", "yes", "y"):
            print("Cancelado.")
            return 1

    from azure.identity import AzureCliCredential
    from azure.mgmt.eventhub import EventHubManagementClient
    from azure.mgmt.eventhub.models import Eventhub, RetentionDescription

    credential = AzureCliCredential()
    client = EventHubManagementClient(credential, SUBSCRIPTION_ID)

    ok_count, fail_count = 0, 0
    for name in topics:
        try:
            current = client.event_hubs.get(RESOURCE_GROUP, NAMESPACE, name)
            partition_count = current.partition_count
            current_hours = current.retention_description.retention_time_in_hours if current.retention_description else None
            print(f"[{name}] configuracao atual: {partition_count} particoes, "
                  f"retentionTimeInHours={current_hours} (message_retention_in_days={current.message_retention_in_days})")

            if args.retention_hours is not None:
                target_hours = args.retention_hours
                if current_hours != target_hours:
                    print(f"[{name}] retencao sera corrigida: {current_hours}h -> {target_hours}h")
            elif args.retention_days is not None:
                target_hours = args.retention_days * 24
                if current_hours != target_hours:
                    print(f"[{name}] retencao sera corrigida: {current_hours}h -> {target_hours}h ({args.retention_days}d)")
            else:
                target_hours = current_hours

            client.event_hubs.delete(RESOURCE_GROUP, NAMESPACE, name)
            print(f"[{name}] apagado.")

            # Pequena espera para evitar corrida de propagacao do delete antes do recreate.
            time.sleep(3)

            client.event_hubs.create_or_update(
                RESOURCE_GROUP, NAMESPACE, name,
                Eventhub(
                    partition_count=partition_count,
                    retention_description=RetentionDescription(
                        cleanup_policy="Delete",
                        retention_time_in_hours=target_hours,
                    ),
                ),
            )
            print(f"[{name}] recriado (particoes={partition_count}, retentionTimeInHours={target_hours}) -- OK\n")
            ok_count += 1
        except Exception as e:
            print(f"[{name}] FALHA: {type(e).__name__}: {e}\n")
            fail_count += 1

    print(f"Resumo: {ok_count} limpos, {fail_count} falharam, de {len(topics)} topicos.")
    return 0 if fail_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
