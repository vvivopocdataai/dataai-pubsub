#!/usr/bin/env python3
"""
register_schemas.py

Registra os 26 schemas Avro (schemas/<domain>/*.avsc) no Azure Event Hubs Schema Registry,
no schema group unico existente no namespace Standard: "application-schemas".

MOTIVO DESTE SCRIPT EXISTIR SEPARADO DO AGENTE:
Diferente da criacao do schema GROUP (operacao de control-plane via ARM, que o agente
ja executa diretamente), o CADASTRO do CONTEUDO de cada schema (e a emissao do schemaId)
e uma operacao de DATA-PLANE contra o proprio namespace (endpoint
"<namespace>.servicebus.windows.net", protocolo AMQP/HTTPS via SDK), nao exposta por
nenhuma ferramenta Azure MCP disponivel (nem azure_arm, que so fala com
management.azure.com, nem azure_eventhubs, que nao tem comando de schema registry).
Por isso este script precisa ser executado por voce (ou pela pipeline de CI), com suas
proprias credenciais, fora do agente.

ATENCAO - LIMITE CONHECIDO DO TIER STANDARD:
O tier Standard do Event Hubs permite no maximo 25 VERSOES DE SCHEMA no total, somadas
em todos os schema groups do namespace (ver docs.microsoft.com/azure/event-hubs/event-hubs-quotas).
Temos 26 schemas distintos para cadastrar (1a versao cada) - ou seja, **o 26o schema deve
falhar por quota** neste tier. O script tenta registrar todos e informa exatamente qual
schema falhou e por que, para você decidir entre:
  (a) solicitar upgrade do namespace para tier Premium (permite 1000 versoes), ou
  (b) consolidar/descartar 1 dos 26 contratos para caber em 25 no Standard.

Pre-requisitos:
    pip install azure-schemaregistry azure-identity

Autenticacao (via DefaultAzureCredential, tenta nesta ordem):
    - az login (Azure CLI) já efetuado localmente, ou
    - variaveis de ambiente AZURE_CLIENT_ID / AZURE_TENANT_ID / AZURE_CLIENT_SECRET (service principal), ou
    - Managed Identity (se executado dentro do Azure, ex.: AKS/VM/Function).

RBAC necessario (no namespace eh-dataai-vivopoc):
    "Schema Registry Contributor" (ou Owner/Contributor) para o principal usado no login.

Uso:
    python3 register_schemas.py --dry-run          # valida e lista o que seria cadastrado, sem chamar o Azure
    python3 register_schemas.py                    # cadastra de fato todos os 26 schemas
    python3 register_schemas.py --schemas-dir ../schemas --namespace eh-dataai-vivopoc --group application-schemas

Saida:
    - Log no console por schema (OK com schemaId/versao, ou FALHA com motivo).
    - Ao final, grava schema-registry-manifest.json com o mapeamento
      {fully_qualified_name: {schemaId, version, group, file}} para os schemas que tiveram sucesso -
      este arquivo e o inventario a referenciar depois em contracts/<domain>/*.odcs.yaml (campo
      schema.registry.schemaId) e em docs/contract-catalog.md.
"""
import argparse
import glob
import json
import os
import sys


def load_schema_files(schemas_dir):
    files = sorted(glob.glob(os.path.join(schemas_dir, "**", "*.avsc"), recursive=True))
    items = []
    for f in files:
        with open(f, "r", encoding="utf-8") as fh:
            raw = fh.read()
        data = json.loads(raw)
        fqn = f'{data["namespace"]}.{data["name"]}'
        items.append({"file": f, "fqn": fqn, "raw": raw, "data": data})
    return items


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--namespace", default="eh-dataai-vivopoc", help="Nome do namespace Event Hubs (sem sufixo .servicebus.windows.net).")
    parser.add_argument("--group", default="application-schemas", help="Nome do schema group existente.")
    parser.add_argument("--schemas-dir", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "schemas"), help="Diretorio raiz dos arquivos .avsc.")
    parser.add_argument("--dry-run", action="store_true", help="Apenas lista/valida, sem chamar o Azure.")
    parser.add_argument("--manifest-out", default="schema-registry-manifest.json", help="Arquivo de saida com o mapeamento schema->schemaId.")
    args = parser.parse_args()

    schemas_dir = os.path.abspath(args.schemas_dir)
    items = load_schema_files(schemas_dir)
    print(f"Encontrados {len(items)} arquivos .avsc em {schemas_dir}\n")

    if len(items) > 25:
        print(f"AVISO: {len(items)} schemas > limite de 25 versoes do tier Standard. "
              f"Pelo menos 1 registro deve falhar por quota — ver cabecalho deste script.\n")

    if args.dry_run:
        for it in items:
            print(f"[dry-run] group={args.group!r} name={it['fqn']!r}  <- {it['file']}")
        print("\nDry-run concluido. Nenhuma chamada foi feita ao Azure.")
        return 0

    try:
        from azure.identity import DefaultAzureCredential
        from azure.schemaregistry import SchemaFormat, SchemaRegistryClient
    except ImportError as e:
        print("ERRO: dependencias ausentes. Rode: pip install azure-schemaregistry azure-identity")
        print(f"Detalhe: {e}")
        return 2

    fully_qualified_namespace = f"{args.namespace}.servicebus.windows.net"
    credential = DefaultAzureCredential()
    client = SchemaRegistryClient(fully_qualified_namespace=fully_qualified_namespace, credential=credential)

    manifest = {}
    ok_count, fail_count = 0, 0

    with client:
        for it in items:
            try:
                props = client.register_schema(
                    group_name=args.group,
                    name=it["fqn"],
                    definition=it["raw"],
                    format=SchemaFormat.AVRO,
                )
                manifest[it["fqn"]] = {
                    "schemaId": props.id,
                    "version": props.version,
                    "group": args.group,
                    "file": it["file"],
                }
                print(f"OK    {it['fqn']:<70} id={props.id} version={props.version}")
                ok_count += 1
            except Exception as e:
                msg = str(e)
                hint = ""
                if "25" in msg or "quota" in msg.lower() or "Forbidden" in msg or "40" in msg:
                    hint = "  <-- possivel limite de 25 versoes do tier Standard; considere upgrade para Premium."
                print(f"FALHA {it['fqn']:<70} erro={msg}{hint}")
                fail_count += 1

    manifest_path = os.path.abspath(args.manifest_out)
    with open(manifest_path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2, ensure_ascii=False)

    print(f"\nResumo: {ok_count} cadastrados, {fail_count} falharam, de {len(items)} totais.")
    print(f"Manifesto gravado em: {manifest_path}")
    return 0 if fail_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
