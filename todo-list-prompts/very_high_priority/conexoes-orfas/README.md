# Conexões órfãs — recuperar dados já baixados que não estão no app

Auditoria do grafo Neo4j local em 2026-05-02 descobriu que ~466k nodes estão **100% órfãos** (zero relationships) e outros ~1M estão conectados mas irrelevantes a GO. A maior parte **não é lixo**: são loaders que criaram os nodes mas falharam em criar as relationships com o núcleo GO. Conectá-los gera várias features novas no app.

## Inventário e priorização

| # | Item | Volume GO | Pasta | Status |
|---|---|---|---|---|
| 01 | Fix loader CampaignDonation TSE | **119k doações órfãs (todas GO)** | very_high_priority | provável raiz do bug Amilton |
| 02 | PEPRecord CGU ↔ Person GO | **4.088 perfis ganham selo PEP** | very_high_priority | obrigatório regulatório |
| 03 | BNDES ↔ Município GO | R$ 9,6 bi em 202 contratos | high_priority | aba nova |
| 04 | Embargo IBAMA ↔ Município GO | 4.544 embargos | high_priority | aba nova |
| 05 | DOUAct ↔ Município GO | 1.094 atos | high_priority | feed novo |
| 06 | GlobalPEP ↔ Person GO | 1.130 matches | medium_priority | complementa #02 |
| 07 | Expulsion CEAF ↔ Person GO | 95 perfis | medium_priority | risco reputacional alto |
| 08 | Sanction CEIS ↔ Fornecedor GO | depende de PNCP | very_low_priority/blocked | precisa loader PNCP expandido |

## Métrica de sucesso global

Após executar 01–05, o app passa a mostrar:

- 119k doações TSE corretamente vinculadas (resolve bug Amilton)
- Selo PEP em ~4k perfis políticos
- Aba BNDES (R$ 9,6 bi) em perfis de município
- Aba Embargo Ambiental em perfis de município
- Feed DOU semanal mencionando município

Sem baixar nenhum dado novo.

## Próximos passos pós-conexão

Após esses 7 fixes, valerá a pena revisitar **o que realmente é lixo cortável**: provavelmente sobram apenas InternationalSanction OFAC (~39k), BCBPenalty (~3,6k), BarredNGO (~3,6k) e o BNDES não-GO (~1M, mantido para Phase 2 caso surjam cruzamentos novos).
