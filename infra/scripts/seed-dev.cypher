// BR-ACC Dev Seed Data
// Small fixture graph that exercises all 5 analysis patterns
// Run: cypher-shell -f seed-dev.cypher

// ── Clean existing dev data ─────────────────────────────
MATCH (n) DETACH DELETE n;

// ── Persons (Politicians & Family) ──────────────────────
CREATE (p1:Person {
  cpf: '11111111111', name: 'CARLOS ALBERTO SILVA',
  patrimonio_declarado: 500000.0, is_pep: true
});
CREATE (p2:Person {
  cpf: '22222222222', name: 'MARIA SILVA COSTA',
  patrimonio_declarado: 200000.0, is_pep: false
});
CREATE (p3:Person {
  cpf: '33333333333', name: 'JOAO PEREIRA NETO',
  patrimonio_declarado: 800000.0, is_pep: true
});
CREATE (p4:Person {
  cpf: '44444444444', name: 'ANA LUCIA FERREIRA',
  patrimonio_declarado: 150000.0, is_pep: false
});
CREATE (p5:Person {
  cpf: '55555555555', name: 'ROBERTO SANTOS FILHO',
  patrimonio_declarado: 300000.0, is_pep: true
});

// ── Companies ───────────────────────────────────────────
CREATE (co1:Company {
  cnpj: '11222333000181', razao_social: 'SILVA CONSTRUCOES LTDA',
  cnae_principal: '4120400', capital_social: 8000000.0,
  uf: 'SP', municipio: 'SAO PAULO'
});
CREATE (co2:Company {
  cnpj: '22333444000192', razao_social: 'COSTA ENGENHARIA SA',
  cnae_principal: '4120400', capital_social: 3000000.0,
  uf: 'SP', municipio: 'SAO PAULO'
});
CREATE (co3:Company {
  cnpj: '33444555000103', razao_social: 'PEREIRA SERVICOS LTDA',
  cnae_principal: '8130300', capital_social: 500000.0,
  uf: 'RJ', municipio: 'RIO DE JANEIRO'
});
CREATE (co4:Company {
  cnpj: '44555666000114', razao_social: 'FERREIRA TECNOLOGIA SA',
  cnae_principal: '6201501', capital_social: 2000000.0,
  uf: 'MG', municipio: 'BELO HORIZONTE'
});
CREATE (co5:Company {
  cnpj: '55666777000125', razao_social: 'SANTOS CONSULTORIA LTDA',
  cnae_principal: '7020400', capital_social: 1500000.0,
  uf: 'SP', municipio: 'SAO PAULO'
});

// ── Family Relationships ────────────────────────────────
// CARLOS ALBERTO → married to MARIA (family link for self-dealing)
MATCH (p1:Person {cpf: '11111111111'}), (p2:Person {cpf: '22222222222'})
CREATE (p1)-[:CONJUGE_DE]->(p2);

// JOAO → parent of ANA (family link for patrimony)
MATCH (p3:Person {cpf: '33333333333'}), (p4:Person {cpf: '44444444444'})
CREATE (p3)-[:PARENTE_DE]->(p4);

// ── Company Partnerships (SOCIO_DE) ─────────────────────
// MARIA is partner of SILVA CONSTRUCOES (family company)
MATCH (p2:Person {cpf: '22222222222'}), (co1:Company {cnpj: '11222333000181'})
CREATE (p2)-[:SOCIO_DE]->(co1);

// ANA is partner of PEREIRA SERVICOS (family company)
MATCH (p4:Person {cpf: '44444444444'}), (co3:Company {cnpj: '33444555000103'})
CREATE (p4)-[:SOCIO_DE]->(co3);

// ROBERTO is partner of SANTOS CONSULTORIA
MATCH (p5:Person {cpf: '55555555555'}), (co5:Company {cnpj: '55666777000125'})
CREATE (p5)-[:SOCIO_DE]->(co5);

// ── Contracts ───────────────────────────────────────────
CREATE (c1:Contract {
  contract_id: 'CTR-001', object: 'Construcao de ponte municipal',
  value: 2500000.0, contracting_org: 'PREFEITURA SAO PAULO', date: '2024-03-15'
});
CREATE (c2:Contract {
  contract_id: 'CTR-002', object: 'Manutencao de vias publicas',
  value: 800000.0, contracting_org: 'PREFEITURA SAO PAULO', date: '2024-06-01'
});
CREATE (c3:Contract {
  contract_id: 'CTR-003', object: 'Servicos de limpeza hospitalar',
  value: 1200000.0, contracting_org: 'PREFEITURA RIO DE JANEIRO', date: '2024-01-10'
});
CREATE (c4:Contract {
  contract_id: 'CTR-004', object: 'Sistema de gestao publica',
  value: 3500000.0, contracting_org: 'PREFEITURA BELO HORIZONTE', date: '2024-07-20'
});
CREATE (c5:Contract {
  contract_id: 'CTR-005', object: 'Consultoria em licitacoes',
  value: 450000.0, contracting_org: 'PREFEITURA SAO PAULO', date: '2024-09-05'
});
CREATE (c6:Contract {
  contract_id: 'CTR-006', object: 'Reforma de escola municipal',
  value: 1800000.0, contracting_org: 'PREFEITURA SAO PAULO', date: '2024-04-12'
});
CREATE (c7:Contract {
  contract_id: 'CTR-007', object: 'Pavimentacao de estradas rurais',
  value: 950000.0, contracting_org: 'PREFEITURA SAO PAULO', date: '2024-11-01'
});
CREATE (c8:Contract {
  contract_id: 'CTR-008', object: 'Fornecimento de equipamentos medicos',
  value: 600000.0, contracting_org: 'PREFEITURA RIO DE JANEIRO', date: '2024-02-28'
});
CREATE (c9:Contract {
  contract_id: 'CTR-009', object: 'Servicos de TI - datacenter',
  value: 2200000.0, contracting_org: 'PREFEITURA SAO PAULO', date: '2024-08-15'
});
CREATE (c10:Contract {
  contract_id: 'CTR-010', object: 'Auditoria contabil publica',
  value: 350000.0, contracting_org: 'PREFEITURA SAO PAULO', date: '2024-10-01'
});

// Set contract names programmatically
MATCH (c:Contract)
SET c.name = c.contract_id + ' - ' + c.object;

// ── Amendment (for self-dealing pattern) ──────────────────
CREATE (a1:Amendment {
  amendment_id: 'EMD-001', type: 'Individual', function: 'Urbanismo',
  municipality: 'SAO PAULO', uf: 'SP',
  value_committed: 2500000.0, value_paid: 2400000.0
});

// ── Pattern p01: Self-dealing amendment ─────────────────
// CARLOS authored amendment → SILVA CONSTRUCOES (wife's company) won contract
MATCH (p1:Person {cpf: '11111111111'}), (a1:Amendment {amendment_id: 'EMD-001'})
CREATE (p1)-[:AUTOR_EMENDA]->(a1);

MATCH (co1:Company {cnpj: '11222333000181'}), (c1:Contract {contract_id: 'CTR-001'})
CREATE (co1)-[:VENCEU]->(c1);

// ── Pattern p05: Patrimony incompatibility ──────────────
// JOAO declared 800K but daughter ANA's company PEREIRA has 500K capital
// + also partner in another company with high capital (via additional link)
// The 10x ratio test: family_company_capital > patrimonio * 10
// Let's make JOAO have low patrimony but high-capital family companies
// Update JOAO's patrimony to be low
MATCH (p3:Person {cpf: '33333333333'})
SET p3.patrimonio_declarado = 50000.0;

// ANA also has shares in FERREIRA TECNOLOGIA (high capital)
MATCH (p4:Person {cpf: '44444444444'}), (co4:Company {cnpj: '44555666000114'})
CREATE (p4)-[:SOCIO_DE]->(co4);

// ── Pattern p06: Sanctioned still receiving ─────────────
CREATE (s1:Sanction {
  sanction_id: 'SAN-001', type: 'CEIS', date_start: '2023-01-01',
  date_end: '2025-12-31', reason: 'Irregularidade em licitacao',
  source: 'CEIS'
});

MATCH (co3:Company {cnpj: '33444555000103'}), (s1:Sanction {sanction_id: 'SAN-001'})
CREATE (co3)-[:SANCIONADA]->(s1);

// PEREIRA SERVICOS won contract AFTER sanction date
MATCH (co3:Company {cnpj: '33444555000103'}), (c3:Contract {contract_id: 'CTR-003'})
CREATE (co3)-[:VENCEU]->(c3);

MATCH (co3:Company {cnpj: '33444555000103'}), (c8:Contract {contract_id: 'CTR-008'})
CREATE (co3)-[:VENCEU]->(c8);

// ── Pattern p10: Donation-contract loop ─────────────────
// FERREIRA TECNOLOGIA donated to ROBERTO's campaign, then won contract from his org
CREATE (e1:Election {
  election_id: 'ELE-001', year: 2022, cargo: 'PREFEITO', uf: 'MG', municipio: 'BELO HORIZONTE'
});

MATCH (p5:Person {cpf: '55555555555'}), (e1:Election {election_id: 'ELE-001'})
CREATE (p5)-[:CANDIDATO_EM]->(e1);

MATCH (co4:Company {cnpj: '44555666000114'}), (p5:Person {cpf: '55555555555'})
CREATE (co4)-[:DOOU {valor: 100000.0, year: 2022}]->(p5);

MATCH (co4:Company {cnpj: '44555666000114'}), (c4:Contract {contract_id: 'CTR-004'})
CREATE (co4)-[:VENCEU]->(c4);

// ── Pattern p12: Contract concentration ─────────────────
// SILVA CONSTRUCOES dominates SAO PAULO contracts (>30% share)
MATCH (co1:Company {cnpj: '11222333000181'}), (c2:Contract {contract_id: 'CTR-002'})
CREATE (co1)-[:VENCEU]->(c2);

MATCH (co1:Company {cnpj: '11222333000181'}), (c5:Contract {contract_id: 'CTR-005'})
CREATE (co1)-[:VENCEU]->(c5);

MATCH (co1:Company {cnpj: '11222333000181'}), (c6:Contract {contract_id: 'CTR-006'})
CREATE (co1)-[:VENCEU]->(c6);

MATCH (co1:Company {cnpj: '11222333000181'}), (c7:Contract {contract_id: 'CTR-007'})
CREATE (co1)-[:VENCEU]->(c7);

MATCH (co1:Company {cnpj: '11222333000181'}), (c9:Contract {contract_id: 'CTR-009'})
CREATE (co1)-[:VENCEU]->(c9);

MATCH (co1:Company {cnpj: '11222333000181'}), (c10:Contract {contract_id: 'CTR-010'})
CREATE (co1)-[:VENCEU]->(c10);

// COSTA ENGENHARIA gets a few SAO PAULO contracts (for comparison)
MATCH (co2:Company {cnpj: '22333444000192'}), (c2:Contract {contract_id: 'CTR-002'})
CREATE (co2)-[:VENCEU]->(c2);

// SANTOS CONSULTORIA gets one SAO PAULO contract
MATCH (co5:Company {cnpj: '55666777000125'}), (c5:Contract {contract_id: 'CTR-005'})
CREATE (co5)-[:VENCEU]->(c5);

// ── Public Offices ──────────────────────────────────────
CREATE (po1:PublicOffice {
  cpf: '11111111111', name: 'Secretario de Obras', org: 'PREFEITURA SAO PAULO',
  salary: 25000.0
});

MATCH (p1:Person {cpf: '11111111111'}), (po1:PublicOffice {cpf: '11111111111'})
CREATE (p1)-[:RECEBEU_SALARIO]->(po1);

// ── Goias-specific fixtures (Fiscal Cidadao PWA parity) ─────
// The FastAPI PWA parity router (`bracc.routers.pwa_parity`: /status,
// /buscar-tudo, /politico/{id}) expects these GO-scoped node types with
// uf='GO' so the landing page shows non-zero counts in dev.

// GO politicians + Election records (for /api/v1/meta/person-count)
CREATE (pgo1:Person {
  cpf: 'GO_POL_01', name: 'PEDRO DEPUTADO FEDERAL', uf: 'GO',
  is_pep: true, partido: 'PT', cargo: 'DEPUTADO FEDERAL'
});
CREATE (pgo2:Person {
  cpf: 'GO_POL_02', name: 'JULIA DEPUTADA ESTADUAL', uf: 'GO',
  is_pep: true, partido: 'PSD', cargo: 'DEPUTADO ESTADUAL'
});
CREATE (pgo3:Person {
  cpf: 'GO_POL_03', name: 'FERNANDO SENADOR', uf: 'GO',
  is_pep: true, partido: 'MDB', cargo: 'SENADOR'
});

CREATE (ego1:Election {
  election_id: 'ELE-GO-DF-2022', year: 2022,
  cargo: 'DEPUTADO FEDERAL', uf: 'GO', municipio: ''
});
CREATE (ego2:Election {
  election_id: 'ELE-GO-DE-2022', year: 2022,
  cargo: 'DEPUTADO ESTADUAL', uf: 'GO', municipio: ''
});
CREATE (ego3:Election {
  election_id: 'ELE-GO-SN-2022', year: 2022,
  cargo: 'SENADOR', uf: 'GO', municipio: ''
});

MATCH (p:Person {cpf: 'GO_POL_01'}), (e:Election {election_id: 'ELE-GO-DF-2022'})
CREATE (p)-[:CANDIDATO_EM]->(e);
MATCH (p:Person {cpf: 'GO_POL_02'}), (e:Election {election_id: 'ELE-GO-DE-2022'})
CREATE (p)-[:CANDIDATO_EM]->(e);
MATCH (p:Person {cpf: 'GO_POL_03'}), (e:Election {election_id: 'ELE-GO-SN-2022'})
CREATE (p)-[:CANDIDATO_EM]->(e);

// State agencies + employees (folha_go schema)
CREATE (sa1:StateAgency {
  agency_id: 'AGN-SEF-GO', name: 'Secretaria da Fazenda de Goias',
  uf: 'GO', source: 'folha_go'
});
CREATE (sa2:StateAgency {
  agency_id: 'AGN-SEDUC-GO', name: 'Secretaria da Educacao de Goias',
  uf: 'GO', source: 'folha_go'
});

CREATE (se1:StateEmployee {
  employee_id: 'EMP-GO-001', name: 'MARCOS SERVIDOR FAZENDA',
  role: 'AUDITOR FISCAL', agency: 'Secretaria da Fazenda de Goias',
  salary_gross: 18500.0, is_commissioned: false, uf: 'GO',
  source: 'folha_go'
});
CREATE (se2:StateEmployee {
  employee_id: 'EMP-GO-002', name: 'PATRICIA CARGO COMISSIONADO',
  role: 'DIRETORA DAS-5', agency: 'Secretaria da Fazenda de Goias',
  salary_gross: 22000.0, is_commissioned: true, uf: 'GO',
  source: 'folha_go'
});
CREATE (se3:StateEmployee {
  employee_id: 'EMP-GO-003', name: 'ANDRE PROFESSOR ESTADUAL',
  role: 'PROFESSOR PIV-I', agency: 'Secretaria da Educacao de Goias',
  salary_gross: 7800.0, is_commissioned: false, uf: 'GO',
  source: 'folha_go'
});

MATCH (e:StateEmployee {employee_id: 'EMP-GO-001'}), (a:StateAgency {agency_id: 'AGN-SEF-GO'})
CREATE (e)-[:LOTADO_EM]->(a);
MATCH (e:StateEmployee {employee_id: 'EMP-GO-002'}), (a:StateAgency {agency_id: 'AGN-SEF-GO'})
CREATE (e)-[:LOTADO_EM]->(a);
MATCH (e:StateEmployee {employee_id: 'EMP-GO-003'}), (a:StateAgency {agency_id: 'AGN-SEDUC-GO'})
CREATE (e)-[:LOTADO_EM]->(a);

// GO municipalities (tcm_go schema)
CREATE (mgo1:GoMunicipality {
  municipality_id: 'MUN-GOIANIA', name: 'Goiania', cod_ibge: '5208707',
  population: 1555626, total_revenue: 4500000000.0, total_expenditure: 4200000000.0,
  uf: 'GO', source: 'tcm_go'
});
CREATE (mgo2:GoMunicipality {
  municipality_id: 'MUN-APARECIDA', name: 'Aparecida de Goiania', cod_ibge: '5201108',
  population: 590146, total_revenue: 1400000000.0, total_expenditure: 1300000000.0,
  uf: 'GO', source: 'tcm_go'
});
CREATE (mgo3:GoMunicipality {
  municipality_id: 'MUN-ANAPOLIS', name: 'Anapolis', cod_ibge: '5201108',
  population: 391772, total_revenue: 950000000.0, total_expenditure: 890000000.0,
  uf: 'GO', source: 'tcm_go'
});

// GO procurements (pncp_go schema)
CREATE (proc1:GoProcurement {
  procurement_id: 'PROC-GO-001', object: 'Construcao de escola municipal em Goiania',
  agency_name: 'PREFEITURA DE GOIANIA', cnpj_agency: '01612092000123',
  amount_estimated: 2800000.0, modality: 'Concorrencia',
  published_at: '2025-03-10', municipality: 'Goiania', uf: 'GO',
  source: 'pncp_go'
});
CREATE (proc2:GoProcurement {
  procurement_id: 'PROC-GO-002', object: 'Pavimentacao asfaltica - rodovia estadual GO-060',
  agency_name: 'AGETOP - Agencia Goiana de Transportes', cnpj_agency: '04000000000199',
  amount_estimated: 15400000.0, modality: 'Concorrencia',
  published_at: '2025-02-22', municipality: 'Trindade', uf: 'GO',
  source: 'pncp_go'
});
CREATE (proc3:GoProcurement {
  procurement_id: 'PROC-GO-003', object: 'Aquisicao de medicamentos - SMS Anapolis',
  agency_name: 'SECRETARIA MUNICIPAL DE SAUDE DE ANAPOLIS',
  cnpj_agency: '01075555000188',
  amount_estimated: 780000.0, modality: 'Pregao Eletronico',
  published_at: '2025-04-05', municipality: 'Anapolis', uf: 'GO',
  source: 'pncp_go'
});

// GO appointments (querido_diario_go schema)
CREATE (app1:GoAppointment {
  appointment_id: 'APP-GO-001', person_name: 'CARLOS NOMEADO SECRETARIO',
  role: 'Secretario Municipal de Obras', agency: 'Prefeitura de Goiania',
  act_date: '2025-02-15', appointment_type: 'nomeacao',
  territory_name: 'Goiania', uf: 'GO', source: 'querido_diario_go'
});
CREATE (app2:GoAppointment {
  appointment_id: 'APP-GO-002', person_name: 'LUCIANA EXONERADA DAS',
  role: 'Assessora Especial DAS-4', agency: 'Governo do Estado de Goias',
  act_date: '2025-03-01', appointment_type: 'exoneracao',
  territory_name: 'Goias', uf: 'GO', source: 'querido_diario_go'
});

// Goiania city council members (camara_goiania schema)
CREATE (v1:GoVereador {
  vereador_id: 'VER-GYN-001', name: 'AMANDA VEREADORA GOIANIA',
  party: 'PT', municipality: 'Goiania',
  total_expenses: 68000.0, proposals_count: 14, uf: 'GO',
  source: 'camara_goiania'
});
CREATE (v2:GoVereador {
  vereador_id: 'VER-GYN-002', name: 'RICARDO VEREADOR GOIANIA',
  party: 'PL', municipality: 'Goiania',
  total_expenses: 54000.0, proposals_count: 7, uf: 'GO',
  source: 'camara_goiania'
});
CREATE (v3:GoVereador {
  vereador_id: 'VER-GYN-003', name: 'DANIELA VEREADORA GOIANIA',
  party: 'PSOL', municipality: 'Goiania',
  total_expenses: 42000.0, proposals_count: 21, uf: 'GO',
  source: 'camara_goiania'
});

// Election record for Goiania vereadores (so person-count.vereadores > 0)
CREATE (ego4:Election {
  election_id: 'ELE-GYN-VER-2024', year: 2024,
  cargo: 'VEREADOR', uf: 'GO', municipio: 'Goiania'
});
CREATE (pv1:Person {
  cpf: 'GO_VER_01', name: 'AMANDA VEREADORA GOIANIA',
  uf: 'GO', is_pep: true, partido: 'PT', cargo: 'VEREADOR'
});
MATCH (p:Person {cpf: 'GO_VER_01'}), (e:Election {election_id: 'ELE-GYN-VER-2024'})
CREATE (p)-[:CANDIDATO_EM]->(e);

// ── Summary ─────────────────────────────────────────────
// Base fixtures: 5 Person, 5 Company, 10 Contract, 1 Amendment, 1 Sanction, 1 Election, 1 PublicOffice
// GO fixtures: 4 Person(GO), 4 Election(GO), 2 StateAgency, 3 StateEmployee,
//   3 GoMunicipality, 3 GoProcurement, 2 GoAppointment, 3 GoVereador
// All 5 patterns continue to exercise, and Fiscal Cidadao /status shows
// deputados_federais=1, deputados_estaduais=1, senadores=1, vereadores=1
// and GO counters (servidores_estaduais=3, cargos_comissionados=1,
// municipios_go=3, licitacoes_go=3, nomeacoes_go=2, vereadores_goiania=3).
