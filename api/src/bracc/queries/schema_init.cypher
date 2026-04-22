// BR-ACC Neo4j Schema — Constraints and Indexes
// Applied on database initialization

// ── Uniqueness Constraints ──────────────────────────────
CREATE CONSTRAINT person_cpf_unique IF NOT EXISTS
  FOR (p:Person) REQUIRE p.cpf IS UNIQUE;

CREATE CONSTRAINT partner_id_unique IF NOT EXISTS
  FOR (p:Partner) REQUIRE p.partner_id IS UNIQUE;

CREATE CONSTRAINT company_cnpj_unique IF NOT EXISTS
  FOR (c:Company) REQUIRE c.cnpj IS UNIQUE;

CREATE CONSTRAINT contract_contract_id_unique IF NOT EXISTS
  FOR (c:Contract) REQUIRE c.contract_id IS UNIQUE;

CREATE CONSTRAINT sanction_sanction_id_unique IF NOT EXISTS
  FOR (s:Sanction) REQUIRE s.sanction_id IS UNIQUE;

CREATE CONSTRAINT public_office_id_unique IF NOT EXISTS
  FOR (po:PublicOffice) REQUIRE po.office_id IS UNIQUE;

CREATE CONSTRAINT investigation_id_unique IF NOT EXISTS
  FOR (i:Investigation) REQUIRE i.id IS UNIQUE;

CREATE CONSTRAINT amendment_id_unique IF NOT EXISTS
  FOR (a:Amendment) REQUIRE a.amendment_id IS UNIQUE;

CREATE CONSTRAINT health_cnes_code_unique IF NOT EXISTS
  FOR (h:Health) REQUIRE h.cnes_code IS UNIQUE;

CREATE CONSTRAINT finance_id_unique IF NOT EXISTS
  FOR (f:Finance) REQUIRE f.finance_id IS UNIQUE;

CREATE CONSTRAINT embargo_id_unique IF NOT EXISTS
  FOR (e:Embargo) REQUIRE e.embargo_id IS UNIQUE;

CREATE CONSTRAINT education_school_id_unique IF NOT EXISTS
  FOR (e:Education) REQUIRE e.school_id IS UNIQUE;

CREATE CONSTRAINT convenio_id_unique IF NOT EXISTS
  FOR (c:Convenio) REQUIRE c.convenio_id IS UNIQUE;

CREATE CONSTRAINT laborstats_id_unique IF NOT EXISTS
  FOR (l:LaborStats) REQUIRE l.stats_id IS UNIQUE;

CREATE CONSTRAINT inquiry_id_unique IF NOT EXISTS
  FOR (i:Inquiry) REQUIRE i.inquiry_id IS UNIQUE;

CREATE CONSTRAINT inquiry_requirement_id_unique IF NOT EXISTS
  FOR (r:InquiryRequirement) REQUIRE r.requirement_id IS UNIQUE;

CREATE CONSTRAINT inquiry_session_id_unique IF NOT EXISTS
  FOR (s:InquirySession) REQUIRE s.session_id IS UNIQUE;

CREATE CONSTRAINT municipal_bid_id_unique IF NOT EXISTS
  FOR (b:MunicipalBid) REQUIRE b.municipal_bid_id IS UNIQUE;

CREATE CONSTRAINT municipal_contract_id_unique IF NOT EXISTS
  FOR (c:MunicipalContract) REQUIRE c.municipal_contract_id IS UNIQUE;

CREATE CONSTRAINT municipal_bid_item_id_unique IF NOT EXISTS
  FOR (i:MunicipalBidItem) REQUIRE i.municipal_item_id IS UNIQUE;

CREATE CONSTRAINT municipal_gazette_act_id_unique IF NOT EXISTS
  FOR (a:MunicipalGazetteAct) REQUIRE a.municipal_gazette_act_id IS UNIQUE;

CREATE CONSTRAINT state_employee_id IF NOT EXISTS
  FOR (e:StateEmployee) REQUIRE e.employee_id IS UNIQUE;

CREATE CONSTRAINT state_agency_id IF NOT EXISTS
  FOR (a:StateAgency) REQUIRE a.agency_id IS UNIQUE;

CREATE CONSTRAINT go_procurement_id IF NOT EXISTS
  FOR (p:GoProcurement) REQUIRE p.procurement_id IS UNIQUE;

CREATE CONSTRAINT go_municipality_id IF NOT EXISTS
  FOR (m:GoMunicipality) REQUIRE m.municipality_id IS UNIQUE;

CREATE CONSTRAINT municipal_revenue_id IF NOT EXISTS
  FOR (r:MunicipalRevenue) REQUIRE r.revenue_id IS UNIQUE;

CREATE CONSTRAINT municipal_expenditure_id IF NOT EXISTS
  FOR (x:MunicipalExpenditure) REQUIRE x.expenditure_id IS UNIQUE;

CREATE CONSTRAINT judicial_case_id_unique IF NOT EXISTS
  FOR (j:JudicialCase) REQUIRE j.judicial_case_id IS UNIQUE;

CREATE CONSTRAINT source_document_id_unique IF NOT EXISTS
  FOR (s:SourceDocument) REQUIRE s.doc_id IS UNIQUE;

CREATE CONSTRAINT ingestion_run_id_unique IF NOT EXISTS
  FOR (r:IngestionRun) REQUIRE r.run_id IS UNIQUE;

CREATE CONSTRAINT temporal_violation_id_unique IF NOT EXISTS
  FOR (t:TemporalViolation) REQUIRE t.violation_id IS UNIQUE;

// ── Indexes ─────────────────────────────────────────────
CREATE INDEX person_name IF NOT EXISTS
  FOR (p:Person) ON (p.name);

CREATE INDEX person_author_key IF NOT EXISTS
  FOR (p:Person) ON (p.author_key);

CREATE INDEX person_sq_candidato IF NOT EXISTS
  FOR (p:Person) ON (p.sq_candidato);

CREATE INDEX person_cpf_middle6 IF NOT EXISTS
  FOR (p:Person) ON (p.cpf_middle6);

CREATE INDEX person_cpf_partial IF NOT EXISTS
  FOR (p:Person) ON (p.cpf_partial);

CREATE INDEX partner_name IF NOT EXISTS
  FOR (p:Partner) ON (p.name);

CREATE INDEX partner_doc_partial IF NOT EXISTS
  FOR (p:Partner) ON (p.doc_partial);

CREATE INDEX partner_name_doc_partial IF NOT EXISTS
  FOR (p:Partner) ON (p.name, p.doc_partial);

CREATE INDEX company_razao_social IF NOT EXISTS
  FOR (c:Company) ON (c.razao_social);

CREATE INDEX contract_value IF NOT EXISTS
  FOR (c:Contract) ON (c.value);

CREATE INDEX contract_object IF NOT EXISTS
  FOR (c:Contract) ON (c.object);

CREATE INDEX sanction_type IF NOT EXISTS
  FOR (s:Sanction) ON (s.type);

CREATE INDEX election_year IF NOT EXISTS
  FOR (e:Election) ON (e.year);

CREATE INDEX election_composite IF NOT EXISTS
  FOR (e:Election) ON (e.year, e.cargo, e.uf, e.municipio);

CREATE INDEX amendment_function IF NOT EXISTS
  FOR (a:Amendment) ON (a.function);

CREATE INDEX company_cnae_principal IF NOT EXISTS
  FOR (c:Company) ON (c.cnae_principal);

CREATE INDEX contract_contracting_org IF NOT EXISTS
  FOR (c:Contract) ON (c.contracting_org);

CREATE INDEX contract_date IF NOT EXISTS
  FOR (c:Contract) ON (c.date);

CREATE INDEX sanction_date_start IF NOT EXISTS
  FOR (s:Sanction) ON (s.date_start);

CREATE INDEX amendment_value_committed IF NOT EXISTS
  FOR (a:Amendment) ON (a.value_committed);

// ── Finance Indexes ───────────────────────────────────
CREATE INDEX finance_type IF NOT EXISTS
  FOR (f:Finance) ON (f.type);

CREATE INDEX finance_value IF NOT EXISTS
  FOR (f:Finance) ON (f.value);

CREATE INDEX finance_date IF NOT EXISTS
  FOR (f:Finance) ON (f.date);

CREATE INDEX finance_source IF NOT EXISTS
  FOR (f:Finance) ON (f.source);

// ── Embargo Indexes ───────────────────────────────────
CREATE INDEX embargo_uf IF NOT EXISTS
  FOR (e:Embargo) ON (e.uf);

CREATE INDEX embargo_biome IF NOT EXISTS
  FOR (e:Embargo) ON (e.biome);

// ── Health Indexes ────────────────────────────────────
CREATE INDEX health_name IF NOT EXISTS
  FOR (h:Health) ON (h.name);

CREATE INDEX health_uf IF NOT EXISTS
  FOR (h:Health) ON (h.uf);

CREATE INDEX health_municipio IF NOT EXISTS
  FOR (h:Health) ON (h.municipio);

CREATE INDEX health_atende_sus IF NOT EXISTS
  FOR (h:Health) ON (h.atende_sus);

// ── Education Indexes ───────────────────────────────────
CREATE INDEX education_name IF NOT EXISTS
  FOR (e:Education) ON (e.name);

// ── Convenio Indexes ────────────────────────────────────
CREATE INDEX convenio_date_published IF NOT EXISTS
  FOR (c:Convenio) ON (c.date_published);

// ── LaborStats Indexes ──────────────────────────────────
CREATE INDEX laborstats_uf IF NOT EXISTS
  FOR (l:LaborStats) ON (l.uf);

CREATE INDEX laborstats_cnae_subclass IF NOT EXISTS
  FOR (l:LaborStats) ON (l.cnae_subclass);

// ── Person Servidor ID Index ────────────────────────────
CREATE INDEX person_servidor_id IF NOT EXISTS
  FOR (p:Person) ON (p.servidor_id);

// ── PublicOffice Indexes ────────────────────────────────
CREATE INDEX public_office_org IF NOT EXISTS
  FOR (po:PublicOffice) ON (po.org);

CREATE INDEX inquiry_name IF NOT EXISTS
  FOR (i:Inquiry) ON (i.name);

CREATE INDEX inquiry_kind_house IF NOT EXISTS
  FOR (i:Inquiry) ON (i.kind, i.house);

CREATE INDEX inquiry_requirement_date IF NOT EXISTS
  FOR (r:InquiryRequirement) ON (r.date);

CREATE INDEX inquiry_session_date IF NOT EXISTS
  FOR (s:InquirySession) ON (s.date);

CREATE INDEX municipal_bid_date IF NOT EXISTS
  FOR (b:MunicipalBid) ON (b.published_at);

CREATE INDEX municipal_contract_date IF NOT EXISTS
  FOR (c:MunicipalContract) ON (c.signed_at);

CREATE INDEX municipal_gazette_date IF NOT EXISTS
  FOR (a:MunicipalGazetteAct) ON (a.published_at);

CREATE INDEX judicial_case_number IF NOT EXISTS
  FOR (j:JudicialCase) ON (j.case_number);

CREATE INDEX source_document_source_id IF NOT EXISTS
  FOR (s:SourceDocument) ON (s.source_id);

CREATE INDEX source_document_published_at IF NOT EXISTS
  FOR (s:SourceDocument) ON (s.published_at);

CREATE INDEX source_document_retrieved_at IF NOT EXISTS
  FOR (s:SourceDocument) ON (s.retrieved_at);

CREATE INDEX ingestion_run_source_id IF NOT EXISTS
  FOR (r:IngestionRun) ON (r.source_id);

CREATE INDEX ingestion_run_status IF NOT EXISTS
  FOR (r:IngestionRun) ON (r.status);

CREATE INDEX ingestion_run_started_at IF NOT EXISTS
  FOR (r:IngestionRun) ON (r.started_at);

CREATE INDEX temporal_violation_source_id IF NOT EXISTS
  FOR (t:TemporalViolation) ON (t.source_id);

CREATE INDEX temporal_violation_event_date IF NOT EXISTS
  FOR (t:TemporalViolation) ON (t.event_date);

CREATE INDEX socio_snapshot_membership_id IF NOT EXISTS
  FOR ()-[r:SOCIO_DE_SNAPSHOT]-() ON (r.membership_id);

CREATE INDEX socio_snapshot_date IF NOT EXISTS
  FOR ()-[r:SOCIO_DE_SNAPSHOT]-() ON (r.snapshot_date);

// ── Merge-key Indexes (labels that pipelines MERGE by a unique-ish key) ───────
// These speed up MERGE-by-key from O(n) full-scan to O(1) index lookup.
// NOT created as CONSTRAINTs to avoid failing on legacy duplicates; promote to
// CONSTRAINT once each domain has been audited for duplicates.
CREATE INDEX campaign_expense_id IF NOT EXISTS
  FOR (n:CampaignExpense) ON (n.expense_id);

CREATE INDEX campaign_donation_id IF NOT EXISTS
  FOR (n:CampaignDonation) ON (n.donation_id);

CREATE INDEX campaign_donor_id IF NOT EXISTS
  FOR (n:CampaignDonor) ON (n.doador_id);

CREATE INDEX federal_legislator_id_camara IF NOT EXISTS
  FOR (n:FederalLegislator) ON (n.id_camara);

CREATE INDEX state_legislator_id IF NOT EXISTS
  FOR (n:StateLegislator) ON (n.legislator_id);

CREATE INDEX legislative_expense_id IF NOT EXISTS
  FOR (n:LegislativeExpense) ON (n.expense_id);

CREATE INDEX legislative_proposition_id IF NOT EXISTS
  FOR (n:LegislativeProposition) ON (n.proposition_id);

CREATE INDEX expense_id IF NOT EXISTS
  FOR (n:Expense) ON (n.expense_id);

CREATE INDEX senator_id_senado IF NOT EXISTS
  FOR (n:Senator) ON (n.id_senado);

CREATE INDEX canonical_person_id IF NOT EXISTS
  FOR (n:CanonicalPerson) ON (n.canonical_id);

CREATE INDEX custo_mandato_cargo IF NOT EXISTS
  FOR (n:CustoMandato) ON (n.cargo);

CREATE INDEX custo_componente_id IF NOT EXISTS
  FOR (n:CustoComponente) ON (n.componente_id);

CREATE INDEX tcmgo_impedido_id IF NOT EXISTS
  FOR (n:TcmGoImpedido) ON (n.impedido_id);

CREATE INDEX tcmgo_rejected_account_id IF NOT EXISTS
  FOR (n:TcmGoRejectedAccount) ON (n.account_id);

CREATE INDEX tcego_decision_id IF NOT EXISTS
  FOR (n:TceGoDecision) ON (n.decision_id);

CREATE INDEX tcego_audit_id IF NOT EXISTS
  FOR (n:TceGoAudit) ON (n.audit_id);

CREATE INDEX tcego_irregular_account_id IF NOT EXISTS
  FOR (n:TceGoIrregularAccount) ON (n.account_id);

CREATE INDEX bcb_penalty_id IF NOT EXISTS
  FOR (n:BCBPenalty) ON (n.penalty_id);

CREATE INDEX barred_ngo_id IF NOT EXISTS
  FOR (n:BarredNGO) ON (n.ngo_id);

CREATE INDEX bid_id IF NOT EXISTS
  FOR (n:Bid) ON (n.bid_id);

CREATE INDEX cpi_id IF NOT EXISTS
  FOR (n:CPI) ON (n.cpi_id);

CREATE INDEX cvm_proceeding_pas_id IF NOT EXISTS
  FOR (n:CVMProceeding) ON (n.pas_id);

CREATE INDEX dou_act_id IF NOT EXISTS
  FOR (n:DOUAct) ON (n.act_id);

CREATE INDEX declared_asset_id IF NOT EXISTS
  FOR (n:DeclaredAsset) ON (n.asset_id);

CREATE INDEX expulsion_id IF NOT EXISTS
  FOR (n:Expulsion) ON (n.expulsion_id);

CREATE INDEX fund_cnpj IF NOT EXISTS
  FOR (n:Fund) ON (n.fund_cnpj);

CREATE INDEX global_pep_id IF NOT EXISTS
  FOR (n:GlobalPEP) ON (n.pep_id);

CREATE INDEX go_appointment_id IF NOT EXISTS
  FOR (n:GoAppointment) ON (n.appointment_id);

CREATE INDEX go_council_expense_id IF NOT EXISTS
  FOR (n:GoCouncilExpense) ON (n.expense_id);

CREATE INDEX go_gazette_act_id IF NOT EXISTS
  FOR (n:GoGazetteAct) ON (n.act_id);

CREATE INDEX go_legislative_proposal_id IF NOT EXISTS
  FOR (n:GoLegislativeProposal) ON (n.proposal_id);

CREATE INDEX go_security_stat_id IF NOT EXISTS
  FOR (n:GoSecurityStat) ON (n.stat_id);

CREATE INDEX go_state_contract_id IF NOT EXISTS
  FOR (n:GoStateContract) ON (n.contract_id);

CREATE INDEX go_state_sanction_id IF NOT EXISTS
  FOR (n:GoStateSanction) ON (n.sanction_id);

CREATE INDEX go_state_supplier_cnpj IF NOT EXISTS
  FOR (n:GoStateSupplier) ON (n.cnpj);

CREATE INDEX go_vereador_id IF NOT EXISTS
  FOR (n:GoVereador) ON (n.vereador_id);

CREATE INDEX gov_card_expense_id IF NOT EXISTS
  FOR (n:GovCardExpense) ON (n.expense_id);

CREATE INDEX gov_travel_id IF NOT EXISTS
  FOR (n:GovTravel) ON (n.travel_id);

CREATE INDEX international_sanction_id IF NOT EXISTS
  FOR (n:InternationalSanction) ON (n.sanction_id);

CREATE INDEX legal_case_id IF NOT EXISTS
  FOR (n:LegalCase) ON (n.case_id);

CREATE INDEX leniency_id IF NOT EXISTS
  FOR (n:LeniencyAgreement) ON (n.leniency_id);

CREATE INDEX municipal_finance_id IF NOT EXISTS
  FOR (n:MunicipalFinance) ON (n.finance_id);

CREATE INDEX offshore_entity_id IF NOT EXISTS
  FOR (n:OffshoreEntity) ON (n.offshore_id);

CREATE INDEX offshore_officer_id IF NOT EXISTS
  FOR (n:OffshoreOfficer) ON (n.offshore_officer_id);

CREATE INDEX pep_cgu_id IF NOT EXISTS
  FOR (n:PEPRecord) ON (n.pep_id);

CREATE INDEX party_membership_id IF NOT EXISTS
  FOR (n:PartyMembership) ON (n.membership_id);

CREATE INDEX payment_transfer_id IF NOT EXISTS
  FOR (n:Payment) ON (n.transfer_id);

CREATE INDEX tax_waiver_id IF NOT EXISTS
  FOR (n:TaxWaiver) ON (n.waiver_id);

// ── Fulltext Search Index ───────────────────────────────
// standard-folding analyzer makes search accent-insensitive ("João" ~ "joao").
// Analyzer changes on an existing index are handled by ensure_schema().
CREATE FULLTEXT INDEX entity_search IF NOT EXISTS
  FOR (n:Person|Partner|Company|Health|Education|Contract|Amendment|Convenio|Embargo|PublicOffice|Inquiry|InquiryRequirement|MunicipalContract|MunicipalBid|MunicipalGazetteAct|JudicialCase|SourceDocument)
  ON EACH [n.name, n.razao_social, n.cpf, n.cnpj, n.doc_partial, n.doc_raw, n.cnes_code, n.object, n.contracting_org, n.convenente, n.infraction, n.org, n.function, n.subject, n.text, n.topic, n.case_number, n.url]
  OPTIONS { indexConfig: { `fulltext.analyzer`: 'standard-folding' } };

// ── User Constraints ────────────────────────────────────
CREATE CONSTRAINT user_email_unique IF NOT EXISTS
  FOR (u:User) REQUIRE u.email IS UNIQUE;
