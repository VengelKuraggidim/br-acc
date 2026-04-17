MATCH (center)
WHERE elementId(center) = $entity_id
  AND (center:Person OR center:Partner OR center:Company OR center:Contract OR center:Sanction OR center:Election
       OR center:Amendment OR center:Finance OR center:Embargo OR center:Health OR center:Education
       OR center:Convenio OR center:LaborStats OR center:PublicOffice)
OPTIONAL MATCH (center)-[r:SOCIO_DE|DOOU|CANDIDATO_EM|VENCEU|AUTOR_EMENDA|SANCIONADA|OPERA_UNIDADE|DEVE|RECEBEU_EMPRESTIMO|EMBARGADA|MANTEDORA_DE|BENEFICIOU|GEROU_CONVENIO|SAME_AS|POSSIBLE_SAME_AS]-(connected)
WHERE (coalesce($include_probable, false) OR type(r) <> "POSSIBLE_SAME_AS")
  AND NOT (connected:User OR connected:Investigation OR connected:Annotation OR connected:Tag)
WITH center, r, connected, startNode(r) AS src, endNode(r) AS tgt
LIMIT 1000
RETURN center AS e,
       r,
       connected,
       labels(center) AS source_labels,
       labels(connected) AS target_labels,
       type(r) AS rel_type,
       elementId(src) AS source_id,
       elementId(tgt) AS target_id,
       elementId(r) AS rel_id
