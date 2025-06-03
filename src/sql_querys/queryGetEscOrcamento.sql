select u.codigo fk_unidade, u.nome unidade, o.fk_secao, s.nome secao, p.codigo fk_tipo_posto, p.nome tipo_posto, p.percentual_estimativa percentual_posto, s.desempenho_hora FC, o.data, o.hora_ini, o.itens, o.valor, 'FCST' as TIPO
from wfm.esc_orcamento o
inner join wfm.esc_secao s on s.codigo=o.fk_secao
inner join wfm.esc_unidade u on u.codigo=s.fk_unidade
inner join wfm.esc_tipo_posto p on s.codigo=p.fk_secao
where o.data between to_date({start_date},'yyyy-mm-dd') and to_date({end_date},'yyyy-mm-dd') and p.codigo={posto_id}


--- QUERY ESC_ORCAMENTO (escOrcamento.txt)