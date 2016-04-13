drop KEYSPACE IF EXISTS diferentonas;

create KEYSPACE diferentonas WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS diferentonas.similar ( 
id INT,
similar_id INT,
score DOUBLE,
PRIMARY KEY((id), similar_id));

CREATE TABLE IF NOT EXISTS diferentonas.convenios (
ANO_CONVENIO INT,
NR_CONVENIO BIGINT,
TX_MODALIDADE TEXT,
TX_SITUACAO TEXT,
TX_SUBSITUACAO TEXT,
CD_ORGAO_SUPERIOR BIGINT,
NM_ORGAO_SUPERIOR TEXT,
CD_ORGAO_CONCEDENTE BIGINT,
TX_REGIAO_PROPONENTE TEXT,
UF_PROPONENTE TEXT,
NM_MUNICIPIO_PROPONENTE TEXT,
NM_PROPONENTE TEXT,
NM_ORGAO_CONCEDENTE TEXT,
DT_INCLUSAO_PROPOSTA TEXT,
CD_PROGRAMA BIGINT,
NM_PROGRAMA TEXT,
CD_ACAO_PPA TEXT,
DT_INICIO_VIGENCIA TEXT,
DT_FIM_VIGENCIA TEXT,
DT_ASSINATURA_CONVENIO TEXT,
MES_ASSINATURA_CONVENIO INT,
ANO_ASSINATURA_CONVENIO INT,
MES_PUBLICACAO_CONVENIO INT,
ANO_PUBLICACAO_CONVENIO INT,
DT_PUBLICACAO TEXT,
DT_ULTIMO_EMPENHO TEXT,
DT_ULTIMO_PGTO TEXT,
VL_GLOBAL TEXT,
VL_REPASSE TEXT,
VL_CONTRAPARTIDA_TOTAL TEXT,
VL_CONTRAPARTIDA_FINANC TEXT,
VL_CONTRAPARTIDA_BENS_SERV TEXT,
VL_DESEMBOLSADO TEXT,
VL_EMPENHADO TEXT,
TX_OBJETO_CONVENIO TEXT,
TX_JUSTIFICATIVA TEXT,
TX_ENDERECO_PROPONENTE TEXT,
TX_BAIRRO_PROPONENTE TEXT,
NR_CEP_PROPONENTE TEXT,
NM_RESPONS_PROPONENTE TEXT,
CD_RESPONS_PROPONENTE BIGINT,
TX_CARGO_RESPONS_PROPONENTE TEXT,
NM_RESPONS_CONCEDENTE TEXT,
CD_RESPONS_CONCEDENTE BIGINT,
TX_CARGO_RESPONS_CONCEDENTE TEXT,
TX_SITUACAO_PUBLICACAO TEXT,
PRIMARY KEY (nr_convenio)
);

COPY diferentonas.convenios(ano_convenio,nr_convenio,tx_modalidade,tx_situacao,tx_subsituacao,cd_orgao_superior,nm_orgao_superior,cd_orgao_concedente,tx_regiao_proponente,uf_proponente,nm_municipio_proponente,nm_proponente,nm_orgao_concedente,dt_inclusao_proposta,cd_programa,nm_programa,cd_acao_ppa,dt_inicio_vigencia,dt_fim_vigencia,dt_assinatura_convenio,mes_assinatura_convenio,ano_assinatura_convenio,mes_publicacao_convenio,ano_publicacao_convenio,dt_publicacao,dt_ultimo_empenho,dt_ultimo_pgto,vl_global,vl_repasse,vl_contrapartida_total,vl_contrapartida_financ,vl_contrapartida_bens_serv,vl_desembolsado,vl_empenhado,tx_objeto_convenio,tx_justificativa,tx_endereco_proponente,tx_bairro_proponente,nr_cep_proponente,nm_respons_proponente,cd_respons_proponente,tx_cargo_respons_proponente,nm_respons_concedente,cd_respons_concedente,tx_cargo_respons_concedente,tx_situacao_publicacao) FROM 'convenios-por-municipio-detalhes.csv' WITH HEADER=true;