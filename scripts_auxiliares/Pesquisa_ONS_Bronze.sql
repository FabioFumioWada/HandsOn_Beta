SELECT run_id, arquivo_origem, caminho_origem, nome_dataset,
       status, mensagem
FROM handson_beta.controle_global.controle_importacao
WHERE fonte = 'ONS'
  AND etapa = 'INGESTAO_BRONZE'
ORDER BY data_execucao_utc DESC
LIMIT 5;