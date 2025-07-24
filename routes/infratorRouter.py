from bson import ObjectId
from fastapi import APIRouter, HTTPException, Query, UploadFile, File
from fastapi.responses import StreamingResponse
from models.infratores import InfratorCreate, InfratorOut, PaginatedInfratorResponse
from database import infrator_collection, enquadramento_collection, auto_infracao_collection
import matplotlib.pyplot as plt
import pandas as pd
import io
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import math
from logs.logger import logger

router = APIRouter(prefix="/infrator", tags=["Infrator"])

@router.post("/upload-csv")
async def upload_csv(file: UploadFile = File(...)):
    """
    Endpoint para upload e processamento do arquivo CSV de infrações ambientais
    """
    try:
        # Verificar se o arquivo é CSV
        if not file.filename.endswith('.csv'):
            raise HTTPException(status_code=400, detail="Arquivo deve ser um CSV")
        
        # Ler o conteúdo do arquivo
        content = await file.read()
        
        # Criar DataFrame a partir do CSV com otimizações
        # O CSV usa ponto e vírgula como separador
        df = pd.read_csv(
            io.StringIO(content.decode('utf-8')), 
            sep=';',
            dtype=str,  # Força todos os campos como string para evitar warnings
            low_memory=False
        )
        
        logger.info(f"CSV carregado com {len(df)} registros")
        logger.info(f"Colunas disponíveis: {list(df.columns)}")
        
        # Verificar se as colunas necessárias existem
        required_columns = ['NOME_INFRATOR', 'DT_INICIO_ATO_INEQUIVOCO', 'DT_FIM_ATO_INEQUIVOCO']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            raise HTTPException(
                status_code=400, 
                detail=f"Colunas obrigatórias não encontradas no CSV: {missing_columns}"
            )
        
        # Converter datas uma vez para toda a coluna (mais eficiente)
        df['DT_INICIO_CONVERTED'] = pd.to_datetime(df['DT_INICIO_ATO_INEQUIVOCO'], errors='coerce')
        df['DT_FIM_CONVERTED'] = pd.to_datetime(df['DT_FIM_ATO_INEQUIVOCO'], errors='coerce')
        
        # Filtrar linhas com datas válidas
        df_valid = df.dropna(subset=['DT_INICIO_CONVERTED', 'DT_FIM_CONVERTED'])
        registros_com_datas_invalidas = len(df) - len(df_valid)
        
        logger.info(f"Registros com datas válidas: {len(df_valid)}")
        if registros_com_datas_invalidas > 0:
            logger.warning(f"Registros com datas inválidas ignorados: {registros_com_datas_invalidas}")
        
        # Preparar dados para inserção em lote
        infratores_para_inserir = []
        infratores_para_atualizar = []
        erros = []
        
        # Buscar todos os infratores existentes de uma vez (mais eficiente)
        existing_infratores = {}
        try:
            async for doc in infrator_collection.find({}, {"nome_infrator": 1, "municipio": 1, "estado": 1, "historico_infracoes": 1}):
                # Verificar se o documento tem os campos necessários
                if all(field in doc for field in ['nome_infrator', 'municipio', 'estado']):
                    # Usar nome + município + estado como chave única (não as datas)
                    key = f"{doc['nome_infrator']}_{doc['municipio']}_{doc['estado']}"
                    existing_infratores[key] = doc
        except Exception as e:
            logger.warning(f"Erro ao buscar infratores existentes: {str(e)}")
            # Se houver erro, continuar com cache vazio
            existing_infratores = {}
        
        logger.info(f"Infratores existentes na base: {len(existing_infratores)}")
        
        # Processar dados - PRIMEIRO: agrupar por infrator
        logger.info(f"Iniciando processamento de {len(df_valid)} registros válidos...")
        
        # Dicionário para agrupar infrações por infrator (mesmo CSV)
        infratores_agrupados = {}
        
        for index, row in df_valid.iterrows():
            try:
                # Debug: log do primeiro registro para verificar estrutura
                if index == 0:
                    logger.info(f"Primeiro registro - Colunas da linha: {list(row.index)}")
                
                dt_inicio = row['DT_INICIO_CONVERTED'].to_pydatetime()
                dt_fim = row['DT_FIM_CONVERTED'].to_pydatetime()
                
                # Função helper para obter valor seguro da coluna
                def get_safe_value(row, column_name, default=""):
                    try:
                        if column_name not in row.index:
                            logger.warning(f"Coluna '{column_name}' não encontrada na linha {index + 1}")
                            return default
                        value = row[column_name]
                        if pd.isna(value) or str(value).lower() == 'nan':
                            return default
                        return str(value).strip()
                    except Exception as e:
                        logger.warning(f"Erro ao obter valor da coluna '{column_name}' na linha {index + 1}: {str(e)}")
                        return default
                
                # Verificar se nome_infrator existe e não está vazio
                nome_infrator = get_safe_value(row, 'NOME_INFRATOR')
                municipio = get_safe_value(row, 'MUNICIPIO')
                estado = get_safe_value(row, 'UF')
                
                if not nome_infrator:
                    erros.append(f"Linha {index + 1}: Nome do infrator está vazio ou ausente")
                    continue
                
                # Chave única para agrupamento
                key = f"{nome_infrator}_{municipio}_{estado}"
                nova_infracao = get_safe_value(row, 'DES_INFRACAO')
                
                if key not in infratores_agrupados:
                    # Primeiro registro deste infrator
                    infratores_agrupados[key] = {
                        "nome_infrator": nome_infrator,
                        "infracao_area": get_safe_value(row, 'INFRACAO_AREA'),
                        "municipio": municipio,
                        "estado": estado,
                        "des_local_infracao": get_safe_value(row, 'DES_LOCAL_INFRACAO'),
                        "historico_infracoes": [nova_infracao] if nova_infracao else [],
                        "dt_inicio_ato_inequivoco": dt_inicio,
                        "dt_fim_ato_inequivoco": dt_fim,
                        "total_infrações": 1
                    }
                else:
                    # Infrator já existe, adicionar infração ao histórico
                    infrator_existente = infratores_agrupados[key]
                    
                    # Adicionar nova infração se não estiver no histórico
                    if nova_infracao and nova_infracao not in infrator_existente["historico_infracoes"]:
                        infrator_existente["historico_infracoes"].append(nova_infracao)
                    
                    # Atualizar datas (mais antiga para início, mais recente para fim)
                    if dt_inicio < infrator_existente["dt_inicio_ato_inequivoco"]:
                        infrator_existente["dt_inicio_ato_inequivoco"] = dt_inicio
                    
                    if dt_fim > infrator_existente["dt_fim_ato_inequivoco"]:
                        infrator_existente["dt_fim_ato_inequivoco"] = dt_fim
                    
                    infrator_existente["total_infrações"] += 1
                
                # Log de progresso a cada 1000 registros
                if (index + 1) % 1000 == 0:
                    logger.info(f"Processados {index + 1} registros...")
                
            except Exception as e:
                erros.append(f"Linha {index + 1}: {str(e)}")
                logger.error(f"Erro ao processar linha {index + 1}: {str(e)}")
                # Para debug, logar os primeiros 5 erros com mais detalhes
                if len(erros) <= 5:
                    logger.error(f"Detalhes do erro na linha {index + 1}: {type(e).__name__}: {str(e)}")
        
        logger.info(f"Agrupamento concluído. {len(infratores_agrupados)} infratores únicos identificados no CSV")
        
        # SEGUNDO: Verificar quais já existem no banco e separar para inserção/atualização
        for key, infrator_data in infratores_agrupados.items():
            existing_infrator = existing_infratores.get(key)
            
            if existing_infrator:
                # Infrator já existe no banco - preparar atualizações
                novas_infracoes = []
                for infracao in infrator_data["historico_infracoes"]:
                    if infracao not in existing_infrator.get('historico_infracoes', []):
                        novas_infracoes.append(infracao)
                
                if novas_infracoes:
                    infratores_para_atualizar.append({
                        "_id": existing_infrator["_id"],
                        "novas_infracoes": novas_infracoes,
                        "dt_inicio": infrator_data["dt_inicio_ato_inequivoco"],
                        "dt_fim": infrator_data["dt_fim_ato_inequivoco"]
                    })
            else:
                # Novo infrator - preparar para inserção
                # Remover campo auxiliar antes de inserir
                infrator_data_clean = {k: v for k, v in infrator_data.items() if k != "total_infrações"}
                infratores_para_inserir.append(infrator_data_clean)
        
        logger.info(f"Processamento concluído. Novos infratores: {len(infratores_para_inserir)}, Atualizações: {len(infratores_para_atualizar)}, Erros: {len(erros)}")
        
        # Inserção em lote (muito mais eficiente)
        infratores_inseridos = 0
        if infratores_para_inserir:
            try:
                result = await infrator_collection.insert_many(infratores_para_inserir)
                infratores_inseridos = len(result.inserted_ids)
                logger.info(f"Inseridos {infratores_inseridos} novos infratores em lote")
            except Exception as e:
                logger.error(f"Erro na inserção em lote: {str(e)}")
                erros.append(f"Erro na inserção em lote: {str(e)}")
        
        # Atualizações em lote
        atualizacoes_realizadas = 0
        if infratores_para_atualizar:
            try:
                for update_data in infratores_para_atualizar:
                    # Preparar atualizações
                    update_fields = {"$addToSet": {"historico_infracoes": {"$each": update_data["novas_infracoes"]}}}
                    
                    # Atualizar dt_inicio se for mais antiga, dt_fim se for mais recente
                    existing_doc = await infrator_collection.find_one({"_id": update_data["_id"]})
                    if existing_doc:
                        if update_data["dt_inicio"] < existing_doc.get("dt_inicio_ato_inequivoco", update_data["dt_inicio"]):
                            update_fields["$set"] = update_fields.get("$set", {})
                            update_fields["$set"]["dt_inicio_ato_inequivoco"] = update_data["dt_inicio"]
                        
                        if update_data["dt_fim"] > existing_doc.get("dt_fim_ato_inequivoco", update_data["dt_fim"]):
                            update_fields["$set"] = update_fields.get("$set", {})
                            update_fields["$set"]["dt_fim_ato_inequivoco"] = update_data["dt_fim"]
                    
                    await infrator_collection.update_one(
                        {"_id": update_data["_id"]},
                        update_fields
                    )
                    atualizacoes_realizadas += 1
                    
                logger.info(f"Atualizados {atualizacoes_realizadas} infratores existentes")
            except Exception as e:
                logger.error(f"Erro nas atualizações: {str(e)}")
                erros.append(f"Erro nas atualizações: {str(e)}")
        
        # Estatísticas do processamento
        total_registros = len(df)
        registros_com_erro = len(erros)
        
        logger.info(f"Upload concluído: {infratores_inseridos} novos infratores inseridos, {atualizacoes_realizadas} atualizações, {registros_com_erro} erros")
        
        return {
            "message": "Upload processado com sucesso",
            "total_registros": total_registros,
            "registros_validos": len(df_valid),
            "registros_com_datas_invalidas": registros_com_datas_invalidas,
            "infratores_inseridos": infratores_inseridos,
            "infratores_atualizados": atualizacoes_realizadas,
            "registros_com_erro": registros_com_erro,
            "erros": erros[:10] if erros else []  # Retorna apenas os 10 primeiros erros
        }
        
    except Exception as e:
        logger.error(f"Erro no upload do CSV: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao processar arquivo: {str(e)}")

@router.get("/stats")
async def get_infratores_stats():
    """
    Endpoint para obter estatísticas dos infratores
    """
    try:
        # Total de infratores
        total_infratores = await infrator_collection.count_documents({})
        
        if total_infratores == 0:
            return {
                "message": "Nenhum infrator encontrado na base de dados",
                "total_infratores": 0,
                "coleção_existe": False
            }
        
        # Infratores por estado
        pipeline_estados = [
            {"$group": {"_id": "$estado", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        infratores_por_estado = []
        async for doc in infrator_collection.aggregate(pipeline_estados):
            infratores_por_estado.append({"estado": doc["_id"], "count": doc["count"]})
        
        # Infratores por tipo de área
        pipeline_areas = [
            {"$group": {"_id": "$infracao_area", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        infratores_por_area = []
        async for doc in infrator_collection.aggregate(pipeline_areas):
            if doc["_id"]:  # Ignorar valores vazios
                infratores_por_area.append({"area": doc["_id"], "count": doc["count"]})
        
        return {
            "total_infratores": total_infratores,
            "coleção_existe": True,
            "infratores_por_estado": infratores_por_estado,
            "infratores_por_area": infratores_por_area
        }
        
    except Exception as e:
        logger.error(f"Erro ao obter estatísticas: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao obter estatísticas: {str(e)}")

@router.get("/")
async def list_infratores(
    page: int = Query(1, ge=1, description="Número da página"),
    size: int = Query(10, ge=1, le=100, description="Itens por página"),
    nome: str = Query(None, description="Filtrar por nome do infrator"),
    estado: str = Query(None, description="Filtrar por estado"),
    municipio: str = Query(None, description="Filtrar por município")
):
    """
    Listar infratores com paginação e filtros
    """
    try:
        # Construir filtros
        filters = {}
        if nome:
            filters["nome_infrator"] = {"$regex": nome, "$options": "i"}
        if estado:
            filters["estado"] = {"$regex": estado, "$options": "i"}
        if municipio:
            filters["municipio"] = {"$regex": municipio, "$options": "i"}
        
        # Calcular skip
        skip = (page - 1) * size
        
        # Buscar total de registros
        total = await infrator_collection.count_documents(filters)
        
        if total == 0:
            return PaginatedInfratorResponse(
                total=0,
                page=page,
                size=size,
                items=[]
            )
        
        # Buscar infratores
        cursor = infrator_collection.find(filters).skip(skip).limit(size)
        infratores = []
        
        async for doc in cursor:
            doc["id"] = str(doc["_id"])
            del doc["_id"]
            infratores.append(InfratorOut(**doc))

        logger.info(f"Listando {len(infratores)} infratores na página {page} com tamanho {size}")
        
        return PaginatedInfratorResponse(
            total=total,
            page=page,
            size=size,
            items=infratores
        )
        
    except Exception as e:
        logger.error(f"Erro ao listar infratores: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao listar infratores: {str(e)}")

@router.get("/infratores/count")
async def count_infratores(
    nome: str = Query(None, description="Filtrar por nome do infrator"),
    estado: str = Query(None, description="Filtrar por estado"),
    municipio: str = Query(None, description="Filtrar por município")
):
    """
    Contar infratores com filtros
    """
    try:
        # Construir filtros
        filters = {}
        if nome:
            filters["nome_infrator"] = {"$regex": nome, "$options": "i"}
        if estado:
            filters["estado"] = {"$regex": estado, "$options": "i"}
        if municipio:
            filters["municipio"] = {"$regex": municipio, "$options": "i"}
        
        # Contar infratores
        total = await infrator_collection.count_documents(filters)
        
        logger.info(f"Total de infratores encontrados: {total}")
        
        return {"total_infratores": total}
        
    except Exception as e:
        logger.error(f"Erro ao contar infratores: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao contar infratores: {str(e)}")
    
@router.get("/infrator_report")
async def infrator_report():
    """
    Endpoint para gerar relatório de infratores, incluindo gráficos e estatísticas
    """
    logger.info("Gerando relatório de infratores")
    try:
        cursor = infrator_collection.find({})
        data = await cursor.to_list(length=None)

        if not data:
            logger.warning("Nenhum infrator encontrado para o relatório")
            return {"message": "Nenhum infrator encontrado para o relatório"}
        
        df = pd.DataFrame(data)
        if df.empty:
            logger.warning("DataFrame vazio, nenhum infrator encontrado")
            return {"message": "Nenhum infrator encontrado para o relatório"}
        
        # Converter datas
        df['dt_inicio_ato_inequivoco'] = pd.to_datetime(df['dt_inicio_ato_inequivoco'], errors='coerce')
        df['dt_fim_ato_inequivoco'] = pd.to_datetime(df['dt_fim_ato_inequivoco'], errors='coerce')
        df.dropna(subset=['dt_inicio_ato_inequivoco', 'dt_fim_ato_inequivoco'], inplace=True)
        if df.empty:
            logger.warning("Após conversão de datas, DataFrame ainda está vazio")
            return {"message": "Nenhum infrator encontrado para o relatório"}
        # Agrupar por estado
        infratores_por_estado = df.groupby('estado').size().reset_index(name='count')
        infratores_por_estado = infratores_por_estado.sort_values(by='count', ascending=False)
        logger.info(f"Infratores por estado: {infratores_por_estado.to_dict(orient='records')}")
        # Agrupar por área de infração
        infratores_por_area = df.groupby('infracao_area').size().reset_index(name='count')
        infratores_por_area = infratores_por_area.sort_values(by='count', ascending=False)
        logger.info(f"Infratores por área de infração: {infratores_por_area.to_dict(orient='records')}")
        # Criar gráfico de barras para infratores por estado
        plt.figure(figsize=(10, 6))
        plt.bar(infratores_por_estado['estado'], infratores_por_estado['count'], color='skyblue')
        plt.title('Número de Infratores por Estado')
        plt.xlabel('Estado')
        plt.ylabel('Número de Infratores')
        plt.xticks(rotation=45)
        plt.tight_layout()
        img_estado = io.BytesIO()
        plt.savefig(img_estado, format='png')
        img_estado.seek(0)
        
        logger.info("Gráfico de infratores por estado gerado com sucesso")
        return StreamingResponse(
            img_estado,
            media_type="image/png",
            headers={"Content-Disposition": "inline; filename=infratores_por_estado.png"}
        )
    except Exception as e:
        logger.error(f"Erro ao gerar relatório de infratores: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao gerar relatório de infratores: {str(e)}") 
    finally:
        plt.close()

