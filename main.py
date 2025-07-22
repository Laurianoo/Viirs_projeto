from datetime import datetime, timedelta
import shapely.geometry
import pandas as pd
import geopandas as gpd
import time
from geopy.distance import geodesic
import configparser
import random
from zap import *
import json
import os
import logging
import pyautogui

setup_logging()

config_path = os.path.join('config', "config.ini") # Caminho do arquivo de configuração
config = configparser.ConfigParser() # Ler o arquivo de configuração
config.read(config_path)

# Função para verificar se um foco está próximo de alguma indústria
def foco_em_industria(foco, industrias, raio_km=1.5):
    foco_coord = (foco['latitude'], foco['longitude'])
    for _, industria in industrias.iterrows():
        industria_coord = (industria['latitude'], industria['longitude'])
        if geodesic(foco_coord, industria_coord).km <= raio_km:
            return True
    return False

def viirs_utc_to_brasilia(acq_date, acq_time):
    from datetime import datetime, timedelta
    time_str = f"{int(acq_time):04d}" # Garante que acq_time tenha 4 dígitos (ex: 332 → '0332')
    datetime_utc = datetime.strptime(f"{acq_date} {time_str}", "%Y-%m-%d %H%M") # Combina data e hora
    datetime_brt = datetime_utc - timedelta(hours=3) # Converte de UTC para BRT (UTC-3)
    datetime_brt_str = datetime_brt.strftime("%H:%M") # Formato HH:MM
    return datetime_brt_str

def main(hora):
    hoje = time.strftime("%Y-%m-%d")
    logging.info(f"Hoje é: {hoje}")
    enviar = hora
    hora_envio(enviar)

    MAP_KEY = config["FIRMS"]["KEY"]
    url_status = f'https://firms.modaps.eosdis.nasa.gov/mapserver/mapkey_status/?MAP_KEY={MAP_KEY}'

    try:
        df_status = pd.read_json(url_status, typ='series')
        tcount = df_status.get('current_transactions', "N/A")
        logging.info(f'Our current transaction count is {tcount}')
    except Exception as e:
        logging.error(f"There is an issue with the query for transaction count: {url_status}. Error: {e}")

    rio_bbox = "-45.4,-23.6,-40.9,-20.7"
    dfs = []

    try:
        # URLs dos dados FIRMS
        sources = {
            "VIIRS_NOAA20_NRT": f'https://firms.modaps.eosdis.nasa.gov/api/area/csv/{MAP_KEY}/VIIRS_NOAA20_NRT/{rio_bbox}/1',
            "VIIRS_NOAA21_NRT": f'https://firms.modaps.eosdis.nasa.gov/api/area/csv/{MAP_KEY}/VIIRS_NOAA21_NRT/{rio_bbox}/1',
            "MODIS_NRT": f'https://firms.modaps.eosdis.nasa.gov/api/area/csv/{MAP_KEY}/MODIS_NRT/{rio_bbox}/1',
            "VIIRS_SNPP_NRT": f'https://firms.modaps.eosdis.nasa.gov/api/area/csv/{MAP_KEY}/VIIRS_SNPP_NRT/{rio_bbox}/1'}

        for source_name, csv_url in sources.items():
            try:
                df_source = pd.read_csv(csv_url)
                if not df_source.empty:
                    dfs.append(df_source)
                logging.info(f"Dados de {source_name} carregados: {len(df_source)} registros.")
            except Exception as e:
                logging.warning(f"Falha ao carregar dados de {source_name} da URL {csv_url}. Erro: {e}")
        
        if not dfs:
            logging.info("Nenhum dado de foco de calor foi carregado das fontes FIRMS.")
            rio_df = pd.DataFrame()
        else:
            rio_df = pd.concat(dfs, ignore_index=True)
            logging.info(f"Total de focos de calor combinados antes da filtragem: {len(rio_df)}")

        df_industrias = pd.read_excel('calor_fixo.xlsx')
        logging.info(f"Dados de indústrias carregados: {len(df_industrias)} registros.")

    except Exception as e:
        logging.error(f"Erro crítico ao carregar os dados iniciais (FIRMS ou indústrias): {e}")
        return

    if rio_df.empty:
        logging.info("DataFrame de focos de calor está vazio. Não há dados para processar.")
        mensagem = f"Nenhum foco de calor encontrado hoje ({hoje}) no Rio de Janeiro após consulta inicial às fontes."
        logging.info(mensagem)
        return

    try:
        # Adiciona uma coluna indicando se o foco está em indústria
        rio_df['em_industria'] = rio_df.apply(lambda row: foco_em_industria(row, df_industrias), axis=1)
        # Filtra apenas os focos do dia de hoje e que NÃO são fixos (não estão em indústria)
        rio_df_hoje_sem_fixos_pd = rio_df[(rio_df['acq_date'] == hoje) & (~rio_df['em_industria'])].copy() # Use ~ para NOT True, e .copy()
        
        if rio_df_hoje_sem_fixos_pd.empty:
            logging.info(f"Nenhum foco de calor encontrado para hoje ({hoje}) que não seja em indústria.")
            mensagem = f"Nenhum foco de calor (excluindo áreas industriais) encontrado hoje ({hoje}) no Rio de Janeiro."
            logging.info(mensagem)
            return

        logging.info(f"Focos para hoje ({hoje}) não industriais: {len(rio_df_hoje_sem_fixos_pd)}")

        # Converte para GeoDataFrame
        geometry = gpd.points_from_xy(rio_df_hoje_sem_fixos_pd['longitude'], rio_df_hoje_sem_fixos_pd['latitude'])
        # Adiciona 'original_id' para rastrear pontos de forma única através dos joins
        current_gdf = gpd.GeoDataFrame(
            rio_df_hoje_sem_fixos_pd.reset_index(drop=True).reset_index().rename(columns={'index': 'original_id'}),
            geometry=geometry,
            crs="EPSG:4326"
        )
        logging.info(f"GeoDataFrame inicial criado com {len(current_gdf)} pontos.")

        # Join espacial com municípios, bairros e distritos
        rio_shape = gpd.read_file(r'RJ_setores_CD2022\RJ_setores_CD2022.shp')
        rio_shape = rio_shape.to_crs(current_gdf.crs)
        
        current_gdf = gpd.sjoin(current_gdf, rio_shape[['geometry', 'NM_MUN', 'NM_BAIRRO', 'NM_DIST']], how='inner', predicate='within')
        current_gdf = current_gdf.drop_duplicates(subset=['original_id'], keep='first')
        current_gdf = current_gdf.rename(columns={'NM_MUN': 'municipio', 'NM_BAIRRO': 'Bairro', 'NM_DIST': 'Distrito'})
        if 'index_right' in current_gdf.columns:
            current_gdf = current_gdf.drop(columns=['index_right'])
        
        if current_gdf.empty:
            logging.info(f"Nenhum foco de calor encontrado dentro dos limites municipais definidos no shapefile para hoje ({hoje}).")
            return
        logging.info(f"Pontos após join com municípios: {len(current_gdf)}")

        UCs = gpd.read_file(r'UCs_ZAs\ucs_estaduais.shp')
        ZAs = gpd.read_file(r'UCs_ZAs\gpl_ucs_estaduais_ZA.shp')
        UCs = UCs.to_crs(current_gdf.crs)
        ZAs = ZAs.to_crs(current_gdf.crs)

        # Join com Unidades de Conservação (UCs)
        uc_data_to_join = UCs[['geometry', 'nome']].rename(columns={'nome': 'Unidade de Conservação'})
        current_gdf = gpd.sjoin(current_gdf, uc_data_to_join, how='left', predicate='within')
        current_gdf = current_gdf.drop_duplicates(subset=['original_id'], keep='first')
        if 'index_right' in current_gdf.columns:
            current_gdf = current_gdf.drop(columns=['index_right'])
        logging.info(f"Pontos após join com UCs (antes da filtragem final): {len(current_gdf)}")

        # Join com Zonas de Amortecimento (ZAs)
        za_data_to_join = ZAs[['geometry', 'Nome']].rename(columns={'Nome': 'Zona de Amortecimento'})
        current_gdf = gpd.sjoin(current_gdf, za_data_to_join, how='left', predicate='within')
        current_gdf = current_gdf.drop_duplicates(subset=['original_id'], keep='first')
        if 'index_right' in current_gdf.columns:
            current_gdf = current_gdf.drop(columns=['index_right'])
        logging.info(f"Pontos após join com ZAs (antes da filtragem final): {len(current_gdf)}")
        
        # Filtrar: manter apenas focos que estão em UMA UC OU UMA ZA (ou ambas)
        current_gdf = current_gdf[current_gdf['Unidade de Conservação'].notna() | current_gdf['Zona de Amortecimento'].notna()].copy()

        if current_gdf.empty:
            logging.info(f"Nenhum foco de calor encontrado em Unidades de Conservação ou Zonas de Amortecimento para hoje ({hoje}).")
            mensagem = f"Nenhum foco de calor detectado em Unidades de Conservação ou Zonas de Amortecimento hoje ({hoje}) no Rio de Janeiro."
            logging.info(mensagem)
            return
            
        logging.info(f"Pontos finais após filtro de UC/ZA: {len(current_gdf)}")
        
        colunas_finais_desejadas = [
            'latitude', 'longitude', 'acq_date', 'acq_time', 'daynight',
            'municipio', 'Bairro', 'Distrito', 'satellite', 'instrument',
            'Unidade de Conservação', 'Zona de Amortecimento']
        # Selecionar apenas as colunas existentes em current_gdf para evitar erros
        colunas_presentes = [col for col in colunas_finais_desejadas if col in current_gdf.columns]
        rio_df_final_para_mensagem = current_gdf[colunas_presentes].reset_index(drop=True)
        
        lista_dicts = rio_df_final_para_mensagem.to_dict(orient='records')
        logging.info("Dados finais dos focos (lista de dicionários):")
        for item in lista_dicts: # Log formatado para melhor leitura
            logging.info(item)
        
        mensagem = f"Focos de calor encontrados no Rio de Janeiro hoje ({hoje}) em UCs ou ZAs:\n\n"
        qt = len(rio_df_final_para_mensagem)
        if qt == 0:
            mensagem = f"Nenhum foco de calor encontrado hoje ({hoje}) no Rio de Janeiro em UCs ou ZAs após todas as filtragens."
            logging.info(mensagem)
            return

        for i in range(qt):
            ponto_atual = rio_df_final_para_mensagem.iloc[i]
            h = viirs_utc_to_brasilia(ponto_atual['acq_date'], ponto_atual['acq_time'])
            mensagem += f"Foco {i+1}:\n"
            mensagem += f"  Latitude: {ponto_atual['latitude']}\n"
            mensagem += f"  Longitude: {ponto_atual['longitude']}\n"
            mensagem += f"  Data: {ponto_atual['acq_date']}\n"
            mensagem += f"  Hora (Brasília): {h}\n"
            mensagem += f"  Período: {'Dia' if ponto_atual['daynight'] == 'D' else 'Noite'}\n"
            mensagem += f"  Município: {ponto_atual.get('municipio', 'N/A')}\n"
            if pd.notna(ponto_atual.get('Bairro')):
                mensagem += f"  Bairro: {ponto_atual['Bairro']}\n"
            if pd.notna(ponto_atual.get('Distrito')):
                mensagem += f"  Distrito: {ponto_atual['Distrito']}\n"
            if 'Unidade de Conservação' in ponto_atual and pd.notna(ponto_atual['Unidade de Conservação']):
                mensagem += f"  Unidade de Conservação: {ponto_atual['Unidade de Conservação']}\n"
            if 'Zona de Amortecimento' in ponto_atual and pd.notna(ponto_atual['Zona de Amortecimento']):
                mensagem += f"  Zona de Amortecimento: {ponto_atual['Zona de Amortecimento']}\n"
            mensagem += f"  Fonte: {ponto_atual.get('satellite', 'N/A')}, {ponto_atual.get('instrument', 'N/A')}\n\n"

        print("--- MENSAGEM GERADA ---")
        print(mensagem)
        print("-----------------------")
        try:
            dia, ultimos_focos = carregar_estado()
            if dia == hoje and ultimos_focos == qt:
                logging.info(f"Já foi enviada uma mensagem hoje ({hoje}) com {qt} focos. Aguardando novas detecções.")
                return
        except Exception as e:
            logging.error(f"Erro durante o carregamento do arquivo de estado: {e}")
        try:
            enviar_mensagem("7ExFDthoBHrJgycHSTy4X67GVFSsffH9", "120363399968018396@g.us", mensagem)
            salvar_estado(hoje, qt)
            logging.info("Processo de alerta concluído e mensagem enviada.")
        except Exception as e:
            logging.error(f"Erro durante o agendamento ou envio da mensagem: {e}")
    except Exception as e:
        logging.error(f"Erro durante o processamento geoespacial ou geração da mensagem: {e}", exc_info=True)

# Execução, você define o horário de rodagem na chamada da main, pode chamar diversas vezes e deixar rodando o dia todo de fundo
try:
    main('10:30')
except Exception as e:
    logging.error(f"Erro fatal na execução do script principal: {e}", exc_info=True)
try:
    main('16:00')
except Exception as e:
    logging.error(f"Erro fatal na execução do script principal: {e}", exc_info=True)
try:
    main('17:30')
except Exception as e:
    logging.error(f"Erro fatal na execução do script principal: {e}", exc_info=True)