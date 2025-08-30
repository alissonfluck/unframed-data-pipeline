import psycopg
import sys # Usado para obter detalhes do erro
import os
import json
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

PROCESSED_DATA_DIR = 'processed_data'

# Tentar estabelecer uma conexão
try:
    with psycopg.connect (
        dbname = DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    ) as conn:

        # Imprimir mensagem de sucesso
        print("✅ Conexão com o banco de dados PostgreSQL bem-sucedida!")

        with conn.cursor() as cur:
            try:
                # --- 1. Leitura do Arquivo SQL ---
                print("Lendo o arquivo 'create_tables.sql'...")
                with open('create_tables.sql', 'r', encoding='utf-8') as f:
                    sql_script = f.read()
                print("✅ Arquivo lido com sucesso.")
                # --- 2. Execução do Script SQL ---
                print("Executando o script para criar as tabelas...")
                cur.execute(sql_script)
                print("🚀 Tabelas criadas com sucesso!")

            except FileNotFoundError as e:
                print(f"❌ ERRO: {e}") 
                sys.exit(1)
            
            
            # --- 3. Carregamento dos Dados do Arquivo JSON
            try:
                print(f"\nProcurando pelo lote de dados mais recente em '{PROCESSED_DATA_DIR}'...")

                # Lista com o caminho completo de todos os arquivos .json
                json_files = [
                    os.path.join(PROCESSED_DATA_DIR, f)
                    for f in os.listdir(PROCESSED_DATA_DIR)
                    if f.endswith('.json')
                ]

                if not json_files:
                    raise FileNotFoundError(f"Nenhum arquivo .json encontrado no diretório '{PROCESSED_DATA_DIR}'.")
                # Usa a data de modificação para encontrar o arquivo mais recente
                latest_file_path = max(json_files, key=os.path.getmtime)

                print(f"Arquivo mais recente encontrado: {os.path.basename(latest_file_path)}")

                # Carregando conteúdo do arquivo em uma variável
                with open(latest_file_path, 'r', encoding='utf-8') as f:
                    events_data = json.load(f)

                # Mostrando o número total de eventos
                total_events = len(events_data)
                print(f"✅ Carregados {total_events} eventos do arquivo.") 
            
            except FileNotFoundError as e:
                print(f"❌ ERRO: {e}")
                sys.exit(1)

            # --- 4. Inserção dos Dados no Banco de Dados ---
            print("\nIniciando inserção dos eventos no banco de dados...")

            inserted_count = 0
            unmatched_count = 0

            for event in events_data:
                event_name = event.get('envelope_eventName')

                # Roteador de eventos
                if event_name == "acquisition.visitor.landed":
                    sql = """
                        INSERT INTO visitor_landed (
                            envelope_eventId, envelope_eventTimestamp, envelope_eventName, envelope_eventVersion, 
                            envelope_source, envelope_domain, payload_anonymousId, payload_landingPageUrl, 
                            payload_attribution_source, payload_attribution_medium, payload_attribution_campaign, 
                            payload_device_type, payload_device_browser, payload_device_os, payload_geolocation_country, 
                            payload_geolocation_region, payload_geolocation_city, payload_browserLanguage
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
                    """
                    
                    # Criando a tupla de dados na ordem do comando INSERT
                    data_tuple_visitor = (
                        event.get('envelope_eventId'),
                        event.get('envelope_eventTimestamp'),
                        event.get('envelope_eventName'),
                        event.get('envelope_eventVersion'),
                        event.get('envelope_source'),
                        event.get('envelope_domain'),
                        event.get('payload_anonymousId'),
                        event.get('payload_landingPageUrl'),
                        event.get('payload_attribution_source'),
                        event.get('payload_attribution_medium'),
                        event.get('payload_attribution_campaign'),
                        event.get('payload_device_type'),
                        event.get('payload_device_browser'),
                        event.get('payload_device_os'),
                        event.get('payload_geolocation_country'),
                        event.get('payload_geolocation_region'),
                        event.get('payload_geolocation_city'),
                        event.get('payload_browserLanguage')
                    )

                    # Executando o comando de forma segura
                    cur.execute(sql, data_tuple_visitor)
                    inserted_count += 1

                elif event_name == "membership.user.created":
                    sql = """
                        INSERT INTO user_created (
                            envelope_eventId, envelope_eventTimestamp, envelope_eventName, envelope_eventVersion,
                            envelope_source, envelope_domain, payload_userId, payload_anonymousId,
                            payload_emailHash, payload_initialPlanId, payload_acquisitionChannel
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
                    """
                    data_tuple_user = (
                        event.get('envelope_eventId'),
                        event.get('envelope_eventTimestamp'),
                        event.get('envelope_eventName'),
                        event.get('envelope_eventVersion'),
                        event.get('envelope_source'),
                        event.get('envelope_domain'),
                        event.get('payload_userId'),
                        event.get('payload_anonymousId'),
                        event.get('payload_emailHash'),
                        event.get('payload_initialPlanId'),
                        event.get('payload_acquisitionChannel')
                    )

                    cur.execute(sql, data_tuple_user)
                    inserted_count += 1

                elif event_name == "playback.session.started":
                    sql = """
                        INSERT INTO playback_started (
                            envelope_eventId, envelope_eventTimestamp, envelope_eventName, envelope_eventVersion,
                            envelope_source, envelope_domain, payload_userId, payload_profileId,
                            payload_playback_sessionId, payload_videoId, payload_videoType, payload_device_type,
                            payload_device_manufacturer, payload_device_os, payload_trigger, payload_playbackStartTime
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
                    """
                    data_tuple_playback = (
                        event.get('envelope_eventId'),
                        event.get('envelope_eventTimestamp'),
                        event.get('envelope_eventName'),
                        event.get('envelope_eventVersion'),
                        event.get('envelope_source'),
                        event.get('envelope_domain'),
                        event.get('payload_userId'),
                        event.get('payload_profileId'),
                        event.get('payload_playbackSessionId'),
                        event.get('payload_videoId'),
                        event.get('payload_videoType'),
                        event.get('payload_device_type'),
                        event.get('payload_device_manufacturer'),
                        event.get('payload_device_os'),
                        event.get('payload_trigger'),
                        event.get('payload_playbackStartTime')
                    )

                    cur.execute(sql, data_tuple_playback)
                    inserted_count += 1

                elif event_name == "membership.user.login_succeeded":
                    sql = """
                        INSERT INTO login_succeeded (
                            envelope_eventId, envelope_eventTimestamp, envelope_eventName, envelope_eventVersion,
                            envelope_source, envelope_domain, payload_userId, payload_loginType, payload_isNewDevice
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);
                    """
                    data_tuple_login_succeeded = (
                        event.get('envelope_eventId'),
                        event.get('envelope_eventTimestamp'),
                        event.get('envelope_eventName'),
                        event.get('envelope_eventVersion'),
                        event.get('envelope_source'),
                        event.get('envelope_domain'),
                        event.get('payload_userId'),
                        event.get('payload_loginType'),
                        event.get('payload_isNewDevice')
                    )

                    cur.execute(sql, data_tuple_login_succeeded)
                    inserted_count += 1

                elif event_name == "membership.user.login_failed":
                    sql = """
                        INSERT INTO login_failed (
                            envelope_eventId, envelope_eventTimestamp, envelope_eventName, envelope_eventVersion,
                            envelope_source, envelope_domain, payload_emailAttempted, payload_failureReason,
                            payload_consecutiveFailureCount
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);
                    """
                    data_tuple_login_failed = (
                        event.get('envelope_eventId'),
                        event.get('envelope_eventTimestamp'),
                        event.get('envelope_eventName'),
                        event.get('envelope_eventVersion'),
                        event.get('envelope_source'),
                        event.get('envelope_domain'),
                        event.get('payload_emailAttempted'),
                        event.get('payload_failureReason'),
                        event.get('payload_consecutiveFailureCount')
                    )
                    
                    cur.execute(sql, data_tuple_login_failed)
                    inserted_count += 1

                else:
                    unmatched_count += 1

            conn.commit()

            print(f"✅ Transação confirmada! Inseridos {inserted_count} eventos com sucesso no banco de dados.")
            if unmatched_count > 0:
                print(f"⚠️  {unmatched_count} eventos não tiveram correspondência e foram ignorados.")

except FileNotFoundError:
    print("❌ ERRO: O arquivo 'create_tables.sql' não foi encontrado.")
    print("   Verifique se o arquivo está na mesma pasta que o script Python.")
    sys.exit(1)
except psycopg.Error as e:
    print(f"❌ Erro ao conectar ao PostgreSQL: {e}")
    sys.exit(1)