import json
import os
import shutil
from datetime import datetime

# === Configuração das pastas ===
LANDING_ZONE_PATH = "landing_zone"
ARCHIVE_PATH = "archive"
ERROR_PATH = "error"
OUTPUT_PATH = "processed_data"

def validate_event(event: dict) -> bool:
    # Realizando uma validação estrutural básica no evento
    if "envelope" not in event or "payload" not in event:
        return False
    
    envelope = event["envelope"]
    required_keys = ["eventId", "eventTimestamp", "eventName"]

    for key in required_keys:
        if key not in envelope:
            return False
        
    return True

def clean_event(event: dict) -> dict:
    # Aplicando regras de correção e limpeza
    # Correção do erro gerado propositalmente
    if event["envelope"]["eventName"] == "acquisiton.visitor.landed":
        event["envelope"]["eventName"] = "acquisition.visitor.landed"
        print(f"=== Corrigindo eventName para o eventId: {event['envelope']['eventId']}")
    return event

# Função para achatamento
def flatten_event(unflattened_event: dict) -> dict: # Cada chave transformada em coluna na tabela
    # Transformando um evento aninhado em um dicionário de nível único
    flattened_event = {}

    # Função auxiliar para lidar com aninhamento recursivo (se houver)
    def process_dictionary(d: dict, prefix=''):
        for key, value in d.items():
            new_key = f"{prefix}{key}"
            if isinstance(value, dict):
                process_dictionary(value, f"{new_key}_")
            else:
                flattened_event[new_key] = value
    process_dictionary(unflattened_event)
    return flattened_event

# === Bloco principal de execução ===

if __name__ == "__main__":
    # Verificando e garantindo que as pastas de trabalho existem, se não, as pastas serão criadas
    for path in [LANDING_ZONE_PATH, ARCHIVE_PATH, ERROR_PATH, OUTPUT_PATH]:
        os.makedirs(path, exist_ok=True)

    print(f"[{datetime.now()}] Procurando por novos arquivos em '{LANDING_ZONE_PATH}'...")
 
    # 1. Listar arquivos novos
    # Olha para a landing_zone e faz uma lista de todos os arquivos .json que estão esperando para serem processados
    files_to_process = [f for f in os.listdir(LANDING_ZONE_PATH) if f.endswith('.json')]

    if not files_to_process:
        print("Nenhum arquivo novo para processar.")
    else:
        print(f"Encontrados {len(files_to_process)} arquivos: {files_to_process}")

        current_batch = []

        for file_name in files_to_process:
            source_path = os.path.join(LANDING_ZONE_PATH, file_name)
            print(f"\n=== Processando arquivo: {file_name} ===")
            # Tente processar este arquivo. Se qualquer erro acontecer durante o processo, não pare
            try:
                # 2. Extract
                with open(source_path, 'r', encoding='utf-8') as f:
                    raw_events = json.load(f)

                # 3. Transform (Validar, Limpar e Achatar)
                for event in raw_events:
                    if validate_event(event):
                        cleaned_event = clean_event(event)
                        flattened_event = flatten_event(cleaned_event)
                        current_batch.append(flattened_event)

                # 4. Mover para arquivo (Gerenciamento de Estado)
                # Move o arquivo de landing_zone para a pasta archive
                destination_file_path = os.path.join(ARCHIVE_PATH, file_name)
                shutil.move(source_path, destination_file_path)
                print(f"Arquivo '{file_name}' processado com sucesso e movido para '{ARCHIVE_PATH}'.")

            except Exception as e:
                print(f"Erro ao processar o arquivo '{file_name}': {e}")
                error_file_path = os.path.join(ERROR_PATH, file_name)
                shutil.move(destination_file_path, error_file_path)
                print(f"Arquivo '{file_name}' movido para '{ERROR_PATH}'.")

        # 5. Salvar o lote processado (Load)
        if current_batch:
            batch_timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
            output_filename = f"lote_processado_{batch_timestamp}.json"
            output_path = os.path.join(OUTPUT_PATH, output_filename)

            # Pega todos os eventos que foram processados com sucesso nesta "rodada" e os salva em um novo arquivo de lote na pasta processed_data
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(current_batch, f, indent=2, ensure_ascii=False)
            print(f"\nLote de {len(current_batch)} eventos processados salvo em '{output_path}'")
            







