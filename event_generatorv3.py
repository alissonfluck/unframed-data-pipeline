import uuid
from datetime import datetime, timezone, date, time, timedelta
import json
from faker import Faker
import random
import os

fake = Faker('pt_BR')

OUTPUT_PATH = "landing_zone"
BRAZILIAN_CITIES = [
    # Sudeste
    ("São Paulo", "SP"), ("Rio de Janeiro", "RJ"), ("Belo Horizonte", "MG"), ("Vitória", "ES"),
    ("Guarulhos", "SP"), ("Campinas", "SP"), ("São Bernardo do Campo", "SP"), ("Santo André", "SP"),
    ("Osasco", "SP"), ("Sorocaba", "SP"), ("Ribeirão Preto", "SP"), ("São José dos Campos", "SP"),
    ("Uberlândia", "MG"), ("Contagem", "MG"), ("Juiz de Fora", "MG"), ("Betim", "MG"),
    ("Duque de Caxias", "RJ"), ("Nova Iguaçu", "RJ"), ("Niterói", "RJ"), ("Campos dos Goytacazes", "RJ"),
    ("Serra", "ES"), ("Vila Velha", "ES"),

    # Sul
    ("Curitiba", "PR"), ("Porto Alegre", "RS"), ("Florianópolis", "SC"), ("Joinville", "SC"),
    ("Londrina", "PR"), ("Caxias do Sul", "RS"), ("Maringá", "PR"), ("Blumenau", "SC"),
    ("Ponta Grossa", "PR"), ("Cascavel", "PR"), ("Foz do Iguaçu", "PR"),
    ("Pelotas", "RS"), ("Canoas", "RS"), ("Santa Maria", "RS"),
    ("Chapecó", "SC"), ("Itajaí", "SC"), ("Criciúma", "SC"),

    # Nordeste
    ("Salvador", "BA"), ("Fortaleza", "CE"), ("Recife", "PE"), ("São Luís", "MA"),
    ("Maceió", "AL"), ("Natal", "RN"), ("João Pessoa", "PB"), ("Teresina", "PI"),
    ("Aracaju", "SE"), ("Jaboatão dos Guararapes", "PE"), ("Feira de Santana", "BA"),
    ("Campina Grande", "PB"), ("Olinda", "PE"), ("Caucaia", "CE"),
    ("Vitória da Conquista", "BA"), ("Paulista", "PE"), ("Caruaru", "PE"),
    ("Imperatriz", "MA"), ("Petrolina", "PE"),

    # Centro-Oeste
    ("Brasília", "DF"), ("Goiânia", "GO"), ("Campo Grande", "MS"), ("Cuiabá", "MT"),
    ("Aparecida de Goiânia", "GO"), ("Anápolis", "GO"), ("Várzea Grande", "MT"),
    ("Dourados", "MS"), ("Rondonópolis", "MT"),

    # Norte
    ("Manaus", "AM"), ("Belém", "PA"), ("Porto Velho", "RO"), ("Macapá", "AP"),
    ("Palmas", "TO"), ("Rio Branco", "AC"), ("Boa Vista", "RR"),
    ("Ananindeua", "PA"), ("Santarém", "PA"), ("Marabá", "PA")
]

MOVIE_GENRES = [
    "horror", "documentary", "sci-fi", "comedy", "drama", "action", "thriller", "romance", "animation",
    "fantasy", "adventure", "mystery", "crime", "musical", "family", "biography", "war", "western",
    "historical", "sports", "superhero"
]

CONTENT_TYPES = [
    "movie", "series", "miniseries", "short-film", "documentary-series", "anime", "reality-show", "talk-show"
]

def generate_envelope(event_name: str, event_time: datetime, version: str = "1.0.0") -> dict:
    # Extraindo o domínio a partir do nome do evento e convertendo para Uppercase o primeiro caractere
    domain = event_name.split('.')[0].capitalize()

    # Espalhando os eventos ao longo do dia
    random_delta = timedelta(seconds=random.randint(0, 600))
    final_event_time = event_time + random_delta

    envelope = {
        "eventId": str(uuid.uuid4()),
        "eventTimestamp": final_event_time.isoformat().replace('+00:00', 'Z'),
        "eventName": event_name,
        "eventVersion": version,
        "source": "event-generator-v1", 
        "domain": domain
    }
    return envelope

def generate_payload_visitor_landed() -> dict: # Gerando o payload realista para o evento
    city, state = random.choice(BRAZILIAN_CITIES)

    # Decidir aleatoriamente se a URL será genérica ou específica
    if random.random() < 0.7: # 70% de chance de ser específica
        content_type = random.choice(CONTENT_TYPES)
        genre = random.choice(MOVIE_GENRES)
        video_id = fake.random_int(min=1000, max=9999)
        landing_page_url = f"https://unframed.com/genre/{genre}/{content_type}/{video_id}"
    else:
        possible_paths = ["/", "/planos", "/ajuda"] # 30% de chance de ser genérica
        landing_page_url = f"https://unframed.com{random.choice(possible_paths)}"

    payload = {
        "anonymousId": f"session_{fake.uuid4()}",
        "landingPageUrl": landing_page_url,
        "attribution": {
            "source": fake.random_element(elements=('google', 'instagram', 'facebook', 'organic')),
            "medium": fake.random_element(elements=('cpc', 'social_paid', 'referral')),
            "campaign": fake.bs().replace(' ', '_').lower() # Gera um jargão de negócios
        },
        "device": {
            "type": fake.random_element(elements=('desktop', 'mobile')),
            "browser": fake.user_agent().split(' ')[0], # Pega a primeira parte do User Agent
            "os": fake.random_element(elements=('Windows', 'MacOS', 'Linux', 'Android', 'iOS'))
        },
        "geolocation": {
            "country": "BR",
            "region": state,
            "city": city
        },
        "browserLanguage": "pt_BR"
    }
    return payload

# Função payload user_created
def generate_payload_user_created(anonymous_id: str) -> dict:
    payload = {
        "userId": f"usr_{fake.uuid4()}",
        "anonymousId": anonymous_id, # ID da sessão que levou a pessoa ao cadastro (ponto de costura)
        "emailHash": fake.sha256(),
        "initialPlanId": fake.random_element(elements=('premium_monthly', 'basic_annual')),
        "acquisitionChannel": fake.random_element(elements=('google_cpc', 'instagram_social_paid', 'organic')),
    }
    return payload

# Função payload session_started
def generate_payload_playback_started(user: dict) -> dict:
    payload = {
        "userId": user["userId"],
        "profileId": user["profileId"],
        "playbackSessionId": f"play_{uuid.uuid4()}",
        "videoId": fake.random_int(min=10000, max=99999),
        "videoType": fake.random_element(elements=('movie', 'series_episode')),
        "device": {
            "type": fake.random_element(elements=('smart_tv', 'mobile', 'desktop', 'tablet')),
            "manufacturer": fake.random_element(elements=('Samsung', 'LG', 'Apple', 'Sony')),
            "os": fake.random_element(elements=('Tizen', 'webOS', 'iOS', 'Android', 'Windows'))
        },
        "trigger": fake.random_element(elements=('user_click_on_recommendation', 'autoplay', 'search_result')),
        "playbackStartTime": 0
    }
    return payload

# Função payload user.login_succeeded
def generate_payload_login_succeeded(user: dict) -> dict:
    payload = {
        "userId": user["userId"], # Mantendo a coerência do userId
        "loginType": fake.random_element(elements=('password', 'google_sso')),
        "isNewDevice": fake.boolean(chance_of_getting_true=15) # 15% de chance de um novo aparelho
    }
    return payload

# Função payload user.login.failed
def generate_payload_login_failed() -> dict:
    payload = {
        "emailAttempted": fake.email(),
        "failureReason": fake.random_element(elements=('WRONG_PASSWORD', 'ACCOUNT_NOT_FOUND', 'ACCOUNT_LOCKED')),
        "consecutiveFailureCount": fake.random_int(min=1, max=5)
    }
    return payload

# =============================================================================
# FUNÇÕES DE SIMULAÇÃO DE JORNADA
# =============================================================================

def simulate_bounce(base_dt: datetime) -> list:
    # Jornada: Visitante entra e sai
    payload_landed = generate_payload_visitor_landed()
    return [{"envelope": generate_envelope("acquisition.visitor.landed", base_dt), "payload": payload_landed}]

def simulate_signup_only(base_dt: datetime) -> list:
    # Jornada: Visitante se cadastra, mas não faz mais nada
    journey = []
    payload_landed = generate_payload_visitor_landed()
    journey.append({"envelope": generate_envelope("acquisition.visitor.landed", base_dt), "payload": payload_landed})
    payload_created = generate_payload_user_created(payload_landed["anonymousId"])
    journey.append({"envelope": generate_envelope("membership.user.created", base_dt), "payload": payload_created})
    return journey

def simulate_failed_login(base_dt: datetime) -> list:
    # Jornada: Usuário tenta logar e falha.
    payload_failed = generate_payload_login_failed()
    return [{"envelope": generate_envelope("membership.user.login_failed", base_dt), "payload": payload_failed}]

def simulate_explorer(base_dt: datetime) -> list:
    # Jornada: Usuário se cadastra, loga mas não assiste
    journey = []
    payload_landed = generate_payload_visitor_landed()
    journey.append({"envelope": generate_envelope("acquisition.visitor.landed", base_dt), "payload": payload_landed})
    payload_created = generate_payload_user_created(payload_landed["anonymousId"])
    journey.append({"envelope": generate_envelope("membership.user.created", base_dt), "payload": payload_created})
    user_data = {"userId": payload_created["userId"]}
    payload_login = generate_payload_login_succeeded(user_data)
    journey.append({"envelope": generate_envelope("membership.user.login_succeeded", base_dt), "payload": payload_login})
    return journey

def simulate_full_engagement(base_dt: datetime) -> list:
    # Jornada: O caminho completo
    journey = []
    payload_landed = generate_payload_visitor_landed()
    journey.append({"envelope": generate_envelope("acquisition.visitor.landed", base_dt), "payload": payload_landed})
    payload_created = generate_payload_user_created(payload_landed["anonymousId"])
    journey.append({"envelope": generate_envelope("membership.user.created", base_dt), "payload": payload_created})
    user_data = {"userId": payload_created["userId"], "profileId": f"prof_{fake.uuid4()}"}
    payload_login = generate_payload_login_succeeded(user_data)
    journey.append({"envelope": generate_envelope("membership.user.login_succeeded", base_dt), "payload": payload_login})
    for _ in range(random.randint(1, 3)):
        payload_playback = generate_payload_playback_started(user_data)
        journey.append({"envelope": generate_envelope("playback.session.started", base_dt), "payload": payload_playback})
    return journey

# =============================================================================
# ORQUESTRAÇÃO PRINCIPAL (COM PROBABILIDADES)
# =============================================================================

def generate_random_journey(journey_date: date):
    # A jornada é decidida com base em pesos de probabilidades
    base_dt = datetime.combine(journey_date, time(hour=random.randint(0, 23)), tzinfo=timezone.utc)

    journey_functions = [
        simulate_bounce,
        simulate_signup_only,
        simulate_failed_login,
        simulate_explorer,
        simulate_full_engagement
    ]
    # Pesos ajustáveis para conseguir mudar o perfil dos dados
    weights = [0.40, 0.25, 0.05, 0.15, 0.15] # bounce, signup, failed login, explorer e full_engagement

    chosen_journey_func = random.choices(journey_functions, weights=weights, k=1)[0]
    return chosen_journey_func(base_dt)

if __name__ == "__main__":
    # Garante que a pasta de destino exista
    os.makedirs(OUTPUT_PATH, exist_ok=True)

    # Define os dias para os quais queremos gerar dados
    DATES_TO_GENERATE = [
        date(2025, 9, 1),
        date(2025, 5, 8),
        date(2025, 6, 6),
        date(2025, 11, 9),
        date(2025, 8, 6),
        date(2025, 8, 9),
        date(2025, 3, 11),
        date(2025, 7, 28),
        date(2025, 10, 29)
    ]
    NUM_JOURNEYS_PER_DAY = 100

    print(f"Iniciando geração de dados para {len(DATES_TO_GENERATE)} dias...")

    for target_date in DATES_TO_GENERATE:
        all_events_for_day = []
        date_str = target_date.strftime("%Y-%m-%d")
        print(f"\nGerando {NUM_JOURNEYS_PER_DAY} jornadas para o dia {date_str}...")

        for i in range(NUM_JOURNEYS_PER_DAY):
            journey = generate_random_journey(target_date)
            all_events_for_day.extend(journey)

        # Exportação para arquivo
        output_filename = f"user_journeys_{date_str}.json"
        full_output_path = os.path.join(OUTPUT_PATH, output_filename)
        
        try:
            with open(full_output_path, 'w', encoding='utf-8') as f:
                json.dump(all_events_for_day, f, indent=2, ensure_ascii=False)
            print(f"✅ {len(all_events_for_day)} eventos para {date_str} salvos em: {full_output_path}")
        except Exception as e:
            print(f"❌ Ocorreu um erro ao salvar o arquivo para {date_str}: {e}")