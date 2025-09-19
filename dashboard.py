import streamlit as st
import redis
import plotly.express as px
import pandas as pd
from datetime import datetime
import time
import re
import json

# Настройка страницы
st.set_page_config(
    page_title="Аналитика какиш",
    page_icon="📊",
    layout="wide"
)

# Дебаг информация
st.sidebar.title("🔍 Debug Info")
st.sidebar.write("App started at:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

# Функция для парсинга Upstash URL
def parse_upstash_url(redis_url):
    """Парсинг Upstash URL формата rediss://default:password@host:port"""
    try:
        # Извлекаем хост, порт и пароль из URL
        match = re.match(r'rediss://default:([^@]+)@([^:]+):(\d+)', redis_url)
        if match:
            password = match.group(1)
            host = match.group(2)
            port = int(match.group(3))
            return host, port, password
        else:
            st.sidebar.error("Invalid Upstash URL format")
            return None, None, None
    except Exception as e:
        st.sidebar.error(f"URL parsing error: {str(e)}")
        return None, None, None

# Инициализация Redis для пользователей
@st.cache_resource
def init_redis():
    try:
        st.sidebar.info("Initializing Redis connection...")
        
        # Проверка секретов - используем только REDIS_URL
        if "REDIS_URL" not in st.secrets:
            st.sidebar.error("REDIS_URL not found in secrets!")
            return None
        
        redis_url = st.secrets["REDIS_URL"]
        st.sidebar.write("Using REDIS_URL from secrets")
        
        # Парсим URL
        host, port, password = parse_upstash_url(redis_url)
        
        if not all([host, port, password]):
            st.sidebar.error("Failed to parse Redis URL")
            return None
        
        st.sidebar.write(f"Host: {host}")
        st.sidebar.write(f"Port: {port}")
        st.sidebar.write("Password: ******" if password else "No password")
        
        # Подключение к Redis
        st.sidebar.write("Connecting to Redis...")
        r = redis.Redis(
            host=host,
            port=port,
            password=password,  # Используем пароль из URL
            ssl=True,
            ssl_cert_reqs=None,
            decode_responses=True,
            socket_timeout=10,
            socket_connect_timeout=10
        )
        
        # Проверка подключения
        st.sidebar.write("Testing connection...")
        result = r.ping()
        st.sidebar.success(f"✅ Redis connected successfully! Ping: {result}")
        return r
        
    except Exception as e:
        st.sidebar.error(f"❌ Redis connection failed: {str(e)}")
        return None

# Инициализация Redis для событий
@st.cache_resource
def init_redis_events():
    try:
        st.sidebar.info("Initializing Redis Events connection...")
        
        # Проверка секретов - используем REDIS_URL_EVENTS
        if "REDIS_URL_EVENTS" not in st.secrets:
            st.sidebar.error("REDIS_URL_EVENTS not found in secrets!")
            return None
        
        redis_url = st.secrets["REDIS_URL_EVENTS"]
        st.sidebar.write("Using REDIS_URL_EVENTS from secrets")
        
        # Парсим URL для событий
        match = re.match(r'rediss://default:([^@]+)@([^:]+):(\d+)', redis_url)
        if match:
            password = match.group(1)
            host = match.group(2)
            port = int(match.group(3))
        else:
            st.sidebar.error("Invalid Redis Events URL format")
            return None
        
        st.sidebar.write(f"Events Host: {host}")
        st.sidebar.write(f"Events Port: {port}")
        
        # Подключение к Redis для событий
        r = redis.Redis(
            host=host,
            port=port,
            password=password,
            ssl=True,
            ssl_cert_reqs=None,
            decode_responses=True,
            socket_timeout=10,
            socket_connect_timeout=10
        )
        
        # Проверка подключения
        result = r.ping()
        st.sidebar.success(f"✅ Redis Events connected successfully! Ping: {result}")
        return r
        
    except Exception as e:
        st.sidebar.error(f"❌ Redis Events connection failed: {str(e)}")
        return None

# Инициализация обоих подключений
redis_client = init_redis()  # Для пользователей
redis_events_client = init_redis_events()  # Для событий

# Основной заголовок
st.title("📊 Аналитика какиш")
st.caption(f"Last update: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if not redis_client:
    st.error("""
    ❌ Cannot connect to main Redis. Please check REDIS_URL in secrets.
    """)
    st.stop()

# Функции для получения данных пользователей
def get_all_user_keys():
    """Получение всех ключей пользователей"""
    try:
        keys = []
        cursor = 0
        max_iterations = 100
        
        for i in range(max_iterations):
            cursor, partial_keys = redis_client.scan(cursor, match="user:*", count=100)
            keys.extend(partial_keys)
            if cursor == 0:
                break
                
        return keys
        
    except Exception as e:
        st.error(f"Error getting keys: {str(e)}")
        return []

def get_user_data(key):
    """Получение данных пользователя"""
    try:
        user_type = redis_client.type(key)
        
        if user_type == 'hash':
            data = redis_client.hgetall(key)
            data['user_id'] = key
            return data
        else:
            return {'user_id': key}
            
    except Exception as e:
        return {'user_id': key, 'error': str(e)}

def process_users_data():
    """Обработка данных пользователей"""
    st.info("🔄 Loading user data...")
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    keys = get_all_user_keys()
    if not keys:
        st.warning("No user keys found!")
        return pd.DataFrame()
    
    users_data = []
    
    for i, key in enumerate(keys):
        progress = (i + 1) / len(keys)
        progress_bar.progress(progress)
        status_text.text(f"Processing user {i+1}/{len(keys)}")
        
        user_data = get_user_data(key)
        users_data.append(user_data)
        
        if i % 100 == 0:
            time.sleep(0.1)
    
    progress_bar.empty()
    status_text.empty()
    
    if not users_data:
        st.warning("No user data found!")
        return pd.DataFrame()
    
    df = pd.DataFrame(users_data)
    
    # Преобразование дат - пробуем разные возможные колонки
    possible_date_columns = ['agreement', 'agreement_accepted', 'created_at', 'date', 'timestamp', 'registered_at', 'start_date']
    for col in possible_date_columns:
        if col in df.columns:
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                st.sidebar.write(f"✅ Converted {col} to datetime")
            except:
                st.sidebar.write(f"❌ Could not convert {col} to datetime")
    
    # Преобразование bot_was_blocked в boolean
    if 'bot_was_blocked' in df.columns:
        df['bot_was_blocked'] = df['bot_was_blocked'].astype(str).str.lower().isin(['true', '1', 'yes'])
    
    return df

# Загрузка данных пользователей
df = process_users_data()

if df.empty:
    st.info("No user data available. Showing demo data...")
    # Демо данные для тестирования
    demo_data = {
        'user_id': ['user:1', 'user:2', 'user:3', 'user:4', 'user:5'],
        'onboarding_stage': ['complete', 'agreement', 'birth_date', 'complete', 'gender'],
        'agreement': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05'],
        'subscription_expiry': ['2024-12-31', '2024-01-15', '2024-12-31', '2023-12-31', '2024-12-31'],
        'bot_was_blocked': ['True', 'False', 'True', 'False', 'False']
    }
    df = pd.DataFrame(demo_data)
    df['agreement'] = pd.to_datetime(df['agreement'])
    df['subscription_expiry'] = pd.to_datetime(df['subscription_expiry'])
    df['bot_was_blocked'] = df['bot_was_blocked'].astype(bool)

# Покажем доступные колонки для отладки
st.sidebar.subheader("📊 Available Columns")
st.sidebar.write(list(df.columns))

# Поиск колонки с датами для графика
date_column = None
possible_date_columns = ['agreement', 'agreement_accepted', 'created_at', 'date', 'timestamp', 'registered_at', 'start_date']
for col in possible_date_columns:
    if col in df.columns and not df[col].isna().all():
        date_column = col
        break

st.sidebar.write(f"📅 Date column found: {date_column}")

# Верхние метрики
st.subheader("📈 Основные метрики")

col1, col2, col3 = st.columns(3)

with col1:
    total_users = len(df)
    st.metric("👥 Кол-во юзеров", total_users)

with col2:
    if 'onboarding_stage' in df.columns:
        complete_users = len(df[df['onboarding_stage'] == 'complete'])
        st.metric("✅ Клиенты с завершенным онбордингом", complete_users)
    else:
        st.metric("✅ Клиенты с завершенным онбордингом", "N/A")

with col3:
    if 'bot_was_blocked' in df.columns:
        blocked_users = len(df[df['bot_was_blocked'] == True])
        st.metric("🚫 Клиенты забанившие", blocked_users)
    else:
        st.metric("🚫 Клиенты забанившие", "N/A")

# Фильтры для графиков
st.subheader("🎛️ Фильтры")

col1, col2, col3 = st.columns(3)

with col1:
    time_unit = st.selectbox(
        "⏰ Единица времени",
        ["Дни", "Недели", "Месяцы"],
        index=0
    )

with col2:
    # Русские названия для стадий
    stage_options = {
        'agreement': 'Соглашение',
        'birth_date': 'Дата рождения', 
        'gender': 'Пол',
        'goal': 'Цель',
        'activity_level': 'Уровень активности',
        'current_weight': 'Текущий вес',
        'target_weight': 'Целевой вес', 
        'height': 'Рост',
        'daily_calories': 'Калораж',
        'complete': 'Завершенный онбординг'
    }
    
    selected_stages = st.multiselect(
        "🎯 Стадия онбординга",
        options=list(stage_options.keys()),
        format_func=lambda x: stage_options[x],
        default=['complete']
    )

with col3:
    activity_filter = st.selectbox(
        "🔋 Активность клиента",
        ["Все", "Активные", "Неактивные"]
    )

# Применение фильтров
filtered_df = df.copy()

# Фильтр по стадии онбординга
if selected_stages:
    filtered_df = filtered_df[filtered_df['onboarding_stage'].isin(selected_stages)]

# Фильтр по активности (для детальной статистики)
current_time = datetime.now()
if activity_filter == "Активные":
    if 'subscription_expiry' in filtered_df.columns:
        # Преобразуем subscription_expiry в datetime если это еще не сделано
        if filtered_df['subscription_expiry'].dtype != 'datetime64[ns]':
            filtered_df['subscription_expiry'] = pd.to_datetime(filtered_df['subscription_expiry'], errors='coerce')
        filtered_df = filtered_df[filtered_df['subscription_expiry'] > current_time]
elif activity_filter == "Неактивные":
    if 'subscription_expiry' in filtered_df.columns:
        if filtered_df['subscription_expiry'].dtype != 'datetime64[ns]':
            filtered_df['subscription_expiry'] = pd.to_datetime(filtered_df['subscription_expiry'], errors='coerce')
        filtered_df = filtered_df[filtered_df['subscription_expiry'] <= current_time]

# Линейный график по дате
st.subheader("📈 Динамика пользователей по времени")

if date_column and not df[date_column].isna().all():
    time_df = filtered_df.copy()
    time_df = time_df.dropna(subset=[date_column])
    
    if not time_df.empty:
        # Группировка по времени
        if time_unit == "Дни":
            time_df['time_group'] = time_df[date_column].dt.date
        elif time_unit == "Недели":
            time_df['time_group'] = time_df[date_column].dt.to_period('W').dt.start_time
        else:  # Месяцы
            time_df['time_group'] = time_df[date_column].dt.to_period('M').dt.start_time
        
        # Подсчет пользователей по датам
        timeline_data = time_df.groupby('time_group').size().reset_index(name='user_count')
        timeline_data = timeline_data.sort_values('time_group')
        
        # График
        fig_timeline = px.line(
            timeline_data,
            x='time_group',
            y='user_count',
            title=f"Количество пользователей по {time_unit.lower()}",
            labels={'time_group': 'Дата', 'user_count': 'Количество пользователей'}
        )
        st.plotly_chart(fig_timeline, use_container_width=True)
        
        st.info(f"📊 Используется колонка: **{date_column}**")
        st.write(f"**Период:** {timeline_data['time_group'].min()} - {timeline_data['time_group'].max()}")
        st.write(f"**Всего точек данных:** {len(timeline_data)}")
        
    else:
        st.warning(f"Нет данных в колонке {date_column}")
else:
    st.warning("Не найдена подходящая колонка с датами для построения графика")
    st.write("**Доступные колонки:**", list(df.columns))

# Воронка онбординга - КУМУЛЯТИВНАЯ логика
st.subheader("🔄 Воронка онбординга")

# Правильный порядок от соглашения до завершения
onboarding_stages_ordered = [
    'agreement', 'birth_date', 'gender', 'goal', 'activity_level',
    'current_weight', 'target_weight', 'height', 'daily_calories', 'complete'
]

funnel_data = []

for i, stage in enumerate(onboarding_stages_ordered):
    if 'onboarding_stage' in df.columns:
        # Для каждой стадии считаем ВСЕХ пользователей на этой И ПОСЛЕДУЮЩИХ стадиях
        stages_to_include = onboarding_stages_ordered[i:]  # Все стадии от текущей до complete
        
        # Получаем всех пользователей на этих стадиях
        stage_users = set(df[df['onboarding_stage'].isin(stages_to_include)]['user_id'])
        count = len(stage_users)
    else:
        count = 0
    
    funnel_data.append({
        'Стадия': stage_options.get(stage, stage),
        'Количество': count,
        'Порядок': i
    })

funnel_df = pd.DataFrame(funnel_data)
funnel_df = funnel_df.sort_values('Порядок')

if not funnel_df.empty:
    try:
        fig_funnel = px.funnel(
            funnel_df,
            x='Количество',
            y='Стадия',
            title="Воронка онбординга (кумулятивная)",
            labels={'Количество': 'Количество пользователей', 'Стадия': 'Стадия онбординга'}
        )
        st.plotly_chart(fig_funnel, use_container_width=True)
        
        # Также покажем таблицу с данными для ясности
        st.write("**Детализация воронки:**")
        display_df = funnel_df[['Стадия', 'Количество']].copy()
        if display_df['Количество'].iloc[0] > 0:
            display_df['Процент'] = (display_df['Количество'] / display_df['Количество'].iloc[0] * 100).round(1)
            display_df['Процент'] = display_df['Процент'].astype(str) + '%'
        else:
            display_df['Процент'] = '0%'
        st.dataframe(display_df, use_container_width=True)
        
    except Exception as e:
        st.error(f"Ошибка при построении графика: {str(e)}")
        st.write("Данные для воронки:")
        st.dataframe(funnel_df)
else:
    st.warning("Нет данных для построения воронки")

# Детальная статистика
st.subheader("📋 Детальная статистика")

col1, col2 = st.columns(2)

with col1:
    # Статистика по стадиям
    if 'onboarding_stage' in df.columns:
        st.write("**Распределение по стадиям онбординга:**")
        stage_counts = df['onboarding_stage'].value_counts()
        stage_counts_df = stage_counts.reset_index()
        stage_counts_df.columns = ['Стадия', 'Количество']
        stage_counts_df['Стадия'] = stage_counts_df['Стадия'].map(stage_options).fillna(stage_counts_df['Стадия'])
        st.dataframe(stage_counts_df, use_container_width=True)

with col2:
    # Статистика по активности (текущая)
    st.write("**Текущая статистика по активности:**")
    if 'subscription_expiry' in df.columns:
        try:
            # Преобразуем subscription_expiry в datetime если это еще не сделано
            if df['subscription_expiry'].dtype != 'datetime64[ns]':
                df['subscription_expiry'] = pd.to_datetime(df['subscription_expiry'], errors='coerce')
            
            # Фильтруем только валидные даты
            valid_subscriptions = df.dropna(subset=['subscription_expiry'])
            active_users = len(valid_subscriptions[valid_subscriptions['subscription_expiry'] > datetime.now()])
            inactive_users = len(valid_subscriptions) - active_users
            
            activity_stats = pd.DataFrame({
                'Статус': ['Активные', 'Неактивные', 'Без даты истечения'],
                'Количество': [active_users, inactive_users, len(df) - len(valid_subscriptions)]
            })
        except Exception as e:
            st.error(f"Ошибка при обработке subscription_expiry: {str(e)}")
            activity_stats = pd.DataFrame({
                'Статус': ['Ошибка обработки дат'],
                'Количество': [len(df)]
            })
    else:
        active_users = 0
        inactive_users = len(df)
        activity_stats = pd.DataFrame({
            'Статус': ['Активные', 'Неактивные'],
            'Количество': [active_users, inactive_users]
        })
    
    st.dataframe(activity_stats, use_container_width=True)

# Кнопка обновления
if st.button("🔄 Обновить данные", type="primary"):
    st.cache_data.clear()
    st.rerun()

# Информация о данных
st.sidebar.subheader("📊 Data Info")
st.sidebar.write(f"Total users: {len(df)}")
if not df.empty and 'onboarding_stage' in df.columns:
    st.sidebar.write(f"Stages: {df['onboarding_stage'].nunique()} unique")
if date_column:
    st.sidebar.write(f"Date column: {date_column}")

# Функции для работы с событиями
def get_all_event_keys():
    """Получение всех ключей событий"""
    try:
        if not redis_events_client:
            st.error("Redis Events client not initialized")
            return []
            
        keys = []
        cursor = 0
        max_iterations = 50
        
        for i in range(max_iterations):
            cursor, partial_keys = redis_events_client.scan(cursor, match="events_data:*", count=100)
            keys.extend(partial_keys)
            if cursor == 0:
                break
                
        return keys
        
    except Exception as e:
        st.error(f"Error getting event keys: {str(e)}")
        return []

def get_all_event_keys():
    """Получение всех ключей событий"""
    try:
        if not redis_events_client:
            st.error("Redis Events client not initialized")
            return []
            
        keys = []
        cursor = 0
        max_iterations = 50
        
        for i in range(max_iterations):
            cursor, partial_keys = redis_events_client.scan(cursor, match="events_data:*", count=100)
            keys.extend(partial_keys)
            if cursor == 0:
                break
                
        return keys
        
    except Exception as e:
        st.error(f"Error getting event keys: {str(e)}")
        return []

def get_event_data(key):
    """Получение данных события из Hash"""
    try:
        if not redis_events_client:
            return None
            
        # Получаем все поля Hash
        event_data = redis_events_client.hgetall(key)
        if event_data:
            # Пробуем распарсить JSON из поля 'value' или других полей
            if 'value' in event_data:
                try:
                    parsed_data = json.loads(event_data['value'])
                    parsed_data['key'] = key
                    return parsed_data
                except:
                    # Если не JSON, возвращаем как есть
                    event_data['key'] = key
                    return event_data
            else:
                # Ищем любое поле с JSON данными
                for field_name, field_value in event_data.items():
                    if field_name not in ['key', 'timestamp']:  # Пропускаем служебные поля
                        try:
                            parsed_data = json.loads(field_value)
                            parsed_data['key'] = key
                            return parsed_data
                        except:
                            continue
                
                # Если не нашли JSON, возвращаем raw данные
                event_data['key'] = key
                return event_data
        return None
        
    except Exception as e:
        st.warning(f"Error reading event {key}: {str(e)}")
        return None

# Альтернативная версия - если данные в отдельных полях Hash
def get_event_data_alternative(key):
    """Альтернативный метод получения данных события"""
    try:
        if not redis_events_client:
            return None
            
        # Получаем тип ключа
        key_type = redis_events_client.type(key)
        
        if key_type == 'hash':
            # Это Hash - получаем все поля
            event_data = redis_events_client.hgetall(key)
            event_data['key'] = key
            return event_data
        elif key_type == 'string':
            # Это String - пробуем распарсить JSON
            data = redis_events_client.get(key)
            if data:
                try:
                    return json.loads(data)
                except:
                    return {'raw_data': data, 'key': key}
        else:
            return {'key': key, 'type': key_type}
            
    except Exception as e:
        st.warning(f"Error reading event {key}: {str(e)}")
        return None

def calculate_token_costs(event):
    """Расчет стоимости токенов для события"""
    costs = {
        'redis_ops': 0,
        'input_tokens': 0,
        'output_tokens': 0,
        'audio_tokens': 0,
        'cached_tokens': 0,
        'total': 0
    }
    
    # Стоимость Redis операций
    if 'redis_ops' in event:
        costs['redis_ops'] = event['redis_ops'] * 0.0000002
    
    # Стоимость поисковых операций
    search_keys = ['yandex_searches', 'web_searches', 'google_searches']
    for key in search_keys:
        if key in event:
            costs['redis_ops'] += event[key] * 0.0000002
    
    # Стоимость OpenAI токенов
    if 'openai_usage' in event and event['openai_usage']:
        for usage in event['openai_usage']:
            # Audio tokens
            audio_tokens = 0
            if 'completion_tokens_details' in usage and 'audio_tokens' in usage['completion_tokens_details']:
                audio_tokens += usage['completion_tokens_details']['audio_tokens']
            if 'prompt_tokens_details' in usage and 'audio_tokens' in usage['prompt_tokens_details']:
                audio_tokens += usage['prompt_tokens_details']['audio_tokens']
            costs['audio_tokens'] += audio_tokens * 0.00000025
            
            # Cached tokens
            cached_tokens = 0
            if 'prompt_tokens_details' in usage and 'cached_tokens' in usage['prompt_tokens_details']:
                cached_tokens = usage['prompt_tokens_details']['cached_tokens']
            costs['cached_tokens'] += cached_tokens * 0.00000001
            
            # Input tokens (prompt_tokens - audio_tokens - cached_tokens)
            prompt_tokens = usage.get('prompt_tokens', 0)
            input_tokens = prompt_tokens - audio_tokens - cached_tokens
            costs['input_tokens'] += max(0, input_tokens) * 0.0000004
            
            # Output tokens (completion_tokens - audio_tokens)
            completion_tokens = usage.get('completion_tokens', 0)
            output_tokens = completion_tokens - audio_tokens
            costs['output_tokens'] += max(0, output_tokens) * 0.0000016
    
    costs['total'] = sum(costs.values())
    return costs
        
        # ... остальной код ...
# Обновленная функция обработки с проверкой типа данных
def process_events_data():
    """Обработка данных событий"""
    if not redis_events_client:
        st.error("Redis Events not connected")
        return pd.DataFrame()
        
    st.info("🔄 Loading events data...")
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    keys = get_all_event_keys()
    if not keys:
        st.warning("No event keys found!")
        return pd.DataFrame()
    
    events_data = []
    key_types = {}  # Для отладки типов ключей
    
    for i, key in enumerate(keys[:200]):  # Ограничим для теста
        progress = (i + 1) / min(len(keys), 200)
        progress_bar.progress(progress)
        status_text.text(f"Processing event {i+1}/{min(len(keys), 200)}")
        
        # Проверяем тип ключа
        try:
            key_type = redis_events_client.type(key)
            key_types[key_type] = key_types.get(key_type, 0) + 1
        except:
            key_type = 'unknown'
        
        # Пробуем оба метода получения данных
        event_data = get_event_data(key)
        if not event_data:
            event_data = get_event_data_alternative(key)
        
        if event_data:
            events_data.append(event_data)
        
        if i % 20 == 0:
            time.sleep(0.01)
    
    progress_bar.empty()
    status_text.empty()
    
    # Покажем статистику по типам ключей
    st.sidebar.write("**Event Key Types:**", key_types)
    
    if not events_data:
        st.warning("No event data found!")
        return pd.DataFrame()
    
    # Конвертируем в DataFrame
    try:
        df_events = pd.DataFrame(events_data)
        return df_events
    except Exception as e:
        st.error(f"Error creating events DataFrame: {str(e)}")
        st.write("Raw events data sample:", events_data[:3])
        return pd.DataFrame()
        
# Загрузка данных событий и анализ стоимости
# if redis_events_client:
#     events_df = process_events_data()
    
#     if not events_df.empty:
#         st.subheader("💰 Анализ стоимости токенов")
        
#         # Расчет стоимости
#         costs_data = []
#         for _, event_row in events_df.iterrows():
#             # Преобразуем Series в словарь
#             event = event_row.to_dict()
#             costs = calculate_token_costs(event)
#             costs['timestamp'] = event.get('timestamp')
#             costs['event_id'] = event.get('event_id')
#             costs['user_id'] = event.get('user_id')
#             costs_data.append(costs)
        
#         costs_df = pd.DataFrame(costs_data)
        
#         # Преобразование timestamp
#         if 'timestamp' in costs_df.columns:
#             costs_df['timestamp'] = pd.to_datetime(costs_df['timestamp'], errors='coerce')
#             costs_df = costs_df.dropna(subset=['timestamp'])
        
#         if not costs_df.empty:
#             # График стоимости токенов
#             col1, col2 = st.columns(2)
#             with col1:
#                 cost_time_unit = st.selectbox(
#                     "⏰ Единица времени для стоимости",
#                     ["Дни", "Недели", "Месяцы"],
#                     index=0
#                 )
            
#             # Группировка по времени
#             if cost_time_unit == "Дни":
#                 costs_df['time_group'] = costs_df['timestamp'].dt.date
#             elif cost_time_unit == "Недели":
#                 costs_df['time_group'] = costs_df['timestamp'].dt.to_period('W').dt.start_time
#             else:
#                 costs_df['time_group'] = costs_df['timestamp'].dt.to_period('M').dt.start_time
            
#             # Агрегация
#             grouped_costs = costs_df.groupby('time_group').agg({
#                 'redis_ops': 'sum',
#                 'input_tokens': 'sum',
#                 'output_tokens': 'sum',
#                 'audio_tokens': 'sum',
#                 'cached_tokens': 'sum',
#                 'total': 'sum'
#             }).reset_index()
            
#             # Stacked bar chart
#             fig_costs = px.bar(
#                 grouped_costs,
#                 x='time_group',
#                 y=['redis_ops', 'input_tokens', 'output_tokens', 'audio_tokens', 'cached_tokens'],
#                 title=f"Стоимость токенов по {cost_time_unit.lower()} ($)",
#                 labels={'value': 'Стоимость ($)', 'time_group': 'Дата', 'variable': 'Тип токенов'}
#             )
            
#             st.plotly_chart(fig_costs, use_container_width=True)
            
#             # Общая статистика
#             total_costs = {
#                 'Redis Ops': costs_df['redis_ops'].sum(),
#                 'Input Tokens': costs_df['input_tokens'].sum(),
#                 'Output Tokens': costs_df['output_tokens'].sum(),
#                 'Audio Tokens': costs_df['audio_tokens'].sum(),
#                 'Cached Tokens': costs_df['cached_tokens'].sum(),
#                 'Total Cost': costs_df['total'].sum()
#             }
            
#             st.write("**Общая стоимость:**")
#             for key, value in total_costs.items():
#                 st.write(f"- {key}: ${value:.6f}")
                
#         else:
#             st.warning("Нет данных о стоимости для построения графика")
#     else:
#         st.info("Данные событий не найдены")
# else:
#     st.info("Подключение к базе событий не настроено")
if redis_events_client:
    events_df = process_events_data()
    
    if not events_df.empty:
        st.subheader("💰 Анализ стоимости токенов")
        
        # Расчет стоимости
        costs_data = []
        for _, event_row in events_df.iterrows():
            # Преобразуем Series в словарь
            event = event_row.to_dict()
            costs = calculate_token_costs(event)
            costs['timestamp'] = event.get('timestamp')
            costs['event_id'] = event.get('event_id')
            costs['user_id'] = event.get('user_id')
            costs_data.append(costs)
st.sidebar.success("✅ Dashboard loaded successfully!")



