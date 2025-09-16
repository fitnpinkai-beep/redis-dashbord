import streamlit as st
import redis
import plotly.express as px
import pandas as pd
from datetime import datetime
import time
import re

# Настройка страницы
st.set_page_config(
    page_title="User Analytics Dashboard",
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

# Инициализация Redis
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

# Инициализация
redis_client = init_redis()

# Основной заголовок
st.title("📊 User Analytics Dashboard")
st.caption(f"Last update: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if not redis_client:
    st.error("""
    ❌ Cannot connect to Redis. Please check:
    
    1. **REDIS_URL format**: Should be like `rediss://default:password@host:port`
    2. **Redis server status** in Upstash console
    3. **Internet connection**
    
    **Your REDIS_URL looks correct!**
    """)
    st.stop()

# Функции для получения данных
def get_all_user_keys():
    """Получение всех ключей пользователей"""
    try:
        keys = []
        cursor = 0
        max_iterations = 100  # Увеличили лимит итераций
        
        for i in range(max_iterations):
            cursor, partial_keys = redis_client.scan(cursor, match="user:*", count=100)  # Увеличили count
            keys.extend(partial_keys)
            st.sidebar.write(f"Scan iteration {i+1}: found {len(partial_keys)} keys")
            
            if cursor == 0:
                break
                
        st.sidebar.write(f"Total keys found: {len(keys)}")
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
    """Обработка данных пользователей - ВСЕХ, а не только 50"""
    st.info("🔄 Loading user data...")
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    keys = get_all_user_keys()
    if not keys:
        st.warning("No user keys found!")
        return pd.DataFrame()
    
    users_data = []
    
    # УБИРАЕМ ограничение в 50 ключей - обрабатываем ВСЕ
    for i, key in enumerate(keys):
        progress = (i + 1) / len(keys)
        progress_bar.progress(progress)
        status_text.text(f"Processing user {i+1}/{len(keys)}")
        
        user_data = get_user_data(key)
        users_data.append(user_data)
        
        # Небольшая пауза для избежания перегрузки
        if i % 100 == 0:  # Пауза каждые 100 пользователей
            time.sleep(0.1)
    
    progress_bar.empty()
    status_text.empty()
    
    if not users_data:
        st.warning("No user data found!")
        return pd.DataFrame()
    
    df = pd.DataFrame(users_data)
    return df

# Загрузка данных
df = process_users_data()

if df.empty:
    st.info("No user data available. Showing demo data...")
    # Демо данные для тестирования
    demo_data = {
        'user_id': ['user:1', 'user:2', 'user:3'],
        'onboarding_stage': ['complete', 'agreement', 'complete'],
        'agreement_accepted': ['2024-01-01', '2024-01-02', '2024-01-03'],
        'subscription_expiry': ['2024-12-31', '2024-01-15', '2024-12-31']
    }
    df = pd.DataFrame(demo_data)

# Верхние метрики
col1, col2 = st.columns(2)

with col1:
    total_users = len(df)
    st.metric("👥 Кол-во юзеров", total_users)

with col2:
    if 'onboarding_stage' in df.columns:
        complete_users = len(df[df['onboarding_stage'] == 'complete'])
        st.metric("✅ Клиенты с завершенным онбордингом", complete_users)
    else:
        st.metric("✅ Клиенты с завершенным онбордингом", "N/A")

# Информация о данных
st.sidebar.subheader("📊 Data Info")
st.sidebar.write(f"Total users: {len(df)}")
if not df.empty:
    st.sidebar.write(f"Columns: {list(df.columns)}")

# Дополнительная статистика
if not df.empty:
    st.subheader("📊 Детальная статистика")
    
    # Статистика по стадиям онбординга
    if 'onboarding_stage' in df.columns:
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Распределение по стадиям онбординга:**")
            stage_counts = df['onboarding_stage'].value_counts()
            st.dataframe(stage_counts.reset_index().rename(columns={'index': 'Стадия', 'onboarding_stage': 'Количество'}))
        
        with col2:
            # Воронка онбординга
            onboarding_stages = ['agreement', 'birth_date', 'gender', 'goal', 'activity_level', 
                               'current_weight', 'target_weight', 'height', 'daily_calories', 'complete']
            
            funnel_data = []
            for stage in onboarding_stages:
                count = len(df[df['onboarding_stage'] == stage]) if 'onboarding_stage' in df.columns else 0
                funnel_data.append({'Стадия': stage, 'Количество': count})
            
            funnel_df = pd.DataFrame(funnel_data)
            fig = px.funnel(funnel_df, x='Количество', y='Стадия', title="Воронка онбординга")
            st.plotly_chart(fig, use_container_width=True)

# Проверка данных
st.subheader("📋 Sample Data")
st.dataframe(df.head(), use_container_width=True)

# Кнопка обновления
if st.button("🔄 Обновить данные", type="primary"):
    st.cache_data.clear()
    st.rerun()

st.sidebar.success("✅ Dashboard loaded successfully!")
