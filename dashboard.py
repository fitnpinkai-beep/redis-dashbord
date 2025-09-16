import streamlit as st
import redis
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from urllib.parse import urlparse
import time
import json

# Настройка страницы
st.set_page_config(
    page_title="User Analytics Dashboard",
    page_icon="📊",
    layout="wide"
)

# Дебаг информация
st.sidebar.title("🔍 Debug Info")
st.sidebar.write("App started at:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

# Проверка окружения
st.sidebar.subheader("Environment")
st.sidebar.write("Python version:", st.sidebar.code(str(pd.__version__)))
try:
    import redis as redis_lib
    st.sidebar.write("Redis version:", st.sidebar.code(redis_lib.__version__))
except:
    st.sidebar.write("Redis: Not available")

# Инициализация Redis
@st.cache_resource
def init_redis():
    try:
        st.sidebar.info("Initializing Redis connection...")
        
        # Проверка секретов
        if "REDIS_URL" not in st.secrets or "REDIS_TOKEN" not in st.secrets:
            st.sidebar.error("Redis secrets not found!")
            return None
        
        redis_url = st.secrets["REDIS_URL"]
        redis_token = st.secrets["REDIS_TOKEN"]
        
        # Парсинг URL
        parsed_url = urlparse(redis_url)
        host = parsed_url.hostname
        port = parsed_url.port or 6379
        
        # Подключение
        r = redis.Redis(
            host=host,
            port=port,
            password=redis_token,
            ssl=True,
            ssl_cert_reqs=None,
            decode_responses=True,
            socket_timeout=10,
            socket_connect_timeout=10
        )
        
        # Проверка подключения
        r.ping()
        st.sidebar.success("✅ Redis connected successfully!")
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
    1. REDIS_URL and REDIS_TOKEN in secrets
    2. Internet connection
    3. Redis server status
    """)
    st.stop()

# Функции для получения данных
def get_all_user_keys():
    """Получение всех ключей пользователей"""
    try:
        keys = []
        cursor = 0
        while True:
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
            return redis_client.hgetall(key)
        elif user_type == 'string':
            data = redis_client.get(key)
            try:
                return json.loads(data)
            except:
                return {'raw_data': data}
        else:
            return {}
    except Exception as e:
        st.warning(f"Error reading user {key}: {str(e)}")
        return {}

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
        if user_data:
            user_data['user_id'] = key
            users_data.append(user_data)
    
    progress_bar.empty()
    status_text.empty()
    
    if not users_data:
        st.warning("No user data found!")
        return pd.DataFrame()
    
    df = pd.DataFrame(users_data)
    
    # Преобразование дат
    date_columns = ['agreement_accepted', 'subscription_expiry', 'created_at']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    return df

# Загрузка данных
df = process_users_data()

if df.empty:
    st.stop()

# Верхние метрики
col1, col2 = st.columns(2)

with col1:
    total_users = len(df)
    st.metric("👥 Кол-во юзеров", total_users)

with col2:
    complete_users = len(df[df['onboarding_stage'] == 'complete'])
    st.metric("✅ Клиенты с завершенным онбордингом", complete_users)

# Фильтры
st.subheader("📊 Фильтры для графиков")

col1, col2, col3 = st.columns(3)

with col1:
    time_unit = st.selectbox(
        "⏰ Единица времени",
        ["Дни", "Недели", "Месяцы"],
        index=0
    )

with col2:
    onboarding_filter = st.multiselect(
        "🎯 Стадия онбординга",
        options=['agreement', 'birth_date', 'gender', 'goal', 'activity_level', 
                'current_weight', 'target_weight', 'height', 'daily_calories', 'complete'],
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
if onboarding_filter:
    filtered_df = filtered_df[filtered_df['onboarding_stage'].isin(onboarding_filter)]

# Фильтр по активности
current_time = datetime.now()
if activity_filter == "Активные":
    filtered_df = filtered_df[pd.to_datetime(filtered_df['subscription_expiry']) > current_time]
elif activity_filter == "Неактивные":
    filtered_df = filtered_df[pd.to_datetime(filtered_df['subscription_expiry']) <= current_time]

# Линейный график по дате
st.subheader("📈 Динамика пользователей по времени")

if 'agreement_accepted' in filtered_df.columns and not filtered_df['agreement_accepted'].isna().all():
    time_df = filtered_df.copy()
    time_df = time_df.dropna(subset=['agreement_accepted'])
    
    if not time_df.empty:
        # Группировка по времени
        if time_unit == "Дни":
            time_df['time_group'] = time_df['agreement_accepted'].dt.date
        elif time_unit == "Недели":
            time_df['time_group'] = time_df['agreement_accepted'].dt.to_period('W').dt.start_time
        else:  # Месяцы
            time_df['time_group'] = time_df['agreement_accepted'].dt.to_period('M').dt.start_time
        
        # Подсчет пользователей
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
    else:
        st.warning("Нет данных с датами agreement_accepted")
else:
    st.warning("Отсутствует колонка agreement_accepted или нет данных")

# Воронка онбординга
st.subheader("🔄 Воронка онбординга")

onboarding_stages = [
    'agreement', 'birth_date', 'gender', 'goal', 'activity_level',
    'current_weight', 'target_weight', 'height', 'daily_calories', 'complete'
]

funnel_data = []
for stage in onboarding_stages:
    count = len(df[df['onboarding_stage'] == stage])
    funnel_data.append({'stage': stage, 'count': count})

funnel_df = pd.DataFrame(funnel_data)

if not funnel_df.empty:
    fig_funnel = px.funnel(
        funnel_df,
        x='count',
        y='stage',
        title="Воронка онбординга по стадиям",
        labels={'count': 'Количество пользователей', 'stage': 'Стадия онбординга'}
    )
    st.plotly_chart(fig_funnel, use_container_width=True)
else:
    st.warning("Нет данных для построения воронки")

# Детальная статистика
st.subheader("📋 Детальная статистика")

# Статистика по стадиям
st.write("**Статистика по стадиям онбординга:**")
stage_stats = df['onboarding_stage'].value_counts().reset_index()
stage_stats.columns = ['Стадия', 'Количество']
st.dataframe(stage_stats, use_container_width=True)

# Активность пользователей
st.write("**Статистика по активности:**")
active_users = len(df[pd.to_datetime(df['subscription_expiry']) > current_time])
inactive_users = len(df) - active_users

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
st.sidebar.write(f"Columns: {list(df.columns)}")
st.sidebar.write(f"Date range: {df['agreement_accepted'].min()} to {df['agreement_accepted'].max() if 'agreement_accepted' in df.columns else 'N/A'}")

# Отладка первых нескольких записей
if st.sidebar.checkbox("Show sample data"):
    st.sidebar.write("Sample data:")
    st.sidebar.dataframe(df.head(3))

st.sidebar.success("✅ Dashboard loaded successfully!")
