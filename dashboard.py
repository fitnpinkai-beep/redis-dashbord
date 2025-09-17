import streamlit as st
import redis
import plotly.express as px
import pandas as pd
from datetime import datetime
import time
import re

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
st.title("📊 Аналитика какиш")
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
    possible_date_columns = ['agreement_accepted', 'agreement', 'created_at', 'date', 'timestamp', 'registered_at', 'start_date']
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

# Загрузка данных
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
possible_date_columns = ['agreement', 'agreement', 'created_at', 'date', 'timestamp', 'registered_at', 'start_date']
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
        filtered_df = filtered_df[filtered_df['subscription_expiry'] > current_time]
elif activity_filter == "Неактивные":
    if 'subscription_expiry' in filtered_df.columns:
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
    
    # Создадим демо-график для примера
    st.info("📈 Демо-график (для примера):")
    demo_dates = pd.date_range(start='2024-01-01', end=datetime.now(), freq='D')
    demo_data = pd.DataFrame({
        'time_group': demo_dates,
        'user_count': [i * 10 for i in range(len(demo_dates))]
    })
    fig_demo = px.line(demo_data, x='time_group', y='user_count', 
                      title="Демо: Динамика пользователей (пример)")
    st.plotly_chart(fig_demo, use_container_width=True)

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
        active_users = len(df[df['subscription_expiry'] > datetime.now()])
        inactive_users = len(df) - active_users
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

st.sidebar.success("✅ Dashboard loaded successfully!")


