import streamlit as st
import redis
import plotly.express as px
import pandas as pd
import time
from datetime import datetime
from urllib.parse import urlparse

# Настройка страницы
st.set_page_config(
    page_title="User Analytics Dashboard",
    page_icon="📊",
    layout="wide"
)

# Инициализация Redis
@st.cache_resource
def init_redis():
    try:
        # Парсинг URL из Upstash
        redis_url = st.secrets["REDIS_URL"]
        parsed_url = urlparse(redis_url)
        
        # Извлечение хоста и порта
        host = parsed_url.hostname
        port = parsed_url.port or 6379
        
        # Подключение к Redis
        r = redis.Redis(
            host=host,
            port=port,
            password=st.secrets["REDIS_TOKEN"],
            ssl=True,
            ssl_cert_reqs=None,  # Отключаем проверку SSL сертификата
            decode_responses=True  # Автоматическое декодирование в строки
        )
        
        # Проверка подключения
        r.ping()
        return r
        
    except Exception as e:
        st.error(f"Redis connection error: {str(e)}")
        return None

def safe_hget(key, field):
    """Безопасное получение значения из hash"""
    try:
        return redis_client.hget(key, field)
    except Exception as e:
        st.warning(f"Error reading {field} from {key}: {str(e)}")
        return None

def get_user_stats():
    """Сбор статистики пользователей"""
    total_users = 0
    completed = 0
    stages = {}
    users_data = []
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    try:
        # Используем SCAN для итерации по ключам
        cursor = 0
        processed = 0
        
        # Сначала подсчитаем approximate количество ключей
        approx_keys = redis_client.dbsize()
        if approx_keys == 0:
            return 0, 0, {}, []
        
        st.info(f"🔍 Found approximately {approx_keys} keys in database")
        
        # Итерация по ключам
        while True:
            cursor, keys = redis_client.scan(cursor, match="user:*", count=100)
            
            if not keys:
                if cursor == 0:
                    break
                continue
            
            for key in keys:
                total_users += 1
                processed += 1
                
                # Обновление прогресса
                if approx_keys > 0:
                    progress = min(processed / approx_keys, 1.0)
                    progress_bar.progress(progress)
                    status_text.text(f"👤 Processing {processed} users...")
                
                # Получение данных пользователя
                onboarding_stage = safe_hget(key, "onboarding_stage")
                created_at = safe_hget(key, "created_at") or "unknown"
                email = safe_hget(key, "email") or safe_hget(key, "user_email") or "no-email"
                
                if onboarding_stage:
                    stages[onboarding_stage] = stages.get(onboarding_stage, 0) + 1
                    if onboarding_stage.lower() == "complete":
                        completed += 1
                
                users_data.append({
                    "user_id": key,
                    "onboarding_stage": onboarding_stage or "not_set",
                    "created_at": created_at,
                    "email": email
                })
            
            if cursor == 0:
                break
                
    except Exception as e:
        st.error(f"Error scanning keys: {str(e)}")
    
    progress_bar.empty()
    status_text.empty()
    
    return total_users, completed, stages, users_data

# Заголовок дашборда
st.title("📊 User Analytics Dashboard")
st.caption("Real-time analytics from Upstash Redis • " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

# Инициализация Redis
redis_client = init_redis()

if not redis_client:
    st.error("""
    ❌ Could not connect to Redis. Please check:
    1. REDIS_URL in secrets (e.g., https://global-xxx.upstash.io)
    2. REDIS_TOKEN in secrets
    3. Internet connection
    """)
    st.stop()

# Проверка подключения
try:
    redis_client.ping()
    st.sidebar.success("✅ Connected to Redis successfully!")
except:
    st.sidebar.error("❌ Redis connection failed")
    st.stop()

# Кнопка для обновления данных
col1, col2 = st.columns([1, 3])
with col1:
    if st.button("🔄 Refresh Data", type="primary", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

with col2:
    st.info("💡 Data is cached. Click refresh to update")

# Получение данных
try:
    with st.spinner("Loading user data from Redis..."):
        total, completed, stages, users_data = get_user_stats()
    
    if total == 0:
        st.warning("No user data found in Redis!")
        st.stop()
    
    # Основные метрики
    st.header("📈 Overview")
    col1, col2, col3, col4 = st.columns(4)
    
    col1.metric("Total Users", total)
    col2.metric("Completed", completed)
    
    if total > 0:
        completion_rate = (completed / total) * 100
        col3.metric("Completion Rate", f"{completion_rate:.1f}%")
        col4.metric("In Progress", total - completed)
    else:
        col3.metric("Completion Rate", "0%")
        col4.metric("In Progress", 0)
    
    # Визуализации
    if stages:
        st.header("📊 Distribution")
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Onboarding Stages")
            fig_pie = px.pie(
                values=list(stages.values()),
                names=list(stages.keys()),
                title="Distribution by Stage"
            )
            st.plotly_chart(fig_pie, use_container_width=True)
        
        with col2:
            st.subheader("Stage Counts")
            df_stages = pd.DataFrame({
                'Stage': list(stages.keys()),
                'Count': list(stages.values())
            })
            fig_bar = px.bar(df_stages, x='Stage', y='Count', 
                           title="Users by Stage", color='Stage')
            st.plotly_chart(fig_bar, use_container_width=True)
    
    # Таблица с данными
    st.header("👥 User Details")
    if users_data:
        df = pd.DataFrame(users_data)
        
        # Фильтры
        col1, col2 = st.columns(2)
        with col1:
            selected_stage = st.selectbox(
                "Filter by stage:",
                ["All"] + sorted(df['onboarding_stage'].unique())
            )
        
        with col2:
            search_term = st.text_input("Search by user ID or email:")
        
        # Применение фильтров
        filtered_df = df
        if selected_stage != "All":
            filtered_df = filtered_df[filtered_df['onboarding_stage'] == selected_stage]
        
        if search_term:
            filtered_df = filtered_df[
                filtered_df['user_id'].str.contains(search_term, case=False) |
                filtered_df['email'].str.contains(search_term, case=False)
            ]
        
        st.dataframe(
            filtered_df,
            use_container_width=True,
            hide_index=True,
            height=400
        )
        
        # Скачивание данных
        csv = filtered_df.to_csv(index=False)
        st.download_button(
            label="📥 Download CSV",
            data=csv,
            file_name="users_data.csv",
            mime="text/csv",
            use_container_width=True
        )
        
        st.info(f"Showing {len(filtered_df)} of {len(df)} users")

except Exception as e:
    st.error(f"Error processing data: {str(e)}")
    import traceback
    st.code(traceback.format_exc())

# Sidebar с информацией
with st.sidebar:
    st.header("ℹ️ About")
    st.write("This dashboard connects to your Upstash Redis instance")
    
    st.divider()
    
    if st.button("Clear Cache"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.success("Cache cleared!")
        time.sleep(1)
        st.rerun()
    
    st.divider()
    st.write("**Made with Streamlit + Redis**")