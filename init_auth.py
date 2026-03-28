import os
import urllib.parse
try:
    import bcrypt
except ImportError:
    print("❌ Error: Falta la librería de encriptado. Instálala ejecutando: pip install bcrypt")
    exit(1)
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def initialize_auth_tables():
    load_dotenv()
    
    DB_HOST = os.getenv('DB_HOST')
    DB_NAME = os.getenv('DB_NAME')
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    
    if not all([DB_HOST, DB_NAME, DB_USER, DB_PASSWORD]):
        print("❌ Error: Faltan credenciales en el archivo .env")
        return

    encoded_pwd = urllib.parse.quote_plus(DB_PASSWORD)
    connection_string = f"postgresql://{DB_USER}:{encoded_pwd}@{DB_HOST}:5432/{DB_NAME}"
    engine = create_engine(connection_string)

    tables_sql = """
    CREATE TABLE IF NOT EXISTS "semaforos_PT".usuarios (
        id SERIAL PRIMARY KEY,
        username VARCHAR(255) UNIQUE NOT NULL,
        password_hash VARCHAR(255) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS "semaforos_PT".log_accesos (
        id SERIAL PRIMARY KEY,
        usuario_id INTEGER REFERENCES "semaforos_PT".usuarios(id) ON DELETE CASCADE,
        ip_address VARCHAR(45) NOT NULL,
        fecha_hora_login TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    default_user = "admin_dashboard"
    default_pass = "seguridad123"
    
    # Bcrypt es el algoritmo definitivo de PHP para Hash Seguro. Python lo replica exactamente.
    print(f"🔒 Generando hash robusto unidireccional (Bcrypt) para '{default_user}'...")
    salt = bcrypt.gensalt(rounds=12) # 12 rondas toma tiempo, lo cual blinda contra ataques de fuerza bruta
    hashed = bcrypt.hashpw(default_pass.encode('utf-8'), salt).decode('utf-8')

    try:
        with engine.begin() as conn:
            print("⚡ Conectado a PostgreSQL. Verificando estructura Auth...")
            conn.execute(text(tables_sql))
            print("✅ Tablas 'usuarios' y 'log_accesos' comprobadas/creadas en el esquema 'semaforos_PT'.")

            # Check if user exists
            result = conn.execute(text('SELECT id FROM "semaforos_PT".usuarios WHERE username = :u'), {"u": default_user})
            
            if result.fetchone() is None:
                conn.execute(
                    text('INSERT INTO "semaforos_PT".usuarios (username, password_hash) VALUES (:u, :p)'),
                    {"u": default_user, "p": hashed}
                )
                print(f"✅ ¡Usuario maestro inyectado exitosamente en Postgres!")
                print(f"   -> Username: {default_user}")
                print(f"   -> Password desencriptada (Anotar): {default_pass}")
                print(f"   -> Hash en BD: {hashed}")
            else:
                print(f"⚠️ El usuario '{default_user}' ya existía. La contraseña maestra está a salvo y no se sobrescribió.")
                
    except Exception as e:
        print(f"❌ Error crítico escribiendo tablas o usuarios: {e}")

if __name__ == "__main__":
    initialize_auth_tables()
