import os
import psycopg

# Paste your live Neon connection string here or read it from environment
DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "postgresql://neondb_owner:npg_2x7qYKoXPwrh@ep-polished-unit-b511m9w5-pooler.c-7.us-east-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
)

def inject_test_task():
    print("🔌 Connecting to live Neon database...")
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            print("🚀 Injecting live test task into queue...")
            cur.execute("""
                INSERT INTO tasks (
                    task_id, 
                    source_platform, 
                    category, 
                    payload, 
                    payout_usd, 
                    est_seconds, 
                    status
                )
                VALUES (%s, %s, %s, %s, %s, %s, 'pending')
                ON CONFLICT (task_id) 
                DO UPDATE SET status = 'pending', payout_usd = EXCLUDED.payout_usd;
            """, (
                "live_render_test_001", 
                "RenderLivePlatform", 
                "Cloud_Verification", 
                '{"action": "verify_live_pipeline", "target": "render"}', 
                10.00,  # $10.00 payout
                20      # 20 seconds est.
            ))
            conn.commit()
            print("✅ Test task inserted successfully with status 'pending'!")
            print("📊 Native Return-on-Compute (RoC) score calculated: $10.00 / 20s = 0.5000 $/sec")

if __name__ == "__main__":
    inject_test_task()
