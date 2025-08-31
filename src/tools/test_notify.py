from dotenv import load_dotenv
load_dotenv()

from src.etl.utils.notify import notify

if __name__ == "__main__":
    notify("Test message from Woo ETL — if you see this, SMTP works ✅", level="success")
    print("Sent. Check your inbox.")
