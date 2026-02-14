from storage import save_entry
from datetime import datetime
from filter import filter_event

def run():
    # Заглушка агента (первый живой сигнал)
    save_entry(
        project="GAEA",
        note=(
            "Social pulse scan executed. No live API connected yet. "
            "This is a bootstrap signal confirming agent → Atlas pipeline."
        ),
        signal="YELLOW"
    )
    print("🟡 Agent executed: bootstrap signal logged")

if __name__ == "__main__":
    run()
