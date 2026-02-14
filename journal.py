from storage import save_entry

def log_initial_gaea():
    save_entry(
        project="GAEA",
        note=(
            "Initial observation. Interest focused on incentive mechanics and "
            "long-term behavioral alignment rather than short-term rewards. "
            "Monitoring how the system adapts incentives over time."
        ),
        signal="YELLOW"
    )

if __name__ == "__main__":
    log_initial_gaea()
