def run_daily():
    state = load_state()
    window = compute_window(state)
    if not window:
        return

    for chunk in chunk_window(window):
        data = fetch(chunk)
        validate(data)
        write(data)

    save_state(window.end)