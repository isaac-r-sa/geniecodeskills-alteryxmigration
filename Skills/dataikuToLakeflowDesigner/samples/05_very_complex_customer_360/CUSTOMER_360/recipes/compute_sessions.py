# -*- coding: utf-8 -*-
import dataiku
import pandas as pd

gap = int(dataiku.get_custom_variables()["session_gap_minutes"])

ev = dataiku.Dataset("web_events").get_dataframe()
ev["event_ts"] = pd.to_datetime(ev["event_ts"])
ev = ev.sort_values(["customer_id", "event_ts", "event_id"])

# a new session starts when the gap to the previous event of the same customer exceeds `gap` minutes
prev = ev.groupby("customer_id")["event_ts"].shift(1)
ev["new_session"] = prev.isna() | ((ev["event_ts"] - prev).dt.total_seconds() > gap * 60)
ev["session_no"] = ev.groupby("customer_id")["new_session"].cumsum()

sessions = (ev.groupby(["customer_id", "session_no"])
              .agg(session_start=("event_ts", "min"), session_end=("event_ts", "max"),
                   n_events=("event_id", "count"),
                   has_checkout=("event_type", lambda s: int((s == "checkout").any())))
              .reset_index())
sessions["duration_min"] = (sessions["session_end"] - sessions["session_start"]).dt.total_seconds() / 60.0

dataiku.Dataset("sessions").write_with_schema(sessions)
