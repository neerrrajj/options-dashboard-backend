import uuid
import pandas as pd

df = pd.read_csv("oc_minute_snapshots_rows-nf.csv")
print("Total rows:", len(df))
print("Unique ids:", df["id"].nunique())

# Find duplicate IDs if any
dupes = df[df.duplicated("id")]
print("Duplicate rows with same ID:", len(dupes))

# Regenerate unique UUIDs
df["id"] = [uuid.uuid4() for _ in range(len(df))]

# Save to a new file (to preserve the original)
df.to_csv("oc_minute_snapshots-nf_updated.csv", index=False)

print("Total rows:", len(df))
print("Unique ids:", df["id"].nunique())

# Check for duplicates on the UNIQUE index columns
# dupes = df[df.duplicated(subset=["timestamp", "instrument", "expiry", "strike"], keep=False)]

# print(f"Total duplicate rows: {len(dupes)}")