import yaml
import os

# === CONFIGURATION ===
CONFIG_FILE = "config.yaml"
INPUT_FOLDER = "Inputs"
OUTPUT_FILE = "data.txt"

def parse_csv_exact(file_path):
    """Reads a CSV file without mangling duplicate headers."""
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    header = lines[0].strip().split(',')
    data = [line.strip().split(',') for line in lines[1:] if line.strip()]
    return header, data

# === LOAD CONFIG FILE ===
with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

# === OPEN OUTPUT FILE ===
with open(OUTPUT_FILE, 'w', encoding='utf-8') as out:
    out.write("# OSeMOSYS data file generated without otoole\n\n")

    for name, definition in config.items():
        file_path = os.path.join(INPUT_FOLDER, f"{name}.csv")

        if not os.path.exists(file_path):
            print(f"⚠️ Skipping missing file: {file_path}")
            continue

        header, rows = parse_csv_exact(file_path)

        # === SETS ===
        if isinstance(definition, dict) and definition.get("type") == "set":
            # Expecting a single-column CSV
            elements = [row[0] for row in rows if row]
            elements_str = " ".join(sorted(set(elements)))
            out.write(f"set {name} := {elements_str} ;\n\n")

        # === PARAMETERS ===
        elif isinstance(definition, dict) and "indices" in definition:
            indices = definition["indices"]

            if len(header) != len(indices) + 1:
                print(f"⚠️ Header mismatch for param {name}")
                continue

            default_str = ""
            if "default" in definition:
                default_str = f" default {definition['default']}"

            out.write(f"param {name}{default_str} :=\n")
            for row in rows:
                if len(row) < len(indices) + 1:
                    continue
                keys = " ".join(row[:len(indices)])
                value = row[len(indices)]
                out.write(f"  {keys} {value}\n")
            out.write(";\n\n")

        else:
            print(f"⚠️ Unhandled config entry: {name} — skipping.")

print(f"✅ Conversion complete — output saved to: {OUTPUT_FILE}")
