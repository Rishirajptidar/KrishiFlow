# Databricks notebook source
# MAGIC %md
# MAGIC Prepare remaining source files

# COMMAND ----------

# inspect the source files
from datetime import datetime

base_path = "/Volumes/krishiflow/source/source_data/"

def inspect_folder(path, depth=0, max_depth=2):
    try:
        items = dbutils.fs.ls(path)

        for item in items:
            indent = "  " * depth

            if item.isDir():
                print(f"{indent}[FOLDER] {item.path}")

                if depth < max_depth:
                    inspect_folder(
                        item.path,
                        depth + 1,
                        max_depth
                    )
            else:
                print(
                    f"{indent}[FILE] {item.name} "
                    f"| Size: {item.size:,} bytes"
                )

    except Exception as e:
        print(f"Could not inspect {path}: {e}")


inspect_folder(base_path)