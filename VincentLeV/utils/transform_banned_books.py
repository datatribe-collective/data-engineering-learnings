import os
import pandas as pd
from utils.constants import BASE_DATA_URL

def transform_data(file_path, header_row=None):
  possible_headers = ["Author,Title", "Title,Author"]

  with open(file_path, "r", encoding="utf-8") as file:
    lines = file.readlines()

  header_index = None
  for header_row in possible_headers:
    header_index = next((i for i, line in enumerate(lines) if header_row in line), None)
    if header_index is not None:
      break

  if header_index is None:
    print(f"Header row '{header_row}' not found in {file_path}")
    return

  # Keep only rows starting from the header and remove trailing unrelated rows
  cleaned_lines = lines[header_index:]
  cleaned_lines = [line for line in cleaned_lines if line.strip()]  # Remove empty lines

  # Write the cleaned data back to the file
  with open(file_path, "w", encoding="utf-8") as file:
      file.writelines(cleaned_lines)

  try:
    dataset = pd.read_csv(file_path)

    # Remove rows containing "PEN America" or "PEN AMERICA"
    dataset = dataset[~dataset.apply(lambda row: row.astype(str).str.contains("PEN America|PEN AMERICA", case=False).any(), axis=1)]

    # Remove rows without both Title and Author
    if "Title" in dataset.columns and "Author" in dataset.columns:
      print(f"Removing rows without both Title and Author in {file_path}")
      dataset = dataset.dropna(subset=["Title", "Author"], how="all")

    # Reformat the "Author" column to "Firstname Lastname" format
    if "Author" in dataset.columns:
      print(f"Reformatting Author column in {file_path}")
      dataset["Author"] = dataset["Author"].apply(
        lambda x: " ".join(x.split(", ")[::-1]) if isinstance(x, str) and ", " in x else x
      )
    if "Type of Ban" in dataset.columns:
      print(f"Renaming Type of Ban to Ban Status in {file_path}")
      dataset.rename(columns={"Type of Ban": "Ban Status"}, inplace=True)
    if "Initiating Action" in dataset.columns:
      print(f"Renaming Initiating Action to Origin of Challenge in {file_path}")
      dataset.rename(columns={"Initiating Action": "Origin of Challenge"}, inplace=True)
    if "Origin of Challenge" in dataset.columns:
      print(f"Replacing values in Origin of Challenge column in {file_path}")
      dataset["Origin of Challenge"] = dataset["Origin of Challenge"].replace({
        "Administrator": "Administration",
        "Formal Challenge, Administration": "Administration/Formal Challenge",
        "Informal Challenge, Administration": "Administration/Informal Challenge"
      })
    if "Date of Challenge/Removal" in dataset.columns:
      print(f"Extracting year from Date of Challenge/Removal column in {file_path}")
      dataset["Year"] = dataset["Date of Challenge/Removal"].apply(
          lambda x: str(x.split()[-1]) if pd.notnull(x) and isinstance(x, str) else None
      )
  
    dataset.to_csv(file_path, index=False)
  except Exception as e:
    print(f"Error processing column in {file_path}: {e}")
    raise

def transform_merged_data(dataset, exclude_columns=None):
  if exclude_columns is None:
    exclude_columns = []

  # Identify base columns by removing suffixes (_x, _y, etc.)
  base_columns = set(col.split("_")[0] for col in dataset.columns if "_" in col or col in dataset.columns)

  for base_col in base_columns:
    if base_col not in exclude_columns:
      # Find all variations of the column (e.g., column, column_x, column_y)
      variations = [col for col in dataset.columns if col == base_col or col.startswith(f"{base_col}_")]

      if len(variations) > 1:
        # Merge all variations into the base column
        merged_column = dataset[variations].bfill(axis=1).iloc[:, 0]
        dataset[base_col] = merged_column.infer_objects(copy=False)

        # Drop all variations except the base column
        dataset.drop(columns=[col for col in variations if col != base_col], inplace=True)

  if "Ban Status" in dataset.columns:
    print("Normalizing Ban Status column")
    dataset["Ban Status"] = dataset["Ban Status"].fillna("").str.lower().str.strip()
    dataset["Ban Status"] = dataset["Ban Status"].apply(
      lambda x: "banned from libraries and classrooms"
      if "classrooms" in x and "libraries" in x else
      "banned from libraries and classrooms"
      if "classrooms" in x or "libraries" in x else x
    )
  if "Year" in dataset.columns:
    print("Normalizing Year column")
    dataset["Year"] = dataset["Year"].apply(lambda x: 
      str(int(x.split("-")[1])) if isinstance(x, str) and "-" in x else
      str(int(x)) if isinstance(x, (int, float)) else
      x.replace(".0", "") if isinstance(x, str) and ".0" in x else
      str(x) if pd.notnull(x) else None
    )

  return dataset

def merge_data():
  try:
    merged_file_path = os.path.join(BASE_DATA_URL, "banned_books.csv")

    dataset_2021_2022_path = os.path.join(BASE_DATA_URL, "pen-2021-2022.csv")
    dataset_2023_path = os.path.join(BASE_DATA_URL, "pen-2023.csv")
    dataset_2023_2024_path = os.path.join(BASE_DATA_URL, "pen-2023-2024.csv")

    dataset_2021_2022 = pd.read_csv(dataset_2021_2022_path)
    dataset_2023 = pd.read_csv(dataset_2023_path)
    dataset_2023_2024 = pd.read_csv(dataset_2023_2024_path)

    # Merge datasets
    merged_dataset = dataset_2021_2022.merge(
      dataset_2023, on=["Title", "Author"], how="outer"
    ).merge(
      dataset_2023_2024, on=["Title", "Author"], how="outer"
    )

    merged_dataset = transform_merged_data(merged_dataset, exclude_columns=["Title", "Author"])

    merged_dataset.to_csv(merged_file_path, index=False)
    print(f"Merged dataset saved to {merged_file_path}")
  except Exception as e:
    print(f"Error in merge_data: {e}")
    raise