import pandas as pd
import os
import datetime
import logging

logger = logging.getLogger(__name__)

def export_to_excel(data, headers, file_name_prefix="report", output_dir="."):
    """
    Exports a list of dictionaries (or similar iterable) to an Excel file.

    Args:
        data (list of dict or list of tuple): The data to export.
                                              If list of tuples, headers must match order.
        headers (list of str): List of column headers for the Excel file.
        file_name_prefix (str): Prefix for the generated Excel file name.
        output_dir (str): Directory where the Excel file will be saved temporarily.

    Returns:
        str or None: The full path to the generated Excel file, or None if an error occurs.
    """
    if not data:
        logger.warning("No data provided for Excel export.")
        return None

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Create a DataFrame
    try:
        if isinstance(data[0], dict):
            df = pd.DataFrame(data)
            # Reorder columns to match headers and drop any extra columns
            df = df[headers]
        else: # Assume list of tuples/lists
            df = pd.DataFrame(data, columns=headers)
    except Exception as e:
        logger.error(f"Error creating DataFrame for Excel export: {e}")
        return None

    timestamp_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(output_dir, f"{file_name_prefix}_{timestamp_str}.xlsx")

    try:
        df.to_excel(file_path, index=False)
        logger.info(f"Excel file successfully generated at: {file_path}")
        return file_path
    except Exception as e:
        logger.error(f"Error exporting data to Excel: {e}")
        return None

# Example Usage (for testing excel_exporter.py individually)
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Example data (list of dictionaries)
    sample_data_dict = [
        {"SKU": "APPLE001", "Name": "Red Apple", "Quantity": 100, "Location": "A1"},
        {"SKU": "BANANA001", "Name": "Yellow Banana", "Quantity": 150, "Location": "B2"},
        {"SKU": "ORANGE001", "Name": "Orange", "Quantity": 75, "Location": "C3"}
    ]
    sample_headers_dict = ["SKU", "Name", "Quantity", "Location"]

    # Example data (list of tuples)
    sample_data_tuple = [
        ("SKU004", "Grapes", 200, "D4"),
        ("SKU005", "Mango", 50, "E5")
    ]
    sample_headers_tuple = ["Product SKU", "Product Name", "Current Quantity", "Storage Location"]


    output_folder = os.path.join(os.path.dirname(__file__), '..', 'temp_excel_output')
    
    # Test with dictionary data
    excel_file_path_dict = export_to_excel(sample_data_dict, sample_headers_dict, "stock_report", output_folder)
    if excel_file_path_dict:
        logger.info(f"Generated dict example: {excel_file_path_dict}")
    else:
        logger.error("Failed to generate dict example Excel.")

    # Test with tuple data
    excel_file_path_tuple = export_to_excel(sample_data_tuple, sample_headers_tuple, "fruit_stock", output_folder)
    if excel_file_path_tuple:
        logger.info(f"Generated tuple example: {excel_file_path_tuple}")
    else:
        logger.error("Failed to generate tuple example Excel.")
    
    # Test with no data
    excel_file_path_empty = export_to_excel([], ["Col1", "Col2"], "empty_report", output_folder)
    if excel_file_path_empty is None:
        logger.info("Correctly handled empty data for Excel export.")