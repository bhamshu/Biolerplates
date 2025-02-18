import os
import csv
from typing import Dict, List, Optional
from datetime import datetime
import google.generativeai as genai
from pathlib import Path
import PyPDF2
import json

class ConfigManager:
    def __init__(self):
        self.GEMINI_GIGA = os.getenv('GEMINI_GIGA')
        if not self.GEMINI_GIGA:
            raise ValueError("GEMINI_GIGA environment variable not set")
        
        self.OUTPUT_DIR = Path("output_csvs")
        self.OUTPUT_DIR.mkdir(exist_ok=True)
        
        # Initialize Gemini
        genai.configure(api_key=self.GEMINI_GIGA)
        self.model = genai.GenerativeModel('gemini-pro')

class PDFExtractor:
    @staticmethod
    def extract_text(pdf_path: str) -> str:
        try:
            with open(pdf_path, 'rb') as file:
                reader = PyPDF2.PdfReader(file)
                text = ""
                for page in reader.pages:
                    text += page.extract_text() + "\n"
                return text
        except Exception as e:
            raise Exception(f"Error extracting text from PDF: {str(e)}")

class GeminiProcessor:
    def __init__(self, model):
        self.model = model
        
    def clean_numeric_values(self, data: Dict) -> Dict:
        """Remove commas from numeric values and clean the data."""
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, str) and value.replace(',', '').replace('-', '').replace('.', '').isdigit():
                    # Convert string numbers with commas to float/int
                    data[key] = float(value.replace(',', ''))
                elif isinstance(value, (dict, list)):
                    self.clean_numeric_values(value)
        elif isinstance(data, list):
            for item in data:
                self.clean_numeric_values(item)
        return data

    def clean_management_discussion(self, data: Dict) -> Dict:
        """Clean management discussion section to have single topic and discussion_text."""
        if 'management_discussion' in data:
            md = data['management_discussion']
            if 'discussion_text' in md and isinstance(md['discussion_text'], str):
                # Already clean
                return data
                
            # Find the longest discussion_text if multiple exist
            longest_text = ""
            if 'discussion_text' in md:
                if isinstance(md['discussion_text'], list):
                    longest_text = max(md['discussion_text'], key=len)
                else:
                    longest_text = str(md['discussion_text'])
            
            # Clean up the section
            data['management_discussion'] = {
                'discussion_id': md.get('discussion_id'),
                'company_id': md.get('company_id'),
                'fiscal_period': md.get('fiscal_period'),
                'topic': md.get('topic', '').split('\n')[0] if md.get('topic') else '',  # Get first line only
                'discussion_text': longest_text,
                'data_source': md.get('data_source', '')
            }
        return data

    def normalize_field_names(self, data: Dict) -> Dict:
        """Normalize field names to match schema expectations."""
        field_mappings = {
            'mfi_instl_holding_pct': 'mf_insti_holding_pct',
            'mf_holding_pct': 'mf_insti_holding_pct',
            'institutional_holding_pct': 'mf_insti_holding_pct',
            # Add more mappings if needed
        }
        
        # Normalize fields in all sections
        for section in data:
            if isinstance(data[section], dict):
                for old_key, new_key in field_mappings.items():
                    if old_key in data[section]:
                        data[section][new_key] = data[section].pop(old_key)
        
        return data
    
    def get_table_data(self, pdf_text: str, table_name: str, schema: str) -> Dict:
        """Get data for a specific table using targeted prompts."""
        
        # Create a focused prompt for each table
        prompts = {
            'company_info': """
                Extract ONLY company information from the text in this exact JSON format:
                {
                    "company_id": null,
                    "company_name": "",
                    "bse_code": "",
                    "nse_code": "",
                    "bloomberg_code": "",
                    "sector": "",
                    "market_cap_cr": null,
                    "enterprise_value_cr": null,
                    "outstanding_shares_cr": null,
                    "beta": null,
                    "face_value_rs": null,
                    "year_high_price_rs": null,
                    "year_low_price_rs": null,
                    "data_source": ""
                }
                IMPORTANT: Use null for missing numeric values, not empty strings. Remove commas from numbers.
            """,
            'shareholding_pattern': """
                Extract ONLY shareholding pattern data from the text in this exact JSON format:
                {
                    "company_id": null,
                    "quarter": "",
                    "promoter_holding_pct": null,
                    "fii_holding_pct": null,
                    "mf_insti_holding_pct": null,
                    "public_holding_pct": null,
                    "others_holding_pct": null,
                    "data_source": ""
                }
                IMPORTANT: Use null for missing numeric values. Remove commas from numbers.
            """,
            # ... similar prompts for other tables ...
        }
        
        try:
            prompt = prompts.get(table_name, "")
            if not prompt:
                raise ValueError(f"No prompt defined for table {table_name}")
                
            response = self.model.generate_content(prompt + "\n\nExtract from this text:\n" + pdf_text)
            print(f"Raw Gemini response for {table_name}:", response.text)
            
            try:
                data = json.loads(response.text)
                return self.clean_numeric_values(data)
            except json.JSONDecodeError:
                print(f"Failed to parse JSON for {table_name}")
                return None
                
        except Exception as e:
            print(f"Error getting data for {table_name}: {str(e)}")
            return None

    def get_structured_data(self, pdf_text: str) -> Dict:
        prompt = """
        Extract ALL the following information from the given text and return it in this EXACT JSON format.
        IMPORTANT: 
        1. Do NOT use commas in numeric values
        2. Return only ONE discussion_text in management_discussion
        3. All numeric values should be plain numbers without formatting
        4. Use null for missing values, not empty strings for numeric fields
        
        {
            "company_info": {
                "company_id": null,
                "company_name": "",
                "bse_code": "",
                "nse_code": "",
                "bloomberg_code": "",
                "sector": "",
                "market_cap_cr": null,
                "enterprise_value_cr": null,
                "outstanding_shares_cr": null,
                "beta": null,
                "face_value_rs": null,
                "year_high_price_rs": null,
                "year_low_price_rs": null,
                "data_source": ""
            },
            "shareholding": {
                "company_id": null,
                "quarter": "",
                "promoter_holding_pct": null,
                "fii_holding_pct": null,
                "mf_insti_holding_pct": null,
                "public_holding_pct": null,
                "others_holding_pct": null,
                "data_source": ""
            },
            "price_performance": {
                "company_id": null,
                "period": "",
                "absolute_return_3m_pct": null,
                "absolute_return_6m_pct": null,
                "absolute_return_1y_pct": null,
                "sensex_return_3m_pct": null,
                "sensex_return_6m_pct": null,
                "sensex_return_1y_pct": null,
                "relative_return_3m_pct": null,
                "relative_return_6m_pct": null,
                "relative_return_1y_pct": null,
                "data_source": ""
            },
            "financial_results": {
                "financial_id": null,
                "company_id": null,
                "fiscal_period": "",
                "revenue_cr": null,
                "yoy_growth_revenue_pct": null,
                "ebitda_cr": null,
                "ebitda_margin_pct": null,
                "net_profit_cr": null,
                "net_profit_margin_pct": null,
                "eps_rs": null,
                "data_source": ""
            },
            "balance_sheet": {
                "balance_sheet_id": null,
                "company_id": null,
                "fiscal_period": "",
                "total_assets_cr": null,
                "total_liabilities_cr": null,
                "current_assets_cr": null,
                "cash_cr": null,
                "inventories_cr": null,
                "accounts_receivable_cr": null,
                "accounts_payable_cr": null,
                "long_term_debt_cr": null,
                "shareholder_equity_cr": null,
                "data_source": ""
            },
            "cash_flow": {
                "cash_flow_id": null,
                "company_id": null,
                "fiscal_period": "",
                "net_cash_from_operations_cr": null,
                "net_cash_from_investing_cr": null,
                "net_cash_from_financing_cr": null,
                "capex_cr": null,
                "free_cash_flow_cr": null,
                "data_source": ""
            },
            "key_ratios": {
                "ratio_id": null,
                "company_id": null,
                "fiscal_period": "",
                "pe_x": null,
                "pb_x": null,
                "ev_ebitda_x": null,
                "roe_pct": null,
                "roce_pct": null,
                "dividend_yield_pct": null,
                "data_source": ""
            },
            "management_discussion": {
                "discussion_id": null,
                "company_id": null,
                "fiscal_period": "",
                "topic": "",
                "discussion_text": "",
                "data_source": ""
            },
            "recommendations": {
                "recommendation_id": null,
                "company_id": null,
                "rating": "",
                "target_price_rs": null,
                "time_horizon_months": null,
                "data_source": ""
            }
        }
        """
        
        try:
            response = self.model.generate_content(prompt + "\n\nExtract data from this text:\n" + pdf_text)
            print("Raw Gemini response:", response.text)
            
            try:
                # First try direct JSON parsing
                data = json.loads(response.text)
            except json.JSONDecodeError:
                # If that fails, try to clean the response and parse again
                cleaned_response = response.text.replace('\n                "discussion_text":', '"temp":')
                try:
                    data = json.loads(cleaned_response)
                except json.JSONDecodeError:
                    # If still fails, try to find JSON-like structure
                    import re
                    json_match = re.search(r'\{.*\}', response.text, re.DOTALL)
                    if json_match:
                        data = json.loads(json_match.group())
                    else:
                        raise Exception("No JSON structure found in response")
            
            # Clean and normalize the data
            data = self.clean_numeric_values(data)
            data = self.clean_management_discussion(data)
            data = self.normalize_field_names(data)
            
            return data
        except Exception as e:
            print(f"Error processing response: {str(e)}")
            print(f"Full response text: {response.text}")
            
            # Return empty structure if parsing fails
            return self.get_empty_structure()

    def get_empty_structure(self) -> Dict:
        """Return empty data structure with all required fields."""
        return {
            "company_info": {
                "company_id": None,
                "company_name": "",
                "bse_code": "",
                "nse_code": "",
                "bloomberg_code": "",
                "sector": "",
                "market_cap_cr": None,
                "enterprise_value_cr": None,
                "outstanding_shares_cr": None,
                "beta": None,
                "face_value_rs": None,
                "year_high_price_rs": None,
                "year_low_price_rs": None,
                "data_source": ""
            },
            "shareholding": {
                "company_id": None,
                "quarter": "",
                "promoter_holding_pct": None,
                "fii_holding_pct": None,
                "mf_insti_holding_pct": None,
                "public_holding_pct": None,
                "others_holding_pct": None,
                "data_source": ""
            }
        }

class CSVWriter:
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        # Load table configurations from schema
        self.table_configs = self._load_schema_configs()
        
    def _load_schema_configs(self) -> Dict:
        """Load table configurations from schema.ddl"""
        configs = {}
        current_table = None
        columns = []
        
        with open('schemas.ddl', 'r') as f:
            for line in f:
                line = line.strip()
                if line.startswith('CREATE TABLE'):
                    current_table = line.split()[2].strip('(')
                    columns = []
                elif line.startswith(')') and current_table:
                    filename = f"{current_table}.csv"
                    configs[current_table] = (filename, columns)
                    current_table = None
                elif current_table and line and not line.startswith('--'):
                    col_name = line.split()[0].strip(',')
                    if col_name and not col_name.startswith(('PRIMARY', 'CONSTRAINT')):
                        columns.append(col_name)
                        
        return configs

    def write_data(self, data: Dict, source_file: str):
        """Write data to all CSV files."""
        for table_name, (filename, headers) in self.table_configs.items():
            if table_name in data and data[table_name]:
                try:
                    filepath = self.output_dir / filename
                    self._write_csv(filepath, headers, data[table_name])
                except Exception as e:
                    print(f"Error writing {table_name} to CSV: {str(e)}")
                    continue

    def _write_csv(self, filepath: Path, headers: List[str], data: Dict):
        """Write data to CSV file."""
        file_exists = filepath.exists()
        mode = 'a' if file_exists else 'w'
        with open(filepath, mode, newline='') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            if not file_exists:
                writer.writeheader()
            # Filter out any extra fields not in headers
            row_data = {k: v for k, v in data.items() if k in headers}
            writer.writerow(row_data)

class PDFProcessor:
    def __init__(self):
        self.config = ConfigManager()
        self.pdf_extractor = PDFExtractor()
        self.gemini_processor = GeminiProcessor(self.config.model)
        self.csv_writer = CSVWriter(self.config.OUTPUT_DIR)
        
    def process_pdf(self, pdf_path: str):
        try:
            pdf_text = self.pdf_extractor.extract_text(pdf_path)
            print(f"Extracted {len(pdf_text)} characters from PDF")
            
            filename = Path(pdf_path).name
            success = True
            
            # Process each table separately
            for table_name in self.csv_writer.table_configs.keys():
                print(f"Processing {table_name}...")
                
                # Get data for this table
                table_data = self.gemini_processor.get_table_data(pdf_text, table_name, 'schemas.ddl')
                
                if table_data:
                    # Add source file and IDs
                    table_data['data_source'] = filename
                    if 'company_id' in table_data and not table_data['company_id']:
                        table_data['company_id'] = hash(filename) % 10000
                        
                    # Add specific IDs for tables that need them
                    id_fields = {
                        'financial_results': 'financial_id',
                        'balance_sheet': 'balance_sheet_id',
                        'cash_flow': 'cash_flow_id',
                        'key_ratios': 'ratio_id',
                        'outlook_or_management_discussion': 'discussion_id',
                        'recommendations_or_targets': 'recommendation_id'
                    }
                    
                    if table_name in id_fields and id_fields[table_name] in table_data:
                        table_data[id_fields[table_name]] = hash(f"{filename}_{table_name}") % 10000
                    
                    # Write to CSV
                    self.csv_writer.write_data({table_name: table_data}, filename)
                else:
                    print(f"No data extracted for {table_name}")
                    success = False
                    
            return success
            
        except Exception as e:
            print(f"Error processing PDF {pdf_path}: {str(e)}")
            return False

def main():
    processor = PDFProcessor()
    
    # Process single PDF
    pdf_path = "/home/orwell/Desktop/gigaml/FRD Latest Report Updates/SP20241006120459650BATA.pdf"
    success = processor.process_pdf(pdf_path)
    
    if success:
        print(f"Successfully processed {pdf_path}")
    else:
        print(f"Failed to process {pdf_path}")

if __name__ == "__main__":
    main() 