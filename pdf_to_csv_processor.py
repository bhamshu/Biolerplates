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
        self.table_configs = {
            'company_info': ('company_info.csv', ['company_id', 'company_name', 'bse_code', 'nse_code', 'bloomberg_code', 
                           'sector', 'market_cap_cr', 'enterprise_value_cr', 'outstanding_shares_cr',
                           'beta', 'face_value_rs', 'year_high_price_rs', 'year_low_price_rs', 'data_source']),
            'shareholding': ('shareholding_pattern.csv', ['company_id', 'quarter', 'promoter_holding_pct', 'fii_holding_pct',
                           'mf_insti_holding_pct', 'public_holding_pct', 'others_holding_pct', 'data_source']),
            'price_performance': ('price_performance.csv', ['company_id', 'period'] + [f for f in ['absolute_return_3m_pct', 
                                'absolute_return_6m_pct', 'absolute_return_1y_pct', 'sensex_return_3m_pct', 'sensex_return_6m_pct',
                                'sensex_return_1y_pct', 'relative_return_3m_pct', 'relative_return_6m_pct', 'relative_return_1y_pct',
                                'data_source']]),
            'financial_results': ('financial_results.csv', ['financial_id', 'company_id', 'fiscal_period', 'revenue_cr',
                                'yoy_growth_revenue_pct', 'ebitda_cr', 'ebitda_margin_pct', 'net_profit_cr',
                                'net_profit_margin_pct', 'eps_rs', 'data_source']),
            # ... add configurations for other tables
        }
    
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
            # Extract text from PDF
            pdf_text = self.pdf_extractor.extract_text(pdf_path)
            print(f"Extracted {len(pdf_text)} characters from PDF")  # Debug print
            
            # Get structured data from Gemini
            structured_data = self.gemini_processor.get_structured_data(pdf_text)
            
            # Add source file info
            filename = Path(pdf_path).name
            structured_data['company_info']['data_source'] = filename
            structured_data['shareholding']['data_source'] = filename
            
            # Generate company_id from filename if not present
            if not structured_data['company_info']['company_id']:
                # Extract numeric part from filename or generate a random one
                company_id = hash(filename) % 10000  # Simple hash-based ID
                structured_data['company_info']['company_id'] = company_id
                structured_data['shareholding']['company_id'] = company_id
            
            # Generate IDs for tables that need them
            if structured_data['financial_results']:
                structured_data['financial_results']['financial_id'] = hash(f"{filename}_fr") % 10000
            if structured_data['balance_sheet']:
                structured_data['balance_sheet']['balance_sheet_id'] = hash(f"{filename}_bs") % 10000
            # ... generate other IDs ...
            
            # Write all data to CSVs
            self.csv_writer.write_data(structured_data, filename)
            
            return True
        except Exception as e:
            print(f"Error processing PDF {pdf_path}: {str(e)}")
            print(f"Stack trace:", e.__traceback__)
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