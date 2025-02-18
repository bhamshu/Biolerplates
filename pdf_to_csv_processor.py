import os
import csv
from typing import Dict, List, Optional
from datetime import datetime
import google.generativeai as genai
from pathlib import Path
import PyPDF2
import json
import time
import fcntl
import tempfile
import shutil

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
    def extract_text(pdf_path: str, pages: Optional[List[int]] = None) -> str:
        """
        Extract text from specified pages of PDF. If pages is None, extracts all pages.
        """
        try:
            with open(pdf_path, 'rb') as file:
                reader = PyPDF2.PdfReader(file)
                text = ""
                
                # If pages not specified, process all pages
                pages_to_process = pages if pages is not None else range(len(reader.pages))
                
                for page_num in pages_to_process:
                    if page_num < len(reader.pages):
                        text += reader.pages[page_num].extract_text() + "\n"
                return text
        except Exception as e:
            raise Exception(f"Error extracting text from PDF: {str(e)}")

class GeminiProcessor:
    def __init__(self, model):
        self.model = model
        # Load schema once during initialization
        self.table_schemas = self._load_table_schemas()

    def _load_table_schemas(self) -> Dict[str, List[str]]:
        """Load all table names and their columns from schema file."""
        schemas = {}
        current_table = None
        columns = []
        
        with open('simplified_schema.ddl', 'r') as f:
            for line in f:
                line = line.strip()
                if line.startswith('CREATE TABLE'):
                    current_table = line.split()[2].strip('(').strip()
                    columns = []
                elif line.startswith(')') and current_table:
                    schemas[current_table] = columns
                    current_table = None
                elif current_table and line and not line.startswith(('--', 'PRIMARY', 'CONSTRAINT')):
                    col_name = line.split()[0].strip(',')
                    if col_name:
                        columns.append(col_name)
        
        return schemas

    def clean_numeric_values(self, data: Dict) -> Dict:
        """Remove commas from numeric values and clean the data."""
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, str):
                    # Remove commas and convert to float if it's a numeric string
                    if value.replace(',', '').replace('-', '').replace('.', '').isdigit():
                        # Convert string numbers with commas to float
                        data[key] = float(value.replace(',', ''))
                    elif value.strip() == '-' or value.strip() == '':
                        # Handle empty or dash values
                        data[key] = None
                elif isinstance(value, (dict, list)):
                    self.clean_numeric_values(value)
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, (dict, list)):
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
    
    def parse_schema_to_json(self, schema_file: str, table_name: str) -> dict:
        """Convert DDL schema to JSON schema for structured output."""
        # Use string type definitions instead of tuples/lists
        type_mapping = {
            'INT': {'type': 'number'},
            'DECIMAL': {'type': 'number'},
            'VARCHAR': {'type': 'string'},
            'TEXT': {'type': 'string'}
        }
        
        # Define schema for structured output using plain dictionaries
        schema = {
            'type': 'object',
            'properties': {
                'rows': {
                    'type': 'array',
                    'items': {
                        'type': 'object',
                        'properties': {},
                        'required': ['data_source']
                    }
                }
            }
        }
        
        # Parse DDL file to build schema
        current_table = None
        required_fields = set(['data_source'])  # Use set for required fields
        
        with open(schema_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line.startswith('CREATE TABLE'):
                    current_table = line.split()[2].strip('(').strip()
                elif current_table == table_name and line and not line.startswith(('--', 'PRIMARY', 'CONSTRAINT', ')')):
                    parts = line.split()
                    if len(parts) >= 2:
                        col_name = parts[0].strip(',')
                        col_type = parts[1].split('(')[0].upper()
                        
                        # Get type definition from mapping
                        type_def = type_mapping.get(col_type, {'type': 'string'})
                        schema['properties']['rows']['items']['properties'][col_name] = type_def.copy()
                        
                        if 'NOT NULL' in line.upper():
                            required_fields.add(col_name)
                
                elif line.startswith(')') and current_table == table_name:
                    break
        
        # Convert set to list for JSON serialization
        schema['properties']['rows']['items']['required'] = list(required_fields)
        
        return schema

    def get_table_data(self, pdf_text: str, table_name: str, schema_file: str) -> Optional[List[Dict]]:
        """Get data for a specific table using structured outputs."""
        try:
            if table_name not in self.table_schemas:
                print(f"Unknown table: {table_name}")
                return None

            # Generate schema for the specific table
            schema = {
                'type': 'object',
                'properties': {
                    'rows': {
                        'type': 'array',
                        'items': {
                            'type': 'object',
                            'properties': {
                                col: {'type': 'string'} for col in self.table_schemas[table_name]
                            },
                            'required': ['data_source']
                        }
                    }
                }
            }
            
            # Configure generation with structured output and low temperature
            generation_config = {
                "temperature": 0.1,
                "response_schema": schema
            }
            
            prompt = f"""
            Extract ALL the following information from the given text into structured data.
            IMPORTANT: 
            1. Do NOT use commas in numeric values
            2. All numeric values should be plain numbers without formatting
            3. Use null for missing values, not empty strings for numeric fields
            4. Extract data for table: {table_name}
            5. Return data in the exact format specified by the schema
            """
            
            # Get response directly
            response = self.model.generate_content(
                prompt + "\n\nExtract from this text:\n" + pdf_text,
                generation_config=generation_config
            )
            
            if not response or not response.text:
                print(f"No response received for {table_name}")
                return None

            # Parse response text as JSON
            try:
                data = json.loads(response.text)
            except json.JSONDecodeError:
                print(f"Invalid JSON response for {table_name}: {response.text}")
                return None

            # Validate and extract rows
            if isinstance(data, dict) and "rows" in data:
                rows = data["rows"]
                if not isinstance(rows, list):
                    print(f"Rows is not a list for {table_name}")
                    return None
                
                # Clean numeric values for each row
                for row in rows:
                    if not isinstance(row, dict):
                        print(f"Row is not a dictionary for {table_name}")
                        continue
                    self.clean_numeric_values(row)
                return rows
            
            print(f"No rows found in response for {table_name}")
            return None
            
        except Exception as e:
            print(f"Error getting data for {table_name}: {str(e)}")
            return None

    def get_structured_data(self, pdf_text: str) -> Dict:
        """Extract all structured data from PDF text using table schemas."""
        try:
            # Get list of tables from loaded schema
            tables = list(self.table_schemas.keys())
            
            # Extract data for each table
            all_data = {}
            for table_name in tables:
                table_data = self.get_table_data(pdf_text, table_name, 'simplified_schema.ddl')
                if table_data:
                    # For single row tables, use first row
                    if table_name in ['company_info', 'shareholding_pattern', 'price_performance']:
                        all_data[table_name] = table_data[0] if table_data else {}
                    else:
                        all_data[table_name] = table_data

            # Clean and normalize the data
            all_data = self.clean_numeric_values(all_data)
            all_data = self.clean_management_discussion(all_data)
            all_data = self.normalize_field_names(all_data)
            
            return all_data

        except Exception as e:
            print(f"Error processing structured data: {str(e)}")
            return {}

    def get_empty_structure(self) -> Dict:
        """Return empty data structure with all required fields."""
        raise NotImplementedError("Use schema from simplified_schema.ddl instead")

class CSVWriter:
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        # Change schema file path
        self.table_configs = self._load_schema_configs()
        
    def _load_schema_configs(self) -> Dict:
        """Load table configurations from simplified_schema.ddl"""
        configs = {}
        current_table = None
        columns = []
        
        with open('simplified_schema.ddl', 'r') as f:  # Changed from schemas.ddl
            for line in f:
                line = line.strip()
                if line.startswith('CREATE TABLE'):
                    # Extract table name without any trailing characters
                    current_table = line.split()[2].strip('(').strip()
                    columns = []
                elif line.startswith(')') and current_table:
                    filename = f"{current_table}.csv"
                    configs[current_table] = (filename, columns)
                    current_table = None
                elif current_table and line and not line.startswith(('--', 'PRIMARY', 'CONSTRAINT')):
                    # Extract just the column name before any type definition
                    col_name = line.split()[0].strip(',')
                    if col_name:
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

    def _write_csv(self, filepath: Path, headers: List[str], data: List[Dict]):
        """Write data to CSV file with file locking for thread safety."""
        # Create a temporary file
        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, newline='')
        
        try:
            writer = csv.DictWriter(temp_file, fieldnames=headers)
            
            # If target file doesn't exist, write header
            file_exists = filepath.exists()
            if not file_exists:
                writer.writeheader()
            
            # Copy existing content if file exists
            if file_exists:
                with open(filepath, 'r', newline='') as existing_file:
                    shutil.copyfileobj(existing_file, temp_file)
            
            # Write new data
            rows = data if isinstance(data, list) else [data]
            for row in rows:
                row_data = {k: v for k, v in row.items() if k in headers}
                writer.writerow(row_data)
                
            temp_file.close()
            
            # Atomically replace the target file
            shutil.move(temp_file.name, filepath)
            
        except Exception as e:
            # Clean up temp file in case of error
            os.unlink(temp_file.name)
            raise e

class PDFProcessor:
    def __init__(self):
        self.config = ConfigManager()
        self.pdf_extractor = PDFExtractor()
        self.gemini_processor = GeminiProcessor(self.config.model)
        self.csv_writer = CSVWriter(self.config.OUTPUT_DIR)
        
    def process_table(self, pdf_path: str, table_name: str, pages: Optional[List[int]] = None, pdf_text: Optional[str] = None) -> bool:
        """Process a single table from specified pages of a PDF."""
        try:
            # Updated table name mapping to match schema
            table_name_mapping = {
                'outlook_or_management_discussion': 'management_discussion',
                'recommendations_or_targets': 'recommendations',
                'shareholding': 'shareholding_pattern',  # Added mapping
                'price_perf': 'price_performance',      # Added mapping
                'financials': 'financial_results'       # Added mapping
            }
            
            schema_table_name = table_name_mapping.get(table_name, table_name)
            
            # Use provided PDF text or extract if not provided
            if pdf_text is None:
                pdf_text = self.pdf_extractor.extract_text(pdf_path, pages)
                print(f"Extracted {len(pdf_text)} characters from PDF")
            
            filename = Path(pdf_path).name
            
            # Get data for this table
            rows = self.gemini_processor.get_table_data(pdf_text, schema_table_name, 'simplified_schema.ddl')
            
            if rows:
                # Add source file and IDs to each row
                company_id = hash(filename) % 10000
                
                for row in rows:
                    row['data_source'] = filename
                    if 'company_id' in row and not row['company_id']:
                        row['company_id'] = company_id
                    
                    # Add specific IDs for tables that need them
                    id_fields = {
                        'financial_results': 'financial_id',
                        'balance_sheet': 'balance_sheet_id',
                        'cash_flow': 'cash_flow_id',
                        'key_ratios': 'ratio_id',
                        'management_discussion': 'discussion_id',
                        'recommendations': 'recommendation_id'
                    }
                    
                    if schema_table_name in id_fields and id_fields[schema_table_name] in row:
                        row[id_fields[schema_table_name]] = hash(f"{filename}_{schema_table_name}_{rows.index(row)}") % 10000
                
                # Write to CSV
                self.csv_writer.write_data({schema_table_name: rows}, filename)
                return True
            else:
                print(f"No data extracted for {schema_table_name}")
                return False
                
        except Exception as e:
            print(f"Error processing table {table_name} from PDF {pdf_path}: {str(e)}")
            return False

    def process_all_tables(self, pdf_path: str, pages: Optional[List[int]] = None) -> Dict[str, bool]:
        """Process all tables from a PDF."""
        # Get tables from schema
        tables = list(self.gemini_processor.table_schemas.keys())
        
        results = {}
        # Extract text once for all tables
        pdf_text = self.pdf_extractor.extract_text(pdf_path, pages)
        print(f"Extracted {len(pdf_text)} characters from PDF")
        
        for table_name in tables:
            try:
                success = self.process_table(
                    pdf_path=pdf_path,
                    table_name=table_name,
                    pages=pages,
                    pdf_text=pdf_text
                )
                results[table_name] = success
            except Exception as e:
                print(f"Error processing {table_name}: {str(e)}")
                results[table_name] = False
                
        return results

def main():
    processor = PDFProcessor()
    pdf_path = "/home/orwell/Desktop/gigaml/FRD Latest Report Updates/SP20241006120459650BATA.pdf"
    
    results = processor.process_all_tables(pdf_path)
    
    for table, success in results.items():
        print(f"{table}: {'Success' if success else 'Failed'}")

if __name__ == "__main__":
    main() 