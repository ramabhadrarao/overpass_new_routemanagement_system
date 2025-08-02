# services/pdf_generator.py
# PDF Generator Service - Wrapper for the actual PDF generator
# Path: /services/pdf_generator.py

import os
import sys
from datetime import datetime
from pathlib import Path
import logging

# Add the project root to Python path so we can import from root
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Import the main PDF generator
from hpcl_pdf_generator_final import HPCLDynamicPDFGenerator

logger = logging.getLogger(__name__)

class PDFGeneratorService:
    def __init__(self, mongodb_uri, output_folder):
        self.mongodb_uri = mongodb_uri
        self.output_folder = output_folder
        Path(self.output_folder).mkdir(parents=True, exist_ok=True)
        
    def generate_route_pdf(self, route_id: str) -> str:
        """Generate PDF for a route using the existing generator"""
        try:
            # Initialize the generator with MongoDB URI
            generator = HPCLDynamicPDFGenerator(self.mongodb_uri)
            
            # Generate output filename
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_filename = f"route_analysis_{route_id}_{timestamp}.pdf"
            output_path = os.path.join(self.output_folder, output_filename)
            
            # Generate the PDF
            result_path = generator.generate_pdf_report(route_id, output_path)
            
            logger.info(f"PDF generated successfully: {result_path}")
            return result_path
            
        except Exception as e:
            logger.error(f"PDF generation failed: {str(e)}")
            raise Exception(f"PDF generation failed: {str(e)}")