import os
import sys
from datetime import datetime
from pathlib import Path

# Import the existing PDF generator
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from hpcl_pdf_generator_final import HPCLDynamicPDFGenerator

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
            
            return result_path
            
        except Exception as e:
            raise Exception(f"PDF generation failed: {str(e)}")