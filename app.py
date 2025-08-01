import os
import logging
from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, send_file
from flask_login import LoginManager, login_user, logout_user, login_required, current_user
from werkzeug.utils import secure_filename
from pymongo import MongoClient
from config import Config
from models import User, Route, APILog
from services.route_processor import RouteProcessor
from services.pdf_generator import PDFGeneratorService
from utils.file_parser import FileParser

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)
app.config.from_object(Config)

# Initialize MongoDB
client = MongoClient(app.config['MONGODB_URI'])
db = client.get_database()

# Initialize services
route_processor = RouteProcessor(db, app.config['OVERPASS_API_URL'])
pdf_generator = PDFGeneratorService(app.config['MONGODB_URI'], app.config['PDF_OUTPUT_FOLDER'])
file_parser = FileParser()

# Initialize models
user_model = User(db)
route_model = Route(db)
api_log_model = APILog(db)

# Initialize Flask-Login
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

# Create upload and output folders
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['PDF_OUTPUT_FOLDER'], exist_ok=True)
os.makedirs('route_data', exist_ok=True)

# User loader for Flask-Login
@login_manager.user_loader
def load_user(user_id):
    user = user_model.find_by_id(user_id)
    if user:
        return UserObj(user)
    return None

class UserObj:
    def __init__(self, user_dict):
        self.id = str(user_dict['_id'])
        self.username = user_dict['username']
        self.email = user_dict['email']
        self.role = user_dict['role']
        self.is_active = user_dict.get('is_active', True)
        
    def is_authenticated(self):
        return True
        
    def is_anonymous(self):
        return False
        
    def get_id(self):
        return self.id

# Create default admin user on startup
def create_default_admin():
    admin = user_model.find_by_username(app.config['DEFAULT_ADMIN_USERNAME'])
    if not admin:
        user_model.create_user(
            username=app.config['DEFAULT_ADMIN_USERNAME'],
            email='admin@routerisk.com',
            password=app.config['DEFAULT_ADMIN_PASSWORD'],
            role='admin'
        )
        logger.info("Default admin user created")

# Routes
@app.route('/')
def index():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))
    return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        
        user = user_model.find_by_username(username)
        
        if user and user_model.verify_password(user, password):
            user_obj = UserObj(user)
            login_user(user_obj)
            user_model.update_last_login(user['_id'])
            flash('Login successful!', 'success')
            return redirect(url_for('dashboard'))
        else:
            if user:
                user_model.increment_login_attempts(username)
            flash('Invalid username or password', 'danger')
            
    return render_template('login.html')

@app.route('/logout')
@login_required
def logout():
    logout_user()
    flash('You have been logged out.', 'info')
    return redirect(url_for('login'))

@app.route('/dashboard')
@login_required
def dashboard():
    # Get routes with pagination
    page = request.args.get('page', 1, type=int)
    per_page = 20
    
    routes = route_model.get_all_routes(skip=(page-1)*per_page, limit=per_page)
    
    # Get statistics
    total_routes = db.routes.count_documents({})
    processed_routes = db.routes.count_documents({'processing_status': 'completed'})
    pending_routes = db.routes.count_documents({'processing_status': 'pending'})
    failed_routes = db.routes.count_documents({'processing_status': 'failed'})
    
    stats = {
        'total': total_routes,
        'processed': processed_routes,
        'pending': pending_routes,
        'failed': failed_routes
    }
    
    return render_template('dashboard.html', routes=routes, stats=stats, page=page)

@app.route('/upload', methods=['GET', 'POST'])
@login_required
def upload():
    if request.method == 'POST':
        if 'csv_file' not in request.files:
            flash('No file selected', 'danger')
            return redirect(request.url)
            
        file = request.files['csv_file']
        
        if file.filename == '':
            flash('No file selected', 'danger')
            return redirect(request.url)
            
        if file and file.filename.endswith('.csv'):
            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(filepath)
            
            # Process the CSV file
            try:
                results = route_processor.process_csv_file(filepath, 'route_data')
                flash(f'CSV processed: {results["processed"]} routes processed, '
                      f'{results["skipped"]} skipped, {results["failed"]} failed', 'success')
                return redirect(url_for('dashboard'))
            except Exception as e:
                flash(f'Error processing CSV: {str(e)}', 'danger')
                logger.error(f"CSV processing error: {str(e)}")
        else:
            flash('Please upload a valid CSV file', 'danger')
            
    return render_template('upload.html')

@app.route('/route/<route_id>')
@login_required
def route_detail(route_id):
    route = route_model.find_by_id(route_id)
    if not route:
        flash('Route not found', 'danger')
        return redirect(url_for('dashboard'))
        
    # Get analysis data
    sharp_turns = list(db.sharpturns.find({'routeId': route['_id']}))
    blind_spots = list(db.blindspots.find({'routeId': route['_id']}))
    emergency_services = list(db.emergencyservices.find({'routeId': route['_id']}))
    road_conditions = list(db.roadconditions.find({'routeId': route['_id']}))
    network_coverage = list(db.networkcoverages.find({'routeId': route['_id']}))
    eco_zones = list(db.ecosensitivezones.find({'routeId': route['_id']}))
    accident_areas = list(db.accidentproneareas.find({'routeId': route['_id']}))
    
    # Get API logs
    api_logs = api_log_model.get_route_api_logs(route_id)
    
    analysis_data = {
        'sharp_turns': sharp_turns,
        'blind_spots': blind_spots,
        'emergency_services': emergency_services,
        'road_conditions': road_conditions,
        'network_coverage': network_coverage,
        'eco_zones': eco_zones,
        'accident_areas': accident_areas
    }
    
    return render_template('route_detail.html', 
                         route=route, 
                         analysis_data=analysis_data,
                         api_logs=api_logs)

@app.route('/generate_pdf/<route_id>')
@login_required
def generate_pdf(route_id):
    try:
        route = route_model.find_by_id(route_id)
        if not route:
            flash('Route not found', 'danger')
            return redirect(url_for('dashboard'))
            
        # Generate PDF
        pdf_path = pdf_generator.generate_route_pdf(route_id)
        
        # Update route with PDF path
        route_model.mark_pdf_generated(route_id, pdf_path)
        
        # Return the PDF file
        return send_file(pdf_path, as_attachment=True, 
                        download_name=f"route_analysis_{route['route_name']}.pdf")
        
    except Exception as e:
        flash(f'Error generating PDF: {str(e)}', 'danger')
        logger.error(f"PDF generation error: {str(e)}")
        return redirect(url_for('route_detail', route_id=route_id))

@app.route('/api/process_route/<route_id>', methods=['POST'])
@login_required
def process_route_api(route_id):
    """API endpoint to process a single route"""
    try:
        route = route_model.find_by_id(route_id)
        if not route:
            return jsonify({'error': 'Route not found'}), 404
            
        # Process the route
        route_processor.reprocess_route(route_id)
        
        return jsonify({'success': True, 'message': 'Route processing started'})
        
    except Exception as e:
        logger.error(f"Route processing error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/route_status/<route_id>')
@login_required
def route_status_api(route_id):
    """API endpoint to get route processing status"""
    route = route_model.find_by_id(route_id)
    if not route:
        return jsonify({'error': 'Route not found'}), 404
        
    return jsonify({
        'status': route['processing_status'],
        'progress': route.get('processing_progress', 0),
        'errors': route.get('processing_errors', [])
    })

@app.route('/api/statistics')
@login_required
def statistics_api():
    """API endpoint for dashboard statistics"""
    # Route statistics
    total_routes = db.routes.count_documents({})
    processed_routes = db.routes.count_documents({'processing_status': 'completed'})
    
    # API usage statistics
    api_stats = api_log_model.get_api_stats()
    
    # Risk distribution
    risk_distribution = db.routes.aggregate([
        {'$match': {'processing_status': 'completed'}},
        {'$group': {
            '_id': '$risk_scores.risk_level',
            'count': {'$sum': 1}
        }}
    ])
    
    return jsonify({
        'routes': {
            'total': total_routes,
            'processed': processed_routes,
            'processing_rate': (processed_routes / total_routes * 100) if total_routes > 0 else 0
        },
        'api_usage': api_stats,
        'risk_distribution': list(risk_distribution)
    })

# Error handlers
@app.errorhandler(404)
def not_found_error(error):
    return render_template('404.html'), 404

@app.errorhandler(500)
def internal_error(error):
    db.rollback()
    return render_template('500.html'), 500

# Initialize app
if __name__ == '__main__':
    with app.app_context():
        create_default_admin()
    
    app.run(debug=True, port=5000)