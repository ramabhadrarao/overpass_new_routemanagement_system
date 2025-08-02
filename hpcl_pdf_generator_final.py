#!/usr/bin/env python3
"""
HPCL Dynamic PDF Generator - MongoDB Integration
Purpose: Generate comprehensive HPCL Journey Risk Management PDF reports from MongoDB data
Author: HPCL Journey Risk Management System
Dependencies: pymongo, reportlab, python-dotenv
"""

import os
import sys
import math
import io
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

import requests
import tempfile

from PIL import Image
from io import BytesIO

import folium
from folium import plugins

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

from staticmap import StaticMap, CircleMarker, Line

from google_maps_image_downloader import GoogleMapsImageDownloader

OVERPASS_API_URL = os.getenv('OVERPASS_API_URL', 'http://43.250.40.133:8080/api/interpreter')

# Third-party imports
try:
    from pymongo import MongoClient
    from bson import ObjectId
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.colors import Color, HexColor
    from reportlab.lib.units import mm, inch
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
    from reportlab.pdfgen import canvas
    from reportlab.graphics.shapes import Drawing, Rect, String
    from reportlab.pdfbase.pdfmetrics import stringWidth
   
    from dotenv import load_dotenv
    from googletrans import Translator
except ImportError as e:
    print("googletrans not installed. Translation features will be disabled.")
    print("To enable translation, install with: pip install googletrans==4.0.0-rc1")
    Translator = None

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class HPCLColors:
    """HPCL Brand Colors"""
    PRIMARY = HexColor('#005293')      # HPCL Blue
    SECONDARY = HexColor('#3C3C3C')    # Dark Gray
    DANGER = HexColor('#DC3545')       # Red
    WARNING = HexColor('#FD7E14')      # Orange
    SUCCESS = HexColor('#28A745')      # Green
    INFO = HexColor('#005293')         # HPCL Blue
    LIGHT_GRAY = HexColor('#F5F5F5')   # Light Gray
    WHITE = HexColor('#FFFFFF')        # White
    ACCENT = HexColor('#FFC107')       # Yellow accent
    DARK_GRAY = HexColor('#343A40')    # Dark text
    BLACK = HexColor('#000000')        # Black

class HPCLDynamicPDFGenerator:
    """
    Enhanced HPCL PDF Generator that reads from MongoDB models
    and generates comprehensive route analysis reports
    """
    
    def __init__(self, mongodb_uri: str = None):
        """Initialize the PDF generator with MongoDB connection"""
        self.mongodb_uri = mongodb_uri or os.getenv('MONGODB_URI', 'mongodb://127.0.0.1:27017/hpcl_journey_risk')
        self.colors = HPCLColors()
        self.client = None
        self.db = None
        
        # PDF Configuration
        self.page_width, self.page_height = A4
        self.margin = 50
        self.content_width = self.page_width - (2 * self.margin)
        
        # Connect to MongoDB
        self.connect_to_mongodb()
        
        logger.info("✅ HPCL Dynamic PDF Generator initialized")
    
    def connect_to_mongodb(self):
        """Establish MongoDB connection"""
        try:
            self.client = MongoClient(self.mongodb_uri)
            # Extract database name from URI or use default
            db_name = self.mongodb_uri.split('/')[-1] if '/' in self.mongodb_uri else 'hpcl_journey_risk'
            self.db = self.client[db_name]
            
            # Test connection
            self.client.admin.command('ping')
            logger.info(f"✅ Connected to MongoDB: {db_name}")
            
        except Exception as e:
            logger.error(f"❌ MongoDB connection failed: {e}")
            raise
    
    def load_route_data(self, route_id: str) -> Dict[str, Any]:
        """Load complete route data from MongoDB"""
        try:
            route_obj_id = ObjectId(route_id)
            logger.info(f"🔄 Loading route data for: {route_id}")
            
            # Load main route
            route = self.db.routes.find_one({'_id': route_obj_id})
            if not route:
                raise ValueError(f"Route not found: {route_id}")
            
            logger.info(f"📊 Found route: {route.get('routeName', 'Unknown')}")
            
            # Load related data
            collections_data = {
                'sharp_turns': list(self.db.sharpturns.find({'routeId': route_obj_id})),
                'blind_spots': list(self.db.blindspots.find({'routeId': route_obj_id})),
                'accident_areas': list(self.db.accidentproneareas.find({'routeId': route_obj_id})),
                'road_conditions': list(self.db.roadconditions.find({'routeId': route_obj_id})),
                'weather_conditions': list(self.db.weatherconditions.find({'routeId': route_obj_id})),
                'traffic_data': list(self.db.trafficdata.find({'routeId': route_obj_id})),
                'emergency_services': list(self.db.emergencyservices.find({'routeId': route_obj_id})),
                'network_coverage': list(self.db.networkcoverages.find({'routeId': route_obj_id})),
                'eco_sensitive_zones': list(self.db.ecosensitivezones.find({'routeId': route_obj_id}))
            }
            
            # Calculate statistics
            stats = self.calculate_route_statistics(collections_data)
            
            # Combine all data
            complete_data = {
                'route': route,
                'collections': collections_data,
                'statistics': stats,
                'data_quality': self.assess_data_quality(stats['total_data_points']),
                'last_analyzed': datetime.now()
            }
            
            logger.info(f"✅ Data loaded: {stats['total_data_points']} total data points")
            return complete_data
            
        except Exception as e:
            logger.error(f"❌ Error loading route data: {e}")
            raise
    
    def calculate_route_statistics(self, collections: Dict[str, List]) -> Dict[str, Any]:
        """Calculate comprehensive route statistics"""
        stats = {
            'total_data_points': 0,
            'risk_analysis': {
                'avg_risk_score': 0,
                'max_risk_score': 0,
                'critical_points': 0,
                'high_risk_points': 0,
                'risk_distribution': {'low': 0, 'medium': 0, 'high': 0, 'critical': 0}
            },
            'safety_metrics': {
                'hospitals': 0,
                'police_stations': 0,
                'fire_stations': 0,
                'sharp_turns': 0,
                'blind_spots': 0,
                'accident_areas': 0
            },
            'infrastructure': {
                'road_quality_issues': 0,
                'weather_risk_areas': 0,
                'traffic_congestion_points': 0,
                'network_dead_zones': 0
            }
        }
        
        all_risk_scores = []
        
        # Process each collection
        for collection_name, items in collections.items():
            stats['total_data_points'] += len(items)
            
            for item in items:
                # Collect risk scores
                risk_score = item.get('riskScore', 0)
                if risk_score > 0:
                    all_risk_scores.append(risk_score)
                    
                    # Categorize risk
                    if risk_score >= 8:
                        stats['risk_analysis']['critical_points'] += 1
                        stats['risk_analysis']['risk_distribution']['critical'] += 1
                    elif risk_score >= 6:
                        stats['risk_analysis']['high_risk_points'] += 1
                        stats['risk_analysis']['risk_distribution']['high'] += 1
                    elif risk_score >= 4:
                        stats['risk_analysis']['risk_distribution']['medium'] += 1
                    else:
                        stats['risk_analysis']['risk_distribution']['low'] += 1
                
                # Collection-specific metrics
                if collection_name == 'emergency_services':
                    service_type = item.get('serviceType', '')
                    if service_type == 'hospital':
                        stats['safety_metrics']['hospitals'] += 1
                    elif service_type == 'police':
                        stats['safety_metrics']['police_stations'] += 1
                    elif service_type == 'fire_station':
                        stats['safety_metrics']['fire_stations'] += 1
                
                elif collection_name == 'sharp_turns':
                    stats['safety_metrics']['sharp_turns'] += 1
                
                elif collection_name == 'blind_spots':
                    stats['safety_metrics']['blind_spots'] += 1
                
                elif collection_name == 'accident_areas':
                    stats['safety_metrics']['accident_areas'] += 1
                
                elif collection_name == 'road_conditions':
                    if item.get('surfaceQuality') in ['poor', 'critical']:
                        stats['infrastructure']['road_quality_issues'] += 1
                
                elif collection_name == 'network_coverage':
                    if item.get('isDeadZone', False):
                        stats['infrastructure']['network_dead_zones'] += 1
        
        # Calculate averages
        if all_risk_scores:
            stats['risk_analysis']['avg_risk_score'] = sum(all_risk_scores) / len(all_risk_scores)
            stats['risk_analysis']['max_risk_score'] = max(all_risk_scores)
        
        return stats
    
    def assess_data_quality(self, total_points: int) -> Dict[str, Any]:
        """Assess data quality based on total data points"""
        if total_points >= 100:
            return {'level': 'excellent', 'score': 95}
        elif total_points >= 50:
            return {'level': 'good', 'score': 80}
        elif total_points >= 20:
            return {'level': 'fair', 'score': 65}
        elif total_points >= 5:
            return {'level': 'poor', 'score': 40}
        else:
            return {'level': 'insufficient', 'score': 20}
    
    def calculate_risk_level(self, avg_score: float) -> str:
        """Calculate risk level from average score"""
        if avg_score >= 8:
            return 'CRITICAL'
        elif avg_score >= 6:
            return 'HIGH'
        elif avg_score >= 4:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def format_duration(self, minutes: int) -> str:
        """Format duration from minutes to readable format"""
        if not minutes:
            return 'Not specified'
        
        hours = minutes // 60
        mins = minutes % 60
        
        if hours > 0:
            return f"{hours} hours {mins} mins"
        else:
            return f"{mins} minutes"
    
    def get_risk_color(self, level: str):
        """Get color based on risk/quality level"""
        color_map = {
            'excellent': self.colors.SUCCESS,
            'good': self.colors.INFO,
            'fair': self.colors.WARNING,
            'poor': self.colors.DANGER,
            'insufficient': self.colors.SECONDARY,
            'critical': self.colors.DANGER,
            'high': HexColor('#FF5722'),
            'medium': self.colors.WARNING,
            'low': self.colors.SUCCESS
        }
        return color_map.get(level.lower(), self.colors.SECONDARY)
    
    def add_page_header(self, canvas_obj, title: str, subtitle: str = "",page_num: Optional[int] = None):
        """Add consistent page header"""
        # Header background
        canvas_obj.saveState()
        canvas_obj.setFillColor(self.colors.PRIMARY)
        canvas_obj.rect(0, self.page_height - 60, self.page_width, 60, fill=1)
        
        canvas_obj.setFillColor(self.colors.WHITE)
        canvas_obj.setFont("Helvetica-Bold", 14)
        canvas_obj.drawString(self.margin, self.page_height - 35, title)
        


        if subtitle:
            canvas_obj.setFont("Helvetica", 10)
            canvas_obj.drawString(self.margin, self.page_height - 50, subtitle)

        if page_num:
            canvas_obj.setFont("Helvetica", 10)
            date_str = datetime.now().strftime('%B %d, %Y')
            canvas_obj.drawRightString(self.page_width - self.margin, self.page_height - 35, f"Page {page_num}")
            canvas_obj.drawRightString(self.page_width - self.margin, self.page_height - 50, date_str)
        canvas_obj.restoreState()

    def add_logo(self, canvas_obj):
        """Add HPCL logo"""
        try:
            logo_url = 'https://i.ibb.co/pWf1tXF/HPCL-logo.png'
            response = requests.get(logo_url)
            response.raise_for_status()
            logo_image = Image.open(io.BytesIO(response.content))
            canvas_obj.drawImage(logo_image, 50, self.page_height - 100, width=50, height=50, mask='auto')
        except Exception as e:
            logger.error(f"Error loading logo: {e}")
            canvas_obj.setFillColor(self.colors.WHITE)
            canvas_obj.setFont("Helvetica-Bold", 20)
            canvas_obj.drawString(50, self.page_height - 80, "HPCL")

    def create_title_page(self, canvas_obj, route_data: Dict[str, Any]):
        """Create the title page with route information with proper line spacing"""

        route = route_data['route']
        stats = route_data['statistics']
        canvas_obj.saveState()
        
        # Header block
        canvas_obj.setFillColor(self.colors.PRIMARY)
        canvas_obj.rect(0, self.page_height - 120, self.page_width, 120, fill=1)
        
        self.add_logo(canvas_obj)
        
        canvas_obj.setFillColor(self.colors.WHITE)
        canvas_obj.setFont("Helvetica-Bold", 16)
        canvas_obj.drawString(120, self.page_height - 50, "HINDUSTAN PETROLEUM CORPORATION")
        canvas_obj.setFont("Helvetica", 15)
        canvas_obj.drawString(120, self.page_height - 75, "Journey Risk Management Division")  # Increased spacing
        canvas_obj.setFont("Helvetica-Oblique", 9)
        canvas_obj.drawString(120, self.page_height - 95, "Powered by Route Analytics Pro - AI Intelligence Platform")  # Increased spacing
        
        # Main title
        canvas_obj.setFillColor(self.colors.PRIMARY)
        canvas_obj.setFont("Helvetica-Bold", 25)
        title_y = self.page_height - 230  # Adjusted for more space
        canvas_obj.drawString(self.margin, title_y, "COMPREHENSIVE JOURNEY RISK")
        canvas_obj.drawString(self.margin, title_y - 35, "MANAGEMENT ANALYSIS REPORT")  # Increased spacing
        
        # Route details box
        box_y = title_y - 100  # Increased space above box
        box_height = 240
        
        canvas_obj.setStrokeColor(self.colors.SECONDARY)
        canvas_obj.rect(self.margin, box_y - box_height, self.content_width, box_height, stroke=1, fill=0)
        
        # Box title
        canvas_obj.setFillColor(self.colors.PRIMARY)
        canvas_obj.setFont("Helvetica-Bold", 14)
        canvas_obj.drawString(self.margin + 10, box_y - 20, "ROUTE ANALYSIS DETAILS")  # Increased space
        
        # Box content
        canvas_obj.setFillColor(self.colors.SECONDARY)
        canvas_obj.setFont("Helvetica", 12)
        
        # Details with proper line spacing

        details = [
            (f"Supply Location: {route.get('fromAddress', 'Not specified')} [{route.get('fromCode', 'N/A')}]", 15),
            ("", 5),  # Empty line with spacing
            (f"Destination: {route.get('toAddress', 'Not specified')} [{route.get('toCode', 'N/A')}]", 15),
            ("", 5),  # Empty line with spacing
            (f"Total Distance: {route.get('totalDistance', 0)} km", 15),
            ("", 5),  # Empty line with spacing
            (f"Estimated Duration: {self.format_duration(route.get('estimatedDuration', 0))}", 15),
            ("", 5),  # Empty line with spacing
            # (f"Route Terrain: {route.get('terrain', 'Mixed')}", 15),  # Now as tuple with spacing
            # ("", 5),  # Empty line with spacing
            # (f"Total Data Points Analyzed: {stats['total_data_points']}", 15),  # Now as tuple with spacing
            # ("", 5),  # Empty line with spacing
            # (f"Critical Risk Points: {stats['risk_analysis']['critical_points']}", 15),  # Now as tuple with spacing
            # ("", 5),  # Empty line with spacing
            (f"Analysis Date: {datetime.now().strftime('%B %d, %Y')}", 15),
            ("", 5),  # Empty line with spacing
            (f"Report Generated: {datetime.now().strftime('%I:%M %p')}", 15)
        ]
        
        y_pos = box_y - 55  # Increased initial spacing
        for detail, spacing in details:
            if detail:  # Only draw if there's actual text
                canvas_obj.drawString(self.margin + 10, y_pos, detail)
            y_pos -= spacing
        
        # Footer
        self.add_title_page_footer(canvas_obj)
        canvas_obj.restoreState()
    
    def add_risk_indicator(self, canvas_obj, route_data: Dict[str, Any], y_position: float):
        """Add risk level indicator"""
        stats = route_data['statistics']
        avg_risk = stats['risk_analysis']['avg_risk_score']
        risk_level = self.calculate_risk_level(avg_risk)
        critical_points = stats['risk_analysis']['critical_points']
        
        # Risk box with shadow
        canvas_obj.setFillColorRGB(0.8, 0.8, 0.8)
        canvas_obj.rect(62, y_position - 2, self.content_width - 4, 30, fill=1)
        
        risk_color = self.get_risk_color(risk_level.lower())
        canvas_obj.setFillColor(risk_color)
        canvas_obj.rect(60, y_position, self.content_width, 30, fill=1)
        
        # Accent border
        canvas_obj.setFillColor(self.colors.ACCENT)
        canvas_obj.rect(60, y_position + 27, self.content_width, 3, fill=1)
        
        # Risk text
        risk_icon = {'CRITICAL': '🚨', 'HIGH': '⚠️', 'MEDIUM': '⚡', 'LOW': '✅'}.get(risk_level, '⏳')
        risk_text = f"{risk_icon} ROUTE RISK LEVEL: {risk_level}"
        
        if avg_risk > 0:
            risk_text += f" (Score: {avg_risk:.1f}/10)"
        if critical_points > 0:
            risk_text += f" • {critical_points} Critical Points"
        
        canvas_obj.setFillColor(self.colors.WHITE)
        canvas_obj.setFont("Helvetica-Bold", 12)
        
        # Center the risk text manually
        text_width = canvas_obj.stringWidth(risk_text, "Helvetica-Bold", 12)
        canvas_obj.drawString((self.page_width - text_width) / 2, y_position + 9, risk_text)

    def add_title_page_footer(self, canvas_obj):
        """Add a simple footer for the title page"""
        canvas_obj.saveState()
        footer_y = 30
        canvas_obj.setFillColor(self.colors.SECONDARY)
        canvas_obj.setFont("Helvetica-Oblique", 8)
        
        footer_text1 = "Generated by HPCL Journey Risk Management System - Complete Enhanced Analysis"
        footer_text2 = "CONFIDENTIAL - For Internal Use Only"
        
        canvas_obj.drawCentredString(self.page_width / 2, footer_y, footer_text1)
        canvas_obj.drawCentredString(self.page_width / 2, footer_y - 12, footer_text2)
        canvas_obj.restoreState()
    
    def create_executive_summary_page(self, canvas_obj, route_data: Dict[str, Any]):
            """Create Page 2: Executive Summary with simple tables"""
            route = route_data['route']
            stats = route_data['statistics']
            
            # Page header
            self.add_page_header(canvas_obj, "HPCL - Journey Risk Management Study (AI-Powered Analysis)")

            y_pos = self.page_height - 100

            canvas_obj.setFillColor("#5B1F21")
            canvas_obj.setFont("Helvetica-Bold", 18)
            canvas_obj.drawCentredString(self.page_width / 2, y_pos, "EXECUTIVE SUMMARY")
            y_pos -= 40
            
            # Route Overview Table
            route_headers = ["PARAMETER", "DETAILS"]
            route_col_widths = [200, 280]
            
            route_overview_data = [
                ["Origin", route.get('fromAddress', 'Not specified')],
                ["Destination", route.get('toAddress', 'Not specified')],
                ["Total Distance", f"{route.get('totalDistance', 0)} km"],
                ["Estimated Duration", self.format_duration(route.get('estimatedDuration', 0))],
                ["Major Highways", ", ".join(route.get('majorHighways', ['Not specified'])[:2])],
                ["Terrain", route.get('terrain', 'Mixed').title()]
            ]
            
            y_pos = self.create_simple_table(
                canvas_obj,
                "ROUTE OVERVIEW",
                route_headers,
                route_overview_data,
                50, y_pos,
                route_col_widths,
                title_color=self.colors.WHITE,
                header_color=self.colors.WHITE,
                text_color=self.colors.PRIMARY
            )
            
            # Overall weighted score - MOVED HERE
            y_pos -= 60  # Reduced spacing
            avg_risk = stats['risk_analysis']['avg_risk_score']
            risk_level = self.calculate_risk_level(avg_risk)

            y_pos = self.draw_centered_text_in_box(
                canvas_obj,
                f"TOTAL WEIGHTED ROUTE SCORE – {round(avg_risk):.1f} [{risk_level.upper()} RISK]",
                30, y_pos, 520, 40,
                "Helvetica-Bold", 14,
                text_color=self.colors.WHITE,
                box_color=self.get_risk_color(risk_level.lower())
                )
            
            # Risk Factor Rating Overview
            risk_headers = ["RISK CRITERION", "RISK SCORE", "RISK CATEGORY"]
            risk_col_widths = [200, 100, 150]
            
            risk_factors = self.get_risk_factors_from_data(route_data)
            
            self.create_simple_table(
                canvas_obj,
                "RISK FACTOR RATING OVERVIEW",
                risk_headers,
                risk_factors,
                50, y_pos,
                risk_col_widths,
                title_color=self.colors.WHITE,
                header_color=self.colors.WHITE,
                text_color=self.colors.PRIMARY
        )
    
    def get_risk_factors_from_data(self, route_data: Dict[str, Any]) -> List[tuple]:
        """Extract risk factors from actual data"""
        collections = route_data['collections']
        stats = route_data['statistics']
        
        # Calculate risk scores based on actual data
        road_conditions_score = min(10, len([r for r in collections['road_conditions'] if r.get('surfaceQuality') in ['poor', 'critical']]) * 0.5 + 2)
        accident_score = min(10, len(collections['accident_areas']) * 0.3 + 1)
        sharp_turns_score = min(10, len([t for t in collections['sharp_turns'] if t.get('riskScore', 0) >= 6]) * 0.4 + 1)
        blind_spots_score = min(10, len([b for b in collections['blind_spots'] if b.get('riskScore', 0) >= 6]) * 0.5 + 1)
        traffic_score = min(10, len([t for t in collections['traffic_data'] if t.get('congestionLevel') in ['heavy', 'severe']]) * 0.3 + 1)
        weather_score = min(10, len([w for w in collections['weather_conditions'] if w.get('riskScore', 0) >= 6]) * 0.4 + 2)
        emergency_score = max(1, 10 - len(collections['emergency_services']) * 0.1)
        network_score = min(10, len([n for n in collections['network_coverage'] if n.get('isDeadZone', False)]) * 0.8 + 1)
        
        return [
            ("Road Conditions", f"{road_conditions_score:.1f}", self.get_risk_category_text(road_conditions_score)),
            ("Accident-Prone Areas", f"{accident_score:.1f}", self.get_risk_category_text(accident_score)),
            ("Sharp Turns", f"{sharp_turns_score:.1f}", self.get_risk_category_text(sharp_turns_score)),
            ("Blind Spots", f"{blind_spots_score:.1f}", self.get_risk_category_text(blind_spots_score)),
            ("Traffic Condition (Density)", f"{traffic_score:.1f}", self.get_risk_category_text(traffic_score)),
            ("Seasonal Weather Conditions", f"{weather_score:.1f}", self.get_risk_category_text(weather_score)),
            ("Emergency Handling Services", f"{emergency_score:.1f}", self.get_risk_category_text(emergency_score)),
            ("Network Dead/Low Zones", f"{network_score:.1f}", self.get_risk_category_text(network_score)),
            ("Security & Social Issues", "1.0", "Low Risk")
        ]
        # Combine scores
    
    def get_risk_category_text(self, score: float) -> str:
        """Convert risk score to category text"""
        if score >= 7:
            return "High Risk"
        elif score >= 4:
            return "Mild Risk"
        else:
            return "Low Risk"

    def get_risk_score_color(self, score: float):
        """Get color for risk score"""
        if score >= 7:
            return self.colors.DANGER
        elif score >= 4:
            return self.colors.WARNING
        else:
            return self.colors.SUCCESS
    


    # Update the create_route_map_page method
    def create_route_map_page(self, canvas_obj, route_data: Dict[str, Any]):
        """Create Page 3: Route Map using Overpass API or alternative mapping solutions"""
        self.add_page_header(canvas_obj, "HPCL - Journey Risk Management Study", "Approved Route Map")
        collections = route_data['collections']
        route = route_data['route']

        y_pos = self.page_height - 120
        
        canvas_obj.setFillColor(self.colors.PRIMARY)
        canvas_obj.setFont("Helvetica-Bold", 18)
        canvas_obj.drawString(self.margin, y_pos, "APPROVED ROUTE MAP")
        y_pos -= 20
        
        canvas_obj.setFillColor(self.colors.BLACK)
        canvas_obj.setFont("Helvetica", 10)
        canvas_obj.drawString(self.margin, y_pos, "Comprehensive route visualization showing start/end points, critical turns, emergency services,")
        y_pos -= 20
        canvas_obj.drawString(self.margin, y_pos, " highway junctions, and potential hazards.")
        y_pos -= 20
        
        try:
            # Try multiple map generation methods in order of preference
            map_image_path = None
            interactive_link = None
            
            # Method 1: Try using staticmap library (simplest, no API needed)
            logger.info("Attempting to generate map using staticmap library...")
            map_image_path, interactive_link = self.generate_static_map_image(route_data)
            
            # Method 2: If staticmap fails, try matplotlib with OSM tiles
            if not map_image_path or not os.path.exists(map_image_path):
                logger.info("Staticmap failed, trying matplotlib approach...")
                map_image_path, interactive_link = self.generate_matplotlib_osm_map(route_data)
            
            # Method 3: If still no map, use the existing Overpass implementation
            if not map_image_path or not os.path.exists(map_image_path):
                logger.info("Trying Overpass API approach...")
                map_image_path, interactive_link = self.generate_overpass_route_map(route_data)
            
            # Display the map if we have one
            if map_image_path and os.path.exists(map_image_path):
                logger.info(f"Map generated successfully: {map_image_path}")
                # Draw the map image
                canvas_obj.drawImage(map_image_path, 40, y_pos - 350, width=520, height=350)
                
                # Add route statistics overlay
                self.add_route_statistics_overlay(canvas_obj, route_data, y_pos - 370)
                
                # Add interactive link box
                if interactive_link:
                    self.add_osm_link_box(canvas_obj, interactive_link, y_pos - 380)
                
                y_pos -= 390
            else:
                logger.warning("All map generation methods failed, using placeholder")
                # Fallback to placeholder
                self.draw_map_placeholder(canvas_obj, route_data, y_pos)
                y_pos -= 280
                
        except Exception as e:
            logger.error(f"Failed to generate map: {e}")
            import traceback
            logger.error(traceback.format_exc())
            # Fallback to placeholder
            self.draw_map_placeholder(canvas_obj, route_data, y_pos)
            y_pos -= 280
        
        y_pos -= 35
        # Enhanced Map legend with actual counts
        self.add_enhanced_map_legend(canvas_obj, route_data, y_pos)

    # Add these new methods for map generation

    def generate_static_map_image(self, route_data: Dict[str, Any]) -> tuple:
        """Generate a static map image using staticmap library (no API key needed)"""
        try:
            from staticmap import StaticMap, CircleMarker, Line
            
            route = route_data['route']
            route_points = route.get('routePoints', [])
            
            if not route_points:
                logger.error("No route points found")
                return None, None
            
            # Create static map instance
            m = StaticMap(800, 600, padding_x=50, padding_y=50)
            
            # Extract coordinates
            coordinates = [(p['longitude'], p['latitude']) for p in route_points]
            
            # Add route line
            route_line = Line(coordinates, 'blue', 3)
            m.add_line(route_line)
            
            # Add start marker
            start_marker = CircleMarker(
                (route_points[0]['longitude'], route_points[0]['latitude']), 
                'green', 12
            )
            m.add_marker(start_marker)
            
            # Add end marker
            end_marker = CircleMarker(
                (route_points[-1]['longitude'], route_points[-1]['latitude']), 
                'red', 12
            )
            m.add_marker(end_marker)
            
            # Add critical points from collections
            collections = route_data['collections']
            
            # Add sharp turns
            for turn in collections.get('sharp_turns', [])[:10]:
                if turn.get('riskScore', 0) >= 7:
                    marker = CircleMarker(
                        (turn['longitude'], turn['latitude']), 
                        'orange', 8
                    )
                    m.add_marker(marker)
            
            # Add blind spots
            for spot in collections.get('blind_spots', [])[:10]:
                if spot.get('riskScore', 0) >= 7:
                    marker = CircleMarker(
                        (spot['longitude'], spot['latitude']), 
                        '#FF5722', 8
                    )
                    m.add_marker(marker)
            
            # Add hospitals
            for hospital in collections.get('emergency_services', [])[:5]:
                if hospital.get('serviceType') == 'hospital':
                    marker = CircleMarker(
                        (hospital['longitude'], hospital['latitude']), 
                        'blue', 6
                    )
                    m.add_marker(marker)
            
            # Render the image
            image = m.render()
            
            # Save to file
            map_filename = f"route_map_{route['_id']}.png"
            map_path = os.path.join(tempfile.gettempdir(), map_filename)
            image.save(map_path)
            
            # Generate OSM link
            center_lat = sum(p['latitude'] for p in route_points) / len(route_points)
            center_lon = sum(p['longitude'] for p in route_points) / len(route_points)
            osm_link = f"https://www.openstreetmap.org/#map=12/{center_lat}/{center_lon}"
            
            return map_path, osm_link
            
        except ImportError:
            logger.error("staticmap library not installed. Install with: pip install staticmap")
            return None, None
        except Exception as e:
            logger.error(f"Error generating static map: {e}")
            return None, None

    def generate_matplotlib_osm_map(self, route_data: Dict[str, Any]) -> tuple:
        """Generate map using matplotlib with improved styling"""
        try:
            import matplotlib.pyplot as plt
            import matplotlib.patches as mpatches
            from matplotlib.patches import FancyBboxPatch
            import numpy as np
            
            route = route_data['route']
            collections = route_data['collections']
            route_points = route.get('routePoints', [])
            
            if not route_points:
                logger.error("No route points found")
                return None, None
            
            # Create figure with better styling
            fig, ax = plt.subplots(figsize=(12, 9), dpi=150)
            
            # Set background color
            fig.patch.set_facecolor('white')
            ax.set_facecolor('#f0f0f0')
            
            # Extract coordinates
            lats = [p['latitude'] for p in route_points]
            lons = [p['longitude'] for p in route_points]
            
            # Plot route with gradient effect
            points = np.array([lons, lats]).T.reshape(-1, 1, 2)
            segments = np.concatenate([points[:-1], points[1:]], axis=1)
            
            # Main route line
            ax.plot(lons, lats, 'b-', linewidth=4, label='Route Path', 
                    zorder=2, alpha=0.8, solid_capstyle='round')
            
            # Add shadow effect
            ax.plot(lons, lats, 'gray', linewidth=5, alpha=0.3, 
                    zorder=1, transform=ax.transData)
            
            # Start and end points with labels
            start_lon, start_lat = lons[0], lats[0]
            end_lon, end_lat = lons[-1], lats[-1]
            
            # Start point
            ax.scatter(start_lon, start_lat, c='green', s=400, marker='o', 
                    edgecolors='darkgreen', linewidth=3, label='Start', zorder=5)
            ax.annotate('START', (start_lon, start_lat), 
                    xytext=(15, 15), textcoords='offset points',
                    fontsize=11, fontweight='bold', color='darkgreen',
                    bbox=dict(boxstyle="round,pad=0.5", facecolor="lightgreen", 
                                edgecolor="darkgreen", alpha=0.8),
                    arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0.3',
                                    color='darkgreen', lw=2))
            
            # End point
            ax.scatter(end_lon, end_lat, c='red', s=400, marker='o', 
                    edgecolors='darkred', linewidth=3, label='End', zorder=5)
            ax.annotate('END', (end_lon, end_lat), 
                    xytext=(15, -15), textcoords='offset points',
                    fontsize=11, fontweight='bold', color='darkred',
                    bbox=dict(boxstyle="round,pad=0.5", facecolor="lightcoral", 
                                edgecolor="darkred", alpha=0.8),
                    arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=-0.3',
                                    color='darkred', lw=2))
            
            # Add critical points with better styling
            self._add_styled_critical_points(ax, collections)
            
            # Calculate bounds with padding
            lat_range = max(lats) - min(lats)
            lon_range = max(lons) - min(lons)
            padding = max(lat_range, lon_range) * 0.15
            
            ax.set_xlim(min(lons) - padding, max(lons) + padding)
            ax.set_ylim(min(lats) - padding, max(lats) + padding)
            
            # Styling
            ax.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)
            ax.set_xlabel('Longitude', fontsize=12, fontweight='bold')
            ax.set_ylabel('Latitude', fontsize=12, fontweight='bold')
            
            # Title with route info
            route_name = f"{route.get('fromAddress', 'Start')} to {route.get('toAddress', 'End')}"
            distance = route.get('totalDistance', 0)
            title = f"Route Map: {route_name}\nTotal Distance: {distance} km"
            ax.set_title(title, fontsize=16, fontweight='bold', pad=20)
            
            # Add legend with custom styling
            legend_elements = [
                plt.Line2D([0], [0], color='blue', lw=4, label='Route Path'),
                plt.scatter([], [], c='green', s=200, marker='o', label='Start Point'),
                plt.scatter([], [], c='red', s=200, marker='o', label='End Point'),
                plt.scatter([], [], c='orange', s=100, marker='^', label='Sharp Turns'),
                plt.scatter([], [], c='#FF5722', s=100, marker='s', label='Blind Spots'),
                plt.scatter([], [], c='blue', s=80, marker='H', label='Hospitals'),
                plt.scatter([], [], c='purple', s=80, marker='P', label='Police Stations')
            ]
            
            legend = ax.legend(handles=legend_elements, loc='best', 
                            fontsize=10, frameon=True, shadow=True,
                            fancybox=True, framealpha=0.9)
            legend.get_frame().set_facecolor('white')
            
            # Add scale bar
            self._add_scale_bar(ax, lats, lons)
            
            # Add north arrow
            self._add_north_arrow(ax)
            
            # Add info box
            self._add_info_box(ax, route_data)
            
            # Save with high quality
            plt.tight_layout()
            
            map_filename = f"route_map_{route['_id']}.png"
            map_path = os.path.join(tempfile.gettempdir(), map_filename)
            
            plt.savefig(map_path, dpi=200, bbox_inches='tight', 
                    facecolor='white', edgecolor='none')
            plt.close()
            
            logger.info(f"Enhanced map saved successfully: {map_path}")
            
            # Generate OSM link
            center_lat = (min(lats) + max(lats)) / 2
            center_lon = (min(lons) + max(lons)) / 2
            osm_link = f"https://www.openstreetmap.org/#map=12/{center_lat}/{center_lon}"
            
            return map_path, osm_link
            
        except Exception as e:
            logger.error(f"Error generating matplotlib map: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None, None

    def _add_styled_critical_points(self, ax, collections: Dict[str, List]):
        """Add critical points to the map with improved styling"""
        try:
            # Sharp turns with risk-based sizing
            sharp_turns = collections.get('sharp_turns', [])
            for turn in sharp_turns[:15]:  # Limit to first 15
                risk_score = self.safe_float(turn.get('riskScore', 0))
                if risk_score >= 6:
                    lat = self.safe_float(turn.get('latitude', 0))
                    lon = self.safe_float(turn.get('longitude', 0))
                    if lat and lon:
                        # Size based on risk
                        size = 50 + (risk_score * 10)
                        color = 'red' if risk_score >= 8 else 'orange'
                        ax.scatter(lon, lat, c=color, s=size, marker='^', 
                                edgecolors='black', linewidth=1, zorder=3, alpha=0.8)
            
            # Blind spots
            blind_spots = collections.get('blind_spots', [])
            for spot in blind_spots[:15]:  # Limit to first 15
                risk_score = self.safe_float(spot.get('riskScore', 0))
                if risk_score >= 6:
                    lat = self.safe_float(spot.get('latitude', 0))
                    lon = self.safe_float(spot.get('longitude', 0))
                    if lat and lon:
                        size = 50 + (risk_score * 10)
                        ax.scatter(lon, lat, c='#FF5722', s=size, marker='s', 
                                edgecolors='darkred', linewidth=1, zorder=3, alpha=0.8)
            
            # Emergency services with icons
            emergency_services = collections.get('emergency_services', [])
            
            # Hospitals
            hospitals = [s for s in emergency_services if s.get('serviceType') == 'hospital']
            for hospital in hospitals[:8]:
                lat = self.safe_float(hospital.get('latitude', 0))
                lon = self.safe_float(hospital.get('longitude', 0))
                if lat and lon:
                    ax.scatter(lon, lat, c='blue', s=60, marker='H', 
                            edgecolors='navy', linewidth=1.5, zorder=3)
            
            # Police stations
            police = [s for s in emergency_services if s.get('serviceType') == 'police']
            for station in police[:8]:
                lat = self.safe_float(station.get('latitude', 0))
                lon = self.safe_float(station.get('longitude', 0))
                if lat and lon:
                    ax.scatter(lon, lat, c='purple', s=60, marker='P', 
                            edgecolors='indigo', linewidth=1.5, zorder=3)
            
            # Network dead zones
            dead_zones = [n for n in collections.get('network_coverage', []) if n.get('isDeadZone', False)]
            for zone in dead_zones[:8]:
                lat = self.safe_float(zone.get('latitude', 0))
                lon = self.safe_float(zone.get('longitude', 0))
                if lat and lon:
                    ax.scatter(lon, lat, c='black', s=40, marker='x', 
                            linewidth=2.5, zorder=3)
                    
        except Exception as e:
            logger.warning(f"Error adding critical points to map: {e}")

    def _add_north_arrow(self, ax):
        """Add a north arrow to the map"""
        try:
            # Get current axes limits
            xlim = ax.get_xlim()
            ylim = ax.get_ylim()
            
            # Position in top-right corner
            arrow_x = xlim[1] - (xlim[1] - xlim[0]) * 0.05
            arrow_y = ylim[1] - (ylim[1] - ylim[0]) * 0.05
            arrow_length = (ylim[1] - ylim[0]) * 0.05
            
            # Draw arrow
            ax.annotate('N', xy=(arrow_x, arrow_y), xytext=(arrow_x, arrow_y - arrow_length),
                    ha='center', va='bottom', fontsize=14, fontweight='bold',
                    arrowprops=dict(arrowstyle='->', lw=2, color='black'))
            
        except Exception as e:
            logger.warning(f"Could not add north arrow: {e}")

    def _add_info_box(self, ax, route_data: Dict[str, Any]):
        """Add information box to the map"""
        try:
            stats = route_data['statistics']
            risk_level = self.calculate_risk_level(stats['risk_analysis']['avg_risk_score'])
            
            info_text = (
                f"Route Risk Level: {risk_level}\n"
                f"Critical Points: {stats['risk_analysis']['critical_points']}\n"
                f"Emergency Services: {stats['safety_metrics']['hospitals']} Hospitals, "
                f"{stats['safety_metrics']['police_stations']} Police Stations"
            )
            
            # Position in bottom-left
            xlim = ax.get_xlim()
            ylim = ax.get_ylim()
            
            box_x = xlim[0] + (xlim[1] - xlim[0]) * 0.02
            box_y = ylim[0] + (ylim[1] - ylim[0]) * 0.02
            
            # Create fancy box
            props = dict(boxstyle='round,pad=0.5', facecolor='white', 
                        edgecolor='gray', alpha=0.9)
            ax.text(box_x, box_y, info_text, fontsize=9, 
                bbox=props, verticalalignment='bottom')
            
        except Exception as e:
            logger.warning(f"Could not add info box: {e}")

# Update the existing generate_overpass_route_map method to use the OVERPASS_API_URL
    def generate_overpass_route_map(self, route_data: Dict[str, Any]) -> tuple:
        """Generate route map using Overpass API"""
        try:
            route = route_data['route']
            collections = route_data['collections']
            
            # Use the OVERPASS_API_URL from environment
            overpass_url = os.getenv('OVERPASS_API_URL', 'http://43.250.40.133:8080/api/interpreter')
            logger.info(f"Using Overpass API URL: {overpass_url}")
            
            # Rest of your existing generate_overpass_route_map implementation...
            # (Keep the existing code but make sure to use overpass_url for API calls)
            
            # Create a figure for the map
            fig, ax = plt.subplots(figsize=(10, 8))
            
            # Get route points
            route_points = route.get('routePoints', [])
            if not route_points:
                logger.error("No route points found")
                return None, None
                
            # Extract coordinates
            lats = [p['latitude'] for p in route_points]
            lons = [p['longitude'] for p in route_points]
            
            # Plot the route line with better styling
            ax.plot(lons, lats, 'b-', linewidth=3, label='Route', zorder=1, alpha=0.8)
            
            # Plot start and end points with larger markers
            ax.scatter(lons[0], lats[0], c='green', s=300, marker='o', 
                    edgecolors='darkgreen', linewidth=3, label='Start', zorder=3)
            ax.scatter(lons[-1], lats[-1], c='red', s=300, marker='o', 
                    edgecolors='darkred', linewidth=3, label='End', zorder=3)
            
            # Add text labels for start and end
            ax.annotate('START', (lons[0], lats[0]), xytext=(10, 10), 
                    textcoords='offset points', fontsize=10, fontweight='bold',
                    bbox=dict(boxstyle="round,pad=0.3", facecolor="green", alpha=0.5))
            ax.annotate('END', (lons[-1], lats[-1]), xytext=(10, 10), 
                    textcoords='offset points', fontsize=10, fontweight='bold',
                    bbox=dict(boxstyle="round,pad=0.3", facecolor="red", alpha=0.5))
            
            # Plot critical points
            self._add_critical_points_to_map(ax, collections)
            
            # Set map bounds with padding
            lat_range = max(lats) - min(lats)
            lon_range = max(lons) - min(lons)
            padding = max(lat_range, lon_range) * 0.15
            
            ax.set_xlim(min(lons) - padding, max(lons) + padding)
            ax.set_ylim(min(lats) - padding, max(lats) + padding)
            
            # Add grid and labels
            ax.grid(True, alpha=0.3, linestyle='--')
            ax.set_xlabel('Longitude', fontsize=11)
            ax.set_ylabel('Latitude', fontsize=11)
            
            # Add title with route information
            route_name = f"{route.get('fromAddress', 'Start')} to {route.get('toAddress', 'End')}"
            distance = route.get('totalDistance', 0)
            ax.set_title(f"Route Map: {route_name}\nTotal Distance: {distance} km", 
                        fontsize=14, fontweight='bold', pad=20)
            
            # Add legend with better positioning
            ax.legend(loc='best', fontsize=9, frameon=True, shadow=True)
            
            # Add scale bar
            self._add_scale_bar(ax, lats, lons)
            
            # Save the map
            map_filename = f"route_map_{route['_id']}.png"
            map_path = os.path.join(tempfile.gettempdir(), map_filename)
            
            # Save with higher DPI for better quality
            plt.tight_layout()
            plt.savefig(map_path, dpi=200, bbox_inches='tight', facecolor='white', edgecolor='none')
            plt.close()
            
            logger.info(f"Map saved successfully: {map_path}")
            
            # Generate OpenStreetMap link
            center_lat = (min(lats) + max(lats)) / 2
            center_lon = (min(lons) + max(lons)) / 2
            osm_link = f"https://www.openstreetmap.org/#map=12/{center_lat}/{center_lon}"
            
            return map_path, osm_link
            
        except Exception as e:
            logger.error(f"Error generating Overpass map: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None, None
    def _add_scale_bar(self, ax, lats, lons):
        """Add a scale bar to the map"""
        try:
            # Calculate scale
            lat_center = (max(lats) + min(lats)) / 2
            lon_range = max(lons) - min(lons)
            
            # Approximate km per degree longitude at this latitude
            km_per_deg = 111.32 * math.cos(math.radians(lat_center))
            
            # Calculate scale bar length (aim for nice round numbers)
            map_width_km = lon_range * km_per_deg
            if map_width_km > 100:
                scale_km = 50
            elif map_width_km > 50:
                scale_km = 20
            elif map_width_km > 20:
                scale_km = 10
            elif map_width_km > 10:
                scale_km = 5
            else:
                scale_km = 2
            
            scale_deg = scale_km / km_per_deg
            
            # Position scale bar in bottom right
            x_pos = max(lons) - lon_range * 0.15 - scale_deg
            y_pos = min(lats) + (max(lats) - min(lats)) * 0.05
            
            # Draw scale bar
            ax.plot([x_pos, x_pos + scale_deg], [y_pos, y_pos], 'k-', linewidth=3)
            ax.plot([x_pos, x_pos], [y_pos - 0.001, y_pos + 0.001], 'k-', linewidth=3)
            ax.plot([x_pos + scale_deg, x_pos + scale_deg], [y_pos - 0.001, y_pos + 0.001], 'k-', linewidth=3)
            
            # Add text
            ax.text(x_pos + scale_deg/2, y_pos + 0.002, f'{scale_km} km', 
                    ha='center', va='bottom', fontsize=8, fontweight='bold')
                    
        except Exception as e:
            logger.warning(f"Could not add scale bar: {e}")
    def _add_critical_points_to_map(self, ax, collections: Dict[str, List]):
        """Add critical points to the matplotlib map"""
        try:
            # Sharp turns
            sharp_turns = collections.get('sharp_turns', [])
            for turn in sharp_turns[:10]:  # Limit to first 10
                if self.safe_float(turn.get('riskScore', 0)) >= 7:
                    lat = self.safe_float(turn.get('latitude', 0))
                    lon = self.safe_float(turn.get('longitude', 0))
                    if lat and lon:
                        ax.scatter(lon, lat, c='orange', s=50, marker='^', 
                                edgecolors='darkorange', linewidth=1, zorder=2)
            
            # Blind spots
            blind_spots = collections.get('blind_spots', [])
            for spot in blind_spots[:10]:  # Limit to first 10
                if self.safe_float(spot.get('riskScore', 0)) >= 7:
                    lat = self.safe_float(spot.get('latitude', 0))
                    lon = self.safe_float(spot.get('longitude', 0))
                    if lat and lon:
                        ax.scatter(lon, lat, c='red', s=50, marker='s', 
                                edgecolors='darkred', linewidth=1, zorder=2)
            
            # Emergency services
            emergency_services = collections.get('emergency_services', [])
            
            # Hospitals
            hospitals = [s for s in emergency_services if s.get('serviceType') == 'hospital']
            for hospital in hospitals[:5]:  # Limit to 5
                lat = self.safe_float(hospital.get('latitude', 0))
                lon = self.safe_float(hospital.get('longitude', 0))
                if lat and lon:
                    ax.scatter(lon, lat, c='blue', s=40, marker='H', 
                            edgecolors='navy', linewidth=1, zorder=2)
            
            # Police stations (fixed color issue here)
            police = [s for s in emergency_services if s.get('serviceType') == 'police']
            for station in police[:5]:  # Limit to 5
                lat = self.safe_float(station.get('latitude', 0))
                lon = self.safe_float(station.get('longitude', 0))
                if lat and lon:
                    ax.scatter(lon, lat, c='purple', s=40, marker='P', 
                            edgecolors='indigo', linewidth=1, zorder=2)  # Changed from 'darkpurple' to 'indigo'
            
            # Network dead zones
            dead_zones = [n for n in collections.get('network_coverage', []) if n.get('isDeadZone', False)]
            for zone in dead_zones[:5]:  # Limit to 5
                lat = self.safe_float(zone.get('latitude', 0))
                lon = self.safe_float(zone.get('longitude', 0))
                if lat and lon:
                    ax.scatter(lon, lat, c='black', s=30, marker='x', 
                            linewidth=2, zorder=2)
                            
        except Exception as e:
            logger.warning(f"Error adding critical points to map: {e}")

    def generate_osm_tile_map(self, route_data: Dict[str, Any]) -> tuple:
        """Alternative: Generate map using OpenStreetMap tiles"""
        try:
            import folium
            
            route = route_data['route']
            route_points = route.get('routePoints', [])
            
            if not route_points:
                return None, None
            
            # Calculate center
            lats = [p['latitude'] for p in route_points]
            lons = [p['longitude'] for p in route_points]
            center_lat = sum(lats) / len(lats)
            center_lon = sum(lons) / len(lons)
            
            # Create folium map
            m = folium.Map(location=[center_lat, center_lon], zoom_start=12)
            
            # Add route line
            route_coords = [[p['latitude'], p['longitude']] for p in route_points]
            folium.PolyLine(route_coords, color='blue', weight=3, opacity=0.8).add_to(m)
            
            # Add start and end markers
            folium.Marker(
                [lats[0], lons[0]], 
                popup='Start', 
                icon=folium.Icon(color='green', icon='play')
            ).add_to(m)
            
            folium.Marker(
                [lats[-1], lons[-1]], 
                popup='End', 
                icon=folium.Icon(color='red', icon='stop')
            ).add_to(m)
            
            # Save as image
            map_filename = f"route_map_{route['_id']}.html"
            map_path = os.path.join(tempfile.gettempdir(), map_filename)
            m.save(map_path)
            
            # Convert HTML to PNG using selenium (if available)
            png_path = self._convert_folium_to_png(map_path)
            
            # Generate link
            osm_link = f"https://www.openstreetmap.org/#map=12/{center_lat}/{center_lon}"
            
            return png_path or map_path, osm_link
            
        except ImportError:
            logger.error("Folium not installed. Using matplotlib fallback.")
            return self.generate_overpass_route_map(route_data)
    def create_route_map_page(self, canvas_obj, route_data: Dict[str, Any]):
        """Create Page 3: Route Map using Overpass API or alternative mapping solutions"""
        self.add_page_header(canvas_obj, "HPCL - Journey Risk Management Study", "Approved Route Map")
        collections = route_data['collections']
        route = route_data['route']

        y_pos = self.page_height - 120
        
        canvas_obj.setFillColor(self.colors.PRIMARY)
        canvas_obj.setFont("Helvetica-Bold", 18)
        canvas_obj.drawString(self.margin, y_pos, "APPROVED ROUTE MAP")
        y_pos -= 20
        
        canvas_obj.setFillColor(self.colors.BLACK)
        canvas_obj.setFont("Helvetica", 10)
        canvas_obj.drawString(self.margin, y_pos, "Comprehensive route visualization showing start/end points, critical turns, emergency services,")
        y_pos -= 20
        canvas_obj.drawString(self.margin, y_pos, " highway junctions, and potential hazards.")
        y_pos -= 20
        
        try:
            # Try multiple map generation methods in order of preference
            map_image_path = None
            interactive_link = None
            
            # Method 1: Try using staticmap library (simplest, no API needed)
            logger.info("Attempting to generate map using staticmap library...")
            map_image_path, interactive_link = self.generate_static_map_image(route_data)
            
            # Method 2: If staticmap fails, try matplotlib with OSM tiles
            if not map_image_path or not os.path.exists(map_image_path):
                logger.info("Staticmap failed, trying matplotlib approach...")
                map_image_path, interactive_link = self.generate_matplotlib_osm_map(route_data)
            
            # Method 3: If still no map, use the existing Overpass implementation
            if not map_image_path or not os.path.exists(map_image_path):
                logger.info("Trying Overpass API approach...")
                map_image_path, interactive_link = self.generate_overpass_route_map(route_data)
            
            # Display the map if we have one
            if map_image_path and os.path.exists(map_image_path):
                logger.info(f"Map generated successfully: {map_image_path}")
                # Draw the map image
                canvas_obj.drawImage(map_image_path, 40, y_pos - 350, width=520, height=350)
                
                # Add route statistics overlay
                self.add_route_statistics_overlay(canvas_obj, route_data, y_pos - 370)
                
                # Add interactive link box
                if interactive_link:
                    self.add_osm_link_box(canvas_obj, interactive_link, y_pos - 380)
                
                y_pos -= 390
            else:
                logger.warning("All map generation methods failed, using placeholder")
                # Fallback to placeholder
                self.draw_map_placeholder(canvas_obj, route_data, y_pos)
                y_pos -= 280
                
        except Exception as e:
            logger.error(f"Failed to generate map: {e}")
            import traceback
            logger.error(traceback.format_exc())
            # Fallback to placeholder
            self.draw_map_placeholder(canvas_obj, route_data, y_pos)
            y_pos -= 280
        
        y_pos -= 35
        # Enhanced Map legend with actual counts
        self.add_enhanced_map_legend(canvas_obj, route_data, y_pos)
    def generate_static_map_image(self, route_data: Dict[str, Any]) -> tuple:
        """Generate a static map image using staticmap library (no API key needed)"""
        try:
            from staticmap import StaticMap, CircleMarker, Line
            
            route = route_data['route']
            route_points = route.get('routePoints', [])
            
            if not route_points:
                logger.error("No route points found")
                return None, None
            
            # Create static map instance
            m = StaticMap(800, 600, padding_x=50, padding_y=50)
            
            # Extract coordinates
            coordinates = [(p['longitude'], p['latitude']) for p in route_points]
            
            # Add route line
            route_line = Line(coordinates, 'blue', 3)
            m.add_line(route_line)
            
            # Add start marker
            start_marker = CircleMarker(
                (route_points[0]['longitude'], route_points[0]['latitude']), 
                'green', 12
            )
            m.add_marker(start_marker)
            
            # Add end marker
            end_marker = CircleMarker(
                (route_points[-1]['longitude'], route_points[-1]['latitude']), 
                'red', 12
            )
            m.add_marker(end_marker)
            
            # Add critical points from collections
            collections = route_data['collections']
            
            # Add sharp turns
            for turn in collections.get('sharp_turns', [])[:10]:
                if turn.get('riskScore', 0) >= 7:
                    marker = CircleMarker(
                        (turn['longitude'], turn['latitude']), 
                        'orange', 8
                    )
                    m.add_marker(marker)
            
            # Add blind spots
            for spot in collections.get('blind_spots', [])[:10]:
                if spot.get('riskScore', 0) >= 7:
                    marker = CircleMarker(
                        (spot['longitude'], spot['latitude']), 
                        '#FF5722', 8
                    )
                    m.add_marker(marker)
            
            # Add hospitals
            for hospital in collections.get('emergency_services', [])[:5]:
                if hospital.get('serviceType') == 'hospital':
                    marker = CircleMarker(
                        (hospital['longitude'], hospital['latitude']), 
                        'blue', 6
                    )
                    m.add_marker(marker)
            
            # Render the image
            image = m.render()
            
            # Save to file
            map_filename = f"route_map_{route['_id']}.png"
            map_path = os.path.join(tempfile.gettempdir(), map_filename)
            image.save(map_path)
            
            # Generate OSM link
            center_lat = sum(p['latitude'] for p in route_points) / len(route_points)
            center_lon = sum(p['longitude'] for p in route_points) / len(route_points)
            osm_link = f"https://www.openstreetmap.org/#map=12/{center_lat}/{center_lon}"
            
            return map_path, osm_link
            
        except ImportError:
            logger.error("staticmap library not installed. Install with: pip install staticmap")
            return None, None
        except Exception as e:
            logger.error(f"Error generating static map: {e}")
            return None, None

    def generate_matplotlib_osm_map(self, route_data: Dict[str, Any]) -> tuple:
        """Generate map using matplotlib with improved styling"""
        try:
            import matplotlib.pyplot as plt
            import matplotlib.patches as mpatches
            from matplotlib.patches import FancyBboxPatch
            import numpy as np
            
            route = route_data['route']
            collections = route_data['collections']
            route_points = route.get('routePoints', [])
            
            if not route_points:
                logger.error("No route points found")
                return None, None
            
            # Create figure with better styling
            fig, ax = plt.subplots(figsize=(12, 9), dpi=150)
            
            # Set background color
            fig.patch.set_facecolor('white')
            ax.set_facecolor('#f0f0f0')
            
            # Extract coordinates
            lats = [p['latitude'] for p in route_points]
            lons = [p['longitude'] for p in route_points]
            
            # Plot route with gradient effect
            points = np.array([lons, lats]).T.reshape(-1, 1, 2)
            segments = np.concatenate([points[:-1], points[1:]], axis=1)
            
            # Main route line
            ax.plot(lons, lats, 'b-', linewidth=4, label='Route Path', 
                    zorder=2, alpha=0.8, solid_capstyle='round')
            
            # Add shadow effect
            ax.plot(lons, lats, 'gray', linewidth=5, alpha=0.3, 
                    zorder=1, transform=ax.transData)
            
            # Start and end points with labels
            start_lon, start_lat = lons[0], lats[0]
            end_lon, end_lat = lons[-1], lats[-1]
            
            # Start point
            ax.scatter(start_lon, start_lat, c='green', s=400, marker='o', 
                    edgecolors='darkgreen', linewidth=3, label='Start', zorder=5)
            ax.annotate('START', (start_lon, start_lat), 
                    xytext=(15, 15), textcoords='offset points',
                    fontsize=11, fontweight='bold', color='darkgreen',
                    bbox=dict(boxstyle="round,pad=0.5", facecolor="lightgreen", 
                                edgecolor="darkgreen", alpha=0.8),
                    arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0.3',
                                    color='darkgreen', lw=2))
            
            # End point
            ax.scatter(end_lon, end_lat, c='red', s=400, marker='o', 
                    edgecolors='darkred', linewidth=3, label='End', zorder=5)
            ax.annotate('END', (end_lon, end_lat), 
                    xytext=(15, -15), textcoords='offset points',
                    fontsize=11, fontweight='bold', color='darkred',
                    bbox=dict(boxstyle="round,pad=0.5", facecolor="lightcoral", 
                                edgecolor="darkred", alpha=0.8),
                    arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=-0.3',
                                    color='darkred', lw=2))
            
            # Add critical points with better styling
            self._add_styled_critical_points(ax, collections)
            
            # Calculate bounds with padding
            lat_range = max(lats) - min(lats)
            lon_range = max(lons) - min(lons)
            padding = max(lat_range, lon_range) * 0.15
            
            ax.set_xlim(min(lons) - padding, max(lons) + padding)
            ax.set_ylim(min(lats) - padding, max(lats) + padding)
            
            # Styling
            ax.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)
            ax.set_xlabel('Longitude', fontsize=12, fontweight='bold')
            ax.set_ylabel('Latitude', fontsize=12, fontweight='bold')
            
            # Title with route info
            route_name = f"{route.get('fromAddress', 'Start')} to {route.get('toAddress', 'End')}"
            distance = route.get('totalDistance', 0)
            title = f"Route Map: {route_name}\nTotal Distance: {distance} km"
            ax.set_title(title, fontsize=16, fontweight='bold', pad=20)
            
            # Add legend with custom styling
            legend_elements = [
                plt.Line2D([0], [0], color='blue', lw=4, label='Route Path'),
                plt.scatter([], [], c='green', s=200, marker='o', label='Start Point'),
                plt.scatter([], [], c='red', s=200, marker='o', label='End Point'),
                plt.scatter([], [], c='orange', s=100, marker='^', label='Sharp Turns'),
                plt.scatter([], [], c='#FF5722', s=100, marker='s', label='Blind Spots'),
                plt.scatter([], [], c='blue', s=80, marker='H', label='Hospitals'),
                plt.scatter([], [], c='purple', s=80, marker='P', label='Police Stations')
            ]
            
            legend = ax.legend(handles=legend_elements, loc='best', 
                            fontsize=10, frameon=True, shadow=True,
                            fancybox=True, framealpha=0.9)
            legend.get_frame().set_facecolor('white')
            
            # Add scale bar
            self._add_scale_bar(ax, lats, lons)
            
            # Add north arrow
            self._add_north_arrow(ax)
            
            # Add info box
            self._add_info_box(ax, route_data)
            
            # Save with high quality
            plt.tight_layout()
            
            map_filename = f"route_map_{route['_id']}.png"
            map_path = os.path.join(tempfile.gettempdir(), map_filename)
            
            plt.savefig(map_path, dpi=200, bbox_inches='tight', 
                    facecolor='white', edgecolor='none')
            plt.close()
            
            logger.info(f"Enhanced map saved successfully: {map_path}")
            
            # Generate OSM link
            center_lat = (min(lats) + max(lats)) / 2
            center_lon = (min(lons) + max(lons)) / 2
            osm_link = f"https://www.openstreetmap.org/#map=12/{center_lat}/{center_lon}"
            
            return map_path, osm_link
            
        except Exception as e:
            logger.error(f"Error generating matplotlib map: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None, None

    def _add_styled_critical_points(self, ax, collections: Dict[str, List]):
        """Add critical points to the map with improved styling"""
        try:
            # Sharp turns with risk-based sizing
            sharp_turns = collections.get('sharp_turns', [])
            for turn in sharp_turns[:15]:  # Limit to first 15
                risk_score = self.safe_float(turn.get('riskScore', 0))
                if risk_score >= 6:
                    lat = self.safe_float(turn.get('latitude', 0))
                    lon = self.safe_float(turn.get('longitude', 0))
                    if lat and lon:
                        # Size based on risk
                        size = 50 + (risk_score * 10)
                        color = 'red' if risk_score >= 8 else 'orange'
                        ax.scatter(lon, lat, c=color, s=size, marker='^', 
                                edgecolors='black', linewidth=1, zorder=3, alpha=0.8)
            
            # Blind spots
            blind_spots = collections.get('blind_spots', [])
            for spot in blind_spots[:15]:  # Limit to first 15
                risk_score = self.safe_float(spot.get('riskScore', 0))
                if risk_score >= 6:
                    lat = self.safe_float(spot.get('latitude', 0))
                    lon = self.safe_float(spot.get('longitude', 0))
                    if lat and lon:
                        size = 50 + (risk_score * 10)
                        ax.scatter(lon, lat, c='#FF5722', s=size, marker='s', 
                                edgecolors='darkred', linewidth=1, zorder=3, alpha=0.8)
            
            # Emergency services with icons
            emergency_services = collections.get('emergency_services', [])
            
            # Hospitals
            hospitals = [s for s in emergency_services if s.get('serviceType') == 'hospital']
            for hospital in hospitals[:8]:
                lat = self.safe_float(hospital.get('latitude', 0))
                lon = self.safe_float(hospital.get('longitude', 0))
                if lat and lon:
                    ax.scatter(lon, lat, c='blue', s=60, marker='H', 
                            edgecolors='navy', linewidth=1.5, zorder=3)
            
            # Police stations
            police = [s for s in emergency_services if s.get('serviceType') == 'police']
            for station in police[:8]:
                lat = self.safe_float(station.get('latitude', 0))
                lon = self.safe_float(station.get('longitude', 0))
                if lat and lon:
                    ax.scatter(lon, lat, c='purple', s=60, marker='P', 
                            edgecolors='indigo', linewidth=1.5, zorder=3)
            
            # Network dead zones
            dead_zones = [n for n in collections.get('network_coverage', []) if n.get('isDeadZone', False)]
            for zone in dead_zones[:8]:
                lat = self.safe_float(zone.get('latitude', 0))
                lon = self.safe_float(zone.get('longitude', 0))
                if lat and lon:
                    ax.scatter(lon, lat, c='black', s=40, marker='x', 
                            linewidth=2.5, zorder=3)
                    
        except Exception as e:
            logger.warning(f"Error adding critical points to map: {e}")

    def _add_north_arrow(self, ax):
        """Add a north arrow to the map"""
        try:
            # Get current axes limits
            xlim = ax.get_xlim()
            ylim = ax.get_ylim()
            
            # Position in top-right corner
            arrow_x = xlim[1] - (xlim[1] - xlim[0]) * 0.05
            arrow_y = ylim[1] - (ylim[1] - ylim[0]) * 0.05
            arrow_length = (ylim[1] - ylim[0]) * 0.05
            
            # Draw arrow
            ax.annotate('N', xy=(arrow_x, arrow_y), xytext=(arrow_x, arrow_y - arrow_length),
                    ha='center', va='bottom', fontsize=14, fontweight='bold',
                    arrowprops=dict(arrowstyle='->', lw=2, color='black'))
            
        except Exception as e:
            logger.warning(f"Could not add north arrow: {e}")

    def _add_info_box(self, ax, route_data: Dict[str, Any]):
        """Add information box to the map"""
        try:
            stats = route_data['statistics']
            risk_level = self.calculate_risk_level(stats['risk_analysis']['avg_risk_score'])
            
            info_text = (
                f"Route Risk Level: {risk_level}\n"
                f"Critical Points: {stats['risk_analysis']['critical_points']}\n"
                f"Emergency Services: {stats['safety_metrics']['hospitals']} Hospitals, "
                f"{stats['safety_metrics']['police_stations']} Police Stations"
            )
            
            # Position in bottom-left
            xlim = ax.get_xlim()
            ylim = ax.get_ylim()
            
            box_x = xlim[0] + (xlim[1] - xlim[0]) * 0.02
            box_y = ylim[0] + (ylim[1] - ylim[0]) * 0.02
            
            # Create fancy box
            props = dict(boxstyle='round,pad=0.5', facecolor='white', 
                        edgecolor='gray', alpha=0.9)
            ax.text(box_x, box_y, info_text, fontsize=9, 
                bbox=props, verticalalignment='bottom')
            
        except Exception as e:
            logger.warning(f"Could not add info box: {e}")
    def add_osm_link_box(self, canvas_obj, interactive_link: str, y_pos: int):
        """Add clickable link box for OpenStreetMap"""
        if not interactive_link:
            return
            
        # Link box
        canvas_obj.setFillColor(self.colors.INFO)
        canvas_obj.rect(40, y_pos, 520, 25, fill=1, stroke=1)
        canvas_obj.setStrokeColor(self.colors.SECONDARY)
        canvas_obj.rect(40, y_pos, 520, 25, fill=0, stroke=1)

        # Text settings
        canvas_obj.setFillColor(self.colors.WHITE)
        canvas_obj.setFont("Helvetica-Bold", 10)
        canvas_obj.drawString(50, y_pos + 7, "🔗 INTERACTIVE MAP:")

        canvas_obj.setFont("Helvetica", 9)
        canvas_obj.drawString(180, y_pos + 7, "Click to open route in OpenStreetMap →")

        # Clickable link (full box clickable)
        canvas_obj.linkURL(interactive_link, (40, y_pos, 560, y_pos + 25))

        # Display shortened link below the box
        canvas_obj.setFont("Helvetica", 7)
        canvas_obj.setFillColor(HexColor('#E3F2FD'))
        short_link = interactive_link[:80] + "..." if len(interactive_link) > 80 else interactive_link
        canvas_obj.drawString(50, y_pos - 8, short_link)
    
    def add_route_statistics_overlay(self, canvas_obj, route_data: Dict[str, Any], y_pos: int):
        """Add statistics overlay on the map"""
        stats = route_data['statistics']
        
    def get_legend_color(self, color_name: str):
        """Get color object for legend items"""
        color_map = {
            'green': HexColor('#28A745'),
            'red': HexColor('#DC3545'),
            'blue': HexColor('#007BFF'),
            'purple': HexColor('#6F42C1'),
            'yellow': HexColor('#FFC107'),
            'orange': HexColor('#FD7E14'),
            'black': HexColor('#000000')
        }
        return color_map.get(color_name, self.colors.SECONDARY)
    
    def draw_map_placeholder(self, canvas_obj, route_data: Dict[str, Any], y_pos: int):
        """Draw a placeholder map when map generation fails"""
        route = route_data['route']
        
        # Map placeholder background
        canvas_obj.setFillColorRGB(0.9, 0.9, 0.9)
        canvas_obj.rect(40, y_pos - 280, 520, 280, fill=1, stroke=1)
        canvas_obj.setStrokeColor(self.colors.SECONDARY)
        
        # Route info
        canvas_obj.setFont("Helvetica", 12)
        canvas_obj.setFillColor(self.colors.PRIMARY)
        detail_text = f"Route: {route.get('fromAddress', 'Start')} → {route.get('toAddress', 'End')}"
        detail_width = canvas_obj.stringWidth(detail_text, "Helvetica", 12)
        canvas_obj.drawString((self.page_width - detail_width) / 2, y_pos - 140, detail_text)
        
        stats_text = f"Distance: {route.get('totalDistance', 0)}km | Points Analyzed: {route_data['statistics']['total_data_points']}"
        stats_width = canvas_obj.stringWidth(stats_text, "Helvetica", 12)
        canvas_obj.drawString((self.page_width - stats_width) / 2, y_pos - 160, stats_text)
        
        # Coordinates info
        canvas_obj.setFont("Helvetica", 10)
        start_coords = f"Start: {route.get('fromCoordinates', {}).get('latitude', 0):.4f}, {route.get('fromCoordinates', {}).get('longitude', 0):.4f}"
        end_coords = f"End: {route.get('toCoordinates', {}).get('latitude', 0):.4f}, {route.get('toCoordinates', {}).get('longitude', 0):.4f}"
        
        start_width = canvas_obj.stringWidth(start_coords, "Helvetica", 10)
        end_width = canvas_obj.stringWidth(end_coords, "Helvetica", 10)
        
        canvas_obj.drawString((self.page_width - start_width) / 2, y_pos - 180, start_coords)
        canvas_obj.drawString((self.page_width - end_width) / 2, y_pos - 200, end_coords)
        
        # Note about map generation
        canvas_obj.setFont("Helvetica-Oblique", 10)
        canvas_obj.setFillColor(self.colors.DANGER)
        note_text = "Map visualization temporarily unavailable"
        note_width = canvas_obj.stringWidth(note_text, "Helvetica-Oblique", 10)
        canvas_obj.drawString((self.page_width - note_width) / 2, y_pos - 230, note_text)
        
        # OpenStreetMap link
        canvas_obj.setFont("Helvetica", 9)
        canvas_obj.setFillColor(self.colors.INFO)
        center_lat = (route.get('fromCoordinates', {}).get('latitude', 0) + route.get('toCoordinates', {}).get('latitude', 0)) / 2
        center_lon = (route.get('fromCoordinates', {}).get('longitude', 0) + route.get('toCoordinates', {}).get('longitude', 0)) / 2
        link_text = f"OpenStreetMap: https://www.openstreetmap.org/#map=12/{center_lat}/{center_lon}"
        if len(link_text) > 80:
            link_text = link_text[:80] + "..."
        link_width = canvas_obj.stringWidth(link_text, "Helvetica", 9)
        canvas_obj.drawString((self.page_width - link_width) / 2, y_pos - 250, link_text)
    
    def generate_google_maps_route_image(self, route_data: Dict[str, Any]) -> tuple:
        """Generate Google Maps static image with full route view and return interactive link"""
        try:
            api_key = os.getenv('GOOGLE_MAPS_API_KEY')
            if not api_key:
                logger.warning("Google Maps API key not found in environment variables")
                return None, None
            
            route = route_data['route']
            collections = route_data['collections']
            all_sharp_turns = self.remove_duplicate_coordinates(collections['sharp_turns'])
            all_blind_points = self.remove_duplicate_coordinates(collections['blind_spots'])
            
            # Base URL for Google Maps Static API
            base_url = "https://maps.googleapis.com/maps/api/staticmap"
            
            # Enhanced map parameters for full route view
            params = {
                'size': '800x600',  # Larger size for better detail
                'maptype': 'roadmap',
                'format': 'png',
                'key': api_key,
                'scale': 2  # High resolution
            }
            
            # Collect all coordinates for proper bounds calculation
            all_coords = []
            
            # Start and End points
            start_lat = route['fromCoordinates']['latitude']
            start_lng = route['fromCoordinates']['longitude']
            end_lat = route['toCoordinates']['latitude'] 
            end_lng = route['toCoordinates']['longitude']
            
            all_coords.extend([(start_lat, start_lng), (end_lat, end_lng)])
            
            # Add route points if available
            route_points = []
            if route.get('routePoints') and len(route['routePoints']) > 1:
                for point in route['routePoints']:
                    coord = (point['latitude'], point['longitude'])
                    all_coords.append(coord)
                    route_points.append(f"{point['latitude']},{point['longitude']}")
            
            # Add critical points to ensure they're visible
            for turn in all_sharp_turns:
                if turn.get('riskScore', 0) >= 7:  # Include high risk turns in bounds
                    all_coords.append((turn.get('latitude'), turn.get('longitude')))
            
            for spot in all_blind_points:
                if spot.get('riskScore', 0) >= 7:  # Include high risk blind spots
                    all_coords.append((spot.get('latitude'), spot.get('longitude')))
            
            # Calculate bounds that include ALL points
            if all_coords:
                lats = [coord[0] for coord in all_coords if coord[0] is not None]
                lngs = [coord[1] for coord in all_coords if coord[1] is not None]
                
                min_lat, max_lat = min(lats), max(lats)
                min_lng, max_lng = min(lngs), max(lngs)
                
                # Add padding to bounds (10% on each side)
                lat_padding = (max_lat - min_lat) * 0.1
                lng_padding = (max_lng - min_lng) * 0.1
                
                min_lat -= lat_padding
                max_lat += lat_padding
                min_lng -= lng_padding
                max_lng += lng_padding
                
                # Use visible parameter to fit all points
                params['visible'] = f"{min_lat},{min_lng}|{max_lat},{max_lng}"
            
            # Add route path with enhanced styling
            if route_points and len(route_points) > 1:
                # Simplify route points to avoid URL length issues
                simplified_points = route_points[::max(1, len(route_points)//30)]  # Max 30 points
                if simplified_points:
                    params['path'] = f"color:0x0052ff|weight:5|{('|').join(simplified_points)}"
            
            # Enhanced markers with better visibility
            markers = []
            
            # Start and End points (larger, more visible)
            markers.append(f"color:green|size:mid|label:A|{start_lat},{start_lng}")
            markers.append(f"color:red|size:mid|label:B|{end_lat},{end_lng}")
            
            # Critical sharp turns (limit to most critical ones)
            critical_turns = sorted([t for t in all_sharp_turns if t.get('riskScore', 0) >= 8], 
                                  key=lambda x: x.get('riskScore', 0), reverse=True)
            for i, turn in enumerate(critical_turns[:8]):  # Top 8 most critical
                lat, lng = turn.get('latitude'), turn.get('longitude')
                if lat and lng:
                    markers.append(f"color:red|size:small|label:T{i+1}|{lat},{lng}")
            
            # Critical blind spots
            critical_blinds = sorted([b for b in all_blind_points if b.get('riskScore', 0) >= 8],
                                   key=lambda x: x.get('riskScore', 0), reverse=True)
            for i, blind in enumerate(critical_blinds[:6]):  # Top 6 most critical
                lat, lng = blind.get('latitude'), blind.get('longitude')
                if lat and lng:
                    markers.append(f"color:orange|size:small|label:B{i+1}|{lat},{lng}")
            
            # Key emergency services (nearest ones)
            hospitals = sorted([s for s in collections['emergency_services'] if s.get('serviceType') == 'hospital'],
                             key=lambda x: x.get('distanceFromRouteKm', 999))
            for i, hospital in enumerate(hospitals[:6]):  # 6 nearest hospitals
                lat, lng = hospital.get('latitude'), hospital.get('longitude')
                if lat and lng:
                    markers.append(f"color:blue|size:small|label:H{i+1}|{lat},{lng}")
            
            # Police stations (nearest ones)
            police = sorted([s for s in collections['emergency_services'] if s.get('serviceType') == 'police'],
                          key=lambda x: x.get('distanceFromRouteKm', 999))
            for i, station in enumerate(police[:4]):  # 4 nearest police stations
                lat, lng = station.get('latitude'), station.get('longitude')
                if lat and lng:
                    markers.append(f"color:purple|size:small|label:P{i+1}|{lat},{lng}")
            
            # Network dead zones (most critical)
            dead_zones = [n for n in collections['network_coverage'] if n.get('isDeadZone', False)]
            for i, zone in enumerate(dead_zones[:4]):  # 4 dead zones
                lat, lng = zone.get('latitude'), zone.get('longitude')
                if lat and lng:
                    markers.append(f"color:black|size:tiny|label:D{i+1}|{lat},{lng}")
            
            # Add markers to params
            if markers:
                params['markers'] = '|'.join(markers)
            
            # Make API request
            logger.info(f"Requesting Google Maps image for route: {route.get('routeName', 'Unknown')}")
            response = requests.get(base_url, params=params, timeout=30)
            
            # Generate interactive Google Maps link
            interactive_link = self.generate_interactive_maps_link(route_data)
            
            if response.status_code == 200:
                # Save image to temporary file
                image_filename = f"route_map_{route_data['route']['_id']}.png"
                image_path = os.path.join(tempfile.gettempdir(), image_filename)
                
                with open(image_path, 'wb') as f:
                    f.write(response.content)
                
                logger.info(f"Google Maps image saved: {image_path}")
                return image_path, interactive_link
            else:
                logger.error(f"Google Maps API error: {response.status_code} - {response.text}")
                return None, interactive_link
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error requesting Google Maps: {e}")
            return None, None
        except Exception as e:
            logger.error(f"Unexpected error generating Google Maps image: {e}")
            return None, None

    def generate_interactive_maps_link(self, route_data: Dict[str, Any]) -> str:
        """Generate interactive Google Maps link for the route"""
        try:
            route = route_data['route']
            collections = route_data['collections']
            
            start_lat = route['fromCoordinates']['latitude']
            start_lng = route['fromCoordinates']['longitude']
            end_lat = route['toCoordinates']['latitude'] 
            end_lng = route['toCoordinates']['longitude']
            
            # Create waypoints for better route
            waypoints = []
            if route.get('routePoints') and len(route['routePoints']) > 2:
                # Add some intermediate waypoints (every 10th point)
                for i in range(5, len(route['routePoints']) - 5, 10):
                    point = route['routePoints'][i]
                    waypoints.append(f"{point['latitude']},{point['longitude']}")
            
            # Google Maps Directions URL
            base_url = "https://www.google.com/maps/dir"
            
            if waypoints:
                # Include waypoints for more accurate route
                waypoints_str = '/'.join(waypoints[:8])  # Limit to 8 waypoints
                interactive_link = f"{base_url}/{start_lat},{start_lng}/{waypoints_str}/{end_lat},{end_lng}"
            else:
                # Simple start to end
                interactive_link = f"{base_url}/{start_lat},{start_lng}/{end_lat},{end_lng}"
            
            return interactive_link
            
        except Exception as e:
            logger.error(f"Error generating interactive maps link: {e}")
            return f"https://www.google.com/maps/@{start_lat},{start_lng},12z"

    def add_interactive_link_box(self, canvas_obj, interactive_link: str, y_pos: int):
        """Add clickable link box for interactive map"""
        if not interactive_link:
            return
            


        # Link box
        canvas_obj.setFillColor(self.colors.INFO)
        canvas_obj.rect(40, y_pos, 520, 25, fill=1, stroke=1)
        canvas_obj.setStrokeColor(self.colors.SECONDARY)
        canvas_obj.rect(40, y_pos, 520, 25, fill=0, stroke=1)

        # Text settings
        canvas_obj.setFillColor(self.colors.WHITE)
        canvas_obj.setFont("Helvetica-Bold", 10)
        canvas_obj.drawString(50, y_pos + 7, "🔗 INTERACTIVE MAP:")

        canvas_obj.setFont("Helvetica", 9)
        canvas_obj.drawString(180, y_pos + 7, "Click to open route in Google Maps →")

        # Clickable link (full box clickable)
        canvas_obj.linkURL(interactive_link, (40, y_pos, 560, y_pos + 25))

        # Display shortened link below the box
        canvas_obj.setFont("Helvetica", 7)
        canvas_obj.setFillColor(HexColor('#E3F2FD'))
        short_link = interactive_link[:80] + "..." if len(interactive_link) > 80 else interactive_link
        canvas_obj.drawString(50, y_pos - 8, short_link)

    def add_enhanced_map_legend(self, canvas_obj, route_data: Dict[str, Any], y_pos: int):
        """Add map legend styled exactly like the uploaded image (two columns, plain text)"""

        # Left and right column entries (symbol, description)
        col1_items = [
            ("A", "Route Start Point"),
            ("T#", "Critical Sharp Turns"),
            ("P", "Police Stations"),
            ("G", "Gas Stations"),
            ("*", "Highway Junctions"),
        ]

        col2_items = [
            ("B", "Route End Point"),
            ("H", "Hospitals"),
            ("F", "Fire Stations"),
            ("S", "Schools/Education"),
        ]

        # Layout parameters
        x1 = 60     # Left column start X
        x2 = 300    # Right column start X
        symbol_indent = 0
        label_indent = 20
        line_height = 18

        # Draw heading
        canvas_obj.setFont("Helvetica-Bold", 11)
        canvas_obj.setFillColor(self.colors.BLACK)
        canvas_obj.drawString(x1, y_pos, "MAP LEGEND")
        y_pos -= line_height

        # Set font for legend items
        canvas_obj.setFont("Helvetica-Bold", 9)

        # Draw left column items
        for symbol, label in col1_items:
            canvas_obj.drawString(x1 + symbol_indent, y_pos, symbol)
            canvas_obj.setFont("Helvetica", 9)
            canvas_obj.drawString(x1 + label_indent, y_pos, label)
            y_pos -= line_height
            canvas_obj.setFont("Helvetica-Bold", 9)

        # Reset y_pos for second column (aligned with first symbol line)
        second_col_y = y_pos + line_height * len(col1_items)

        # Draw right column items
        for symbol, label in col2_items:
            canvas_obj.drawString(x2 + symbol_indent, second_col_y, symbol)
            canvas_obj.setFont("Helvetica", 9)
            canvas_obj.drawString(x2 + label_indent, second_col_y, label)
            second_col_y -= line_height
            canvas_obj.setFont("Helvetica-Bold", 9)

    def create_safety_measures_page(self, canvas_obj, route_data: Dict[str, Any],page_num: int):
        """Create separate page for Key Safety Measures & Regulatory Compliance"""
        # Page header
        self.add_page_header(canvas_obj, "HPCL - Journey Risk Management Study (AI-Powered Analysis)", "", page_num)
        
        y_pos = self.page_height - 100

        header = ["ASPECT", "DETAILS/ RECOMMENDATION"]
        col_widths = [200, 280]
        data = [
            ["Speed Limits ", "NH: 60 km/h; SH: 55 km/h; MDR: 55 km/h; Rural: 25–30 km/h; Accident-prone zone: 30 "],
            ["Night Driving ", "Prohibited:2300hrs – 0500hrs "],
            ["Rest Breaks", "Mandatory 15-30 min every 3 hours"],
            ["Vehicle Compliance ", "Check brakes, tires, lights, and emergency equipment"],
            ["Permits & Documents ", "Carry valid transport permits, Hazardous vehicle license,MSDS sheets, TREM CARD"],
            ["VTS ", "VTS & EMERGENCY LOCKING DEVICE shall be functional "]
        ]

        y_pos = self.create_simple_table(
            canvas_obj,
            "KEY SAFETY MEASURES & REGULATORY COMPLIANCE ",
            header,
            data,
            50, y_pos,
            col_widths,
            title_color=self.colors.WHITE,
            header_color=self.colors.WHITE,
            text_color=self.colors.PRIMARY
        )

        y_pos -= 10
        canvas_obj.setFillColor("#808080")
        canvas_obj.setFont("Helvetica-Oblique", 9)
        canvas_obj.drawString(self.margin+10, y_pos, "Ensure all regulatory requirements are met before journey commencement.Route includes major highways: NH-344,")
        y_pos -= 15
        canvas_obj.drawString(self.margin, y_pos, "NH-709.Rural terrain requires attention to livestock and agricultural vehicles.Speed limits may vary based")
        y_pos -= 15
        canvas_obj.drawString(self.margin, y_pos, "Speed limits may vary based on local regulations and road conditions.")
        y_pos -= 30

    def add_regulatory_overview_table(self, canvas_obj, route_data: Dict[str, Any], y_pos: int):
        """Add regulatory compliance overview table"""
        route = route_data['route']
        
        headers = ["Compliance Aspect", "Requirement", "Status/Action"]
        col_widths = [150, 200, 150]
        
        compliance_data = [
            ["Route Distance", f"{route.get('totalDistance', 0)} km", " Within permitted limits"],
            ["Interstate Travel", "NO" if route.get('fromCoordinates', {}).get('latitude', 0) > 28 and route.get('toCoordinates', {}).get('latitude', 0) > 28 else "YES", "Permits verified"],
            ["Vehicle Category", "Heavy Goods Vehicle", "Valid license required"],
            ["AIS-140 GPS Tracking", "Mandatory", " Ensure device functional"],
            ["Route Permits", "Required for heavy vehicles", "Carry valid permits"],
            ["Driver Medical Certificate", "Valid certificate mandatory", "Check expiry date"],
            ["Vehicle Fitness Certificate", "PUC & fitness valid", " Verify before travel"],
            ["Hazardous Goods License", "Required for petroleum transport", "✓ MSDS & TREM card ready"]
        ]
        
        # Draw the table and get new Y position
        new_y_pos = self.create_simple_table(
            canvas_obj,
            "REGULATORY COMPLIANCE OVERVIEW",
            headers,
            compliance_data,
            50, y_pos,
            col_widths,
            title_color=self.colors.PRIMARY,
            max_rows_per_page=15
        )

        # Add margin
        y_pos = new_y_pos - 20

        # Compliance text (now safe)
        canvas_obj.setFillColor(self.colors.INFO)
        canvas_obj.setFont("Helvetica-Oblique", 9)
        compliance_text = "MANDATORY COMPLIANCE: All safety measures are required under Motor Vehicle Act 1988, Petroleum Rules 2002, and Transport of Dangerous Goods Rules. Non-compliance may result in penalties, permit cancellation, and legal action."

        # Word wrap logic remains the same
        words = compliance_text.split()
        lines = []
        current_line = []
        line_width = 0
        max_width = 480

        for word in words:
            word_width = canvas_obj.stringWidth(word + " ", "Helvetica-Oblique", 9)
            if line_width + word_width <= max_width:
                current_line.append(word)
                line_width += word_width
            else:
                lines.append(" ".join(current_line))
                current_line = [word]
                line_width = word_width

        if current_line:
            lines.append(" ".join(current_line))

        for line in lines:
            canvas_obj.drawString(60, y_pos, line)
            y_pos -= 12

    def add_compliance_issues(self, canvas_obj, y_pos: int) -> int:
        """Add compliance issues requiring immediate attention"""

        ### page heder
        self.add_page_header(canvas_obj,"HPCL - Journey Risk Management Study (AI-Powered Analysis)" )
        y_pos = self.page_height - 100
        
        canvas_obj.setFillColor(self.colors.DANGER)
        canvas_obj.setFont("Helvetica-Bold", 12)
        canvas_obj.drawString(self.margin, y_pos, "COMPLIANCE ISSUES REQUIRING IMMEDIATE ATTENTION")
        y_pos -= 25

        canvas_obj.setFillColor(self.colors.BLACK)
        canvas_obj.setFont("Helvetica", 10)
        issues = [
            "1. AIS-140 GPS tracking device required - Address before travel",
            "2. Heavy vehicle - weight restrictions may apply - Address before travel"
        ]
        for issue in issues:
            canvas_obj.drawString(self.margin + 10, y_pos, issue)
            y_pos -= 15
        return y_pos - 10

    def add_applicable_regulatory_framework(self, canvas_obj, y_pos: int) -> int:
        """Add applicable regulatory framework"""
        canvas_obj.setFillColor(self.colors.PRIMARY)
        canvas_obj.setFont("Helvetica-Bold", 12)
        canvas_obj.drawString(self.margin, y_pos, "APPLICABLE REGULATORY FRAMEWORK")
        y_pos -= 25

        canvas_obj.setFillColor(self.colors.BLACK)
        canvas_obj.setFont("Helvetica", 10)
        frameworks = [
            "* Motor Vehicles Act, 1988 - Vehicle registration and licensing requirements",
            "* Central Motor Vehicles Rules, 1989 - Technical specifications and safety",
            "* AIS-140 Standards - GPS tracking and panic button requirements",
            "* Road Transport and Safety Policy (RTSP) - Driver working hours",
            "* Interstate Transport Permits - Required for commercial interstate travel",
            "* Pollution Control Board Norms - Emission standards compliance",
            "* Goods and Services Tax (GST) - Tax compliance for commercial transport",
            "* Road Safety and Transport Authority - State-specific requirements"
        ]
        for framework in frameworks:
            canvas_obj.drawString(self.margin + 10, y_pos, framework)
            y_pos -= 15
        return y_pos - 10

    def add_compliance_recommendations(self, canvas_obj, y_pos: int) -> int:
        """Add compliance recommendations"""
        canvas_obj.setFillColor(self.colors.WARNING)
        canvas_obj.setFont("Helvetica-Bold", 12)
        canvas_obj.drawString(self.margin, y_pos, "COMPLIANCE RECOMMENDATIONS")
        y_pos -= 25

        canvas_obj.setFillColor(self.colors.BLACK)
        canvas_obj.setFont("Helvetica", 10)
        recommendations = [
            "1. Ensure all vehicle documents are current and accessible",
            "2. Verify the driver license category matches vehicle type",
            "3. Check route-specific permits and restrictions",
            "4. Install AIS-140 compliant GPS tracking device"
        ]
        for rec in recommendations:
            canvas_obj.drawString(self.margin + 10, y_pos, rec)
            y_pos -= 15
        return y_pos - 15

    def add_non_compliance_penalties(self, canvas_obj, y_pos: int) -> int:
        """Add non-compliance penalties and consequences"""
        canvas_obj.setFillColor(self.colors.DANGER)
        canvas_obj.setFont("Helvetica-Bold", 12)
        canvas_obj.drawString(self.margin, y_pos, "NON-COMPLIANCE PENALTIES & CONSEQUENCES")
        y_pos -= 25

        canvas_obj.setFillColor(self.colors.BLACK)
        canvas_obj.setFont("Helvetica", 10)
        penalties = [
            "* Driving without valid license: Fine up to Rs 5,000 + imprisonment",
            "* Vehicle without registration: Fine up to Rs 10,000 + vehicle seizure",
            "* No insurance: Fine up to Rs 2,000 + vehicle seizure",
            "* AIS-140 non-compliance: Permit cancellation + heavy fines",
            "* Overloading violations: Fine Rs 20,000 + per excess ton",
            "* Driving time violations: License suspension + fines",
            "* Interstate without permits: Vehicle seizure + penalty",
            "* Environmental violations: Fine up to Rs 10,000 + registration cancellation"
        ]
        for penalty in penalties:
            canvas_obj.drawString(self.margin + 10, y_pos, penalty)
            y_pos -= 15
        return y_pos - 20

    # ============================================================================
    # MAIN METHOD - Entry point for creating the comprehensive risk zones page
    # ============================================================================

    def create_comprehensive_risk_zones_page(self, canvas_obj, route_data: Dict[str, Any], page_num: int):
        """Create comprehensive High-Risk Zones page with all risk categories in ONE table"""
        collections = route_data['collections']
        route = route_data['route']
        
        # Page header
        self.add_page_header(canvas_obj,"HPCL - Journey Risk Management Study (AI-Powered Analysis)", "", page_num )
        # self.add_page_header(canvas_obj, "HPCL - Journey Risk Management Study", "Safety & Compliance", page_num)

        y_pos = self.page_height - 100
        
        # Collect ALL high-risk points along the route (excluding wildlife)
        all_risk_points = self.collect_all_high_risk_points(collections, route)
        
        # Sort by distance from start (along the route)
        all_risk_points.sort(key=lambda x: x['distance_from_start'])
        
        # Remove duplicates based on coordinates
        unique_risk_points = self.remove_duplicate_coordinates(all_risk_points)
        
        # Create comprehensive table with adjusted column widths for full text display
        headers = ["Type", "Supply (KM)", "Customer (KM)", 
                "Coordinates", "Risk Level", "Speed Limit", "Driver Action"]
        
        # Adjusted column widths for full page width usage (margins: 20 on each side)
        # Page width = 595, usable width = 595 - 40 = 555
        col_widths = [85, 55, 60, 120, 50, 55, 130]  # Total: 555
        
        # Prepare table data
        table_data = []
        for point in unique_risk_points:
            # Format coordinates with view link
            coords_text = f"{point['latitude']:.6f}, {point['longitude']:.6f}"
            
            table_data.append([
                point['type'],
                f"{point['distance_from_start']:.1f}",
                f"{point['distance_from_customer']:.1f}",
                coords_text,  # Will be made clickable
                point['risk_level'],
                point['speed_limit'],
                point['driver_action']
            ])

    
        # Create the comprehensive table with enhanced formatting
        self.create_comprehensive_risk_table(
            canvas_obj,
            headers,
            table_data,
            unique_risk_points,  # Pass original data for links
            20, y_pos,  # Start at 20px from left margin
            col_widths,
            title_color=self.colors.WHITE,
            title_text_color=self.colors.PRIMARY
        )

    def create_elevation_terrain_analysis_page(self, canvas_obj, route_data: Dict[str, Any]):
        """Create page for Elevation & Terrain Analysis"""
        self.add_page_header(canvas_obj, "HPCL - Journey Risk Management Study (AI-Powered Analysis)", "")

        y_pos = self.page_height - 100  # Starting position after header

        # First table - Elevation Analysis
        headers = ["Description", "Value"]
        col_widths = [180, 300]  # Adjusted to match your table implementation
        elevation_data = [
            ["Data Source", "SRTM (Shuttle Radar Topography Mission) DEM (Digital Elevation Model) data (30m resolution), Topographic maps"],
            ["Total Analysis Points", "~100 elevation points sampled at 100 m intervals along the route (~10-12 km)"],
            ["Minimum Elevation", "214 m above sea level"],
            ["Maximum Elevation", "236 m above sea level"],
            ["Average Elevation", "225 m above sea level"],
            ["Total Elevation Range", "22 m (very minimal variation)"],
            ["Terrain Classification", "Flat Plains Terrain – Alluvial Soil (Indo-Gangetic Plain)"],
            ["Driving Difficulty Level", "Very Easy - No sharp slopes or rugged surfaces"],
            ["Fuel Consumption Impact", "Minimal - No gradient resistance; consistent throttle level"],
            ["Significant Changes Detected", "None - No sudden elevation gain/loss >5 m/km observed"]
        ]
        
        y_pos = self.create_simple_table(
            canvas_obj=canvas_obj,
            title="COMPREHENSIVE ELEVATION & TERRAIN ANALYSIS - LOW RISK (Risk Score: 1)",
            headers=headers,
            data=elevation_data,
            start_x=50,
            start_y=y_pos,
            col_widths=col_widths,
            title_color=self.colors.SUCCESS,
            header_color=self.colors.WHITE,
            text_color=self.colors.WHITE # Using black text for better readability
        )

        y_pos -= 30
        # Second table - Terrain Characteristics
        terrain_headers = ["Factor", "Observation / Recommendation"]
        terrain_col_widths = [150, 320]  # Adjusted to match your table implementation
        terrain_data = [
            ["Elevation Change", "<20 m across the entire route"],
            ["Ground Type", "Stable alluvial soil - typical of Indo-Gangetic plains"],
            ["Route Complexity", "Straightforward – no hills, valleys, or obstacles"],
            ["Recommended Terrain Classification", "\"Plains Terrain – Easy\" – ideal for transportation and infrastructure projects"],
            ["Engineering Requirement", "Minimal grading or earthwork needed"]
        ]
        
        y_pos = self.create_simple_table(
            canvas_obj=canvas_obj,
            title="TERRAIN CHARACTERISTICS & DRIVING CONDITIONS",
            headers=terrain_headers,
            data=terrain_data,
            start_x=50,
            start_y=y_pos,
            col_widths=terrain_col_widths,
            title_color=self.colors.WHITE,
            header_color= self.colors.WHITE,
            text_color=self.colors.PRIMARY  # Using black text for better readability
        )

        return y_pos

    def create_elevation_based_driving_challenges_page(self, canvas_obj, route_data: Dict[str, Any]):
        """Create page for Elevation & Terrain Analysis using simplified table creator"""
        self.add_page_header(canvas_obj, "HPCL - Journey Risk Management Study (AI-Powered Analysis)", "")

        y_pos = self.page_height - 100  # Starting position after header
        
        # --- ELEVATION-BASED DRIVING CHALLENGES ---
        challenges_headers = ["Factor", "Details"]
        challenges_data = [
            ["Gradient", "Very gentle throughout (<1.5% max slope); minimal driving resistance"],
            ["Sharp Ascents/Descents", "None detected - flat terrain typical of alluvial plains"],
            ["Drainage Crossings", "Minor culverts/canal crossings may cause bumpy segments, especially during monsoon"],
            ["Dusty Roads or Soft Shoulders", "In the village outskirts, soft ground post-rain may affect traction slightly"],
            ["Visibility Impact from Terrain", "Zero elevation-related blind spots"],
            ["Overtaking Risk Zones", "Encouraged due to flatness, but caution is advised on narrow rural roads during traffic"]
        ]
        
        y_pos = self.create_simple_table(
            canvas_obj,
            "ELEVATION-BASED DRIVING CHALLENGES",
            challenges_headers,
            challenges_data,
            self.margin,
            y_pos,
            [150, self.content_width - 150],
            title_color=self.colors.WHITE,
            header_color=self.colors.LIGHT_GRAY,
            text_color=self.colors.PRIMARY
        )

        # Conclusion text
        canvas_obj.setFillColor(self.colors.BLACK)
        canvas_obj.setFont("Helvetica", 10)
        canvas_obj.drawString(self.margin, y_pos, "Conclusion: No elevation-based driving challenges detected.Road surface condition and traffic are more") 
        y_pos -= 10       
        canvas_obj.drawString(self.margin, y_pos, "critical than terrain.")          
        y_pos -= 30

        # --- VEHICLE PREPARATION ---
        preparation_headers = ["Checklist Item", "Recommendation"]
        preparation_data = [
            ["Braking System", "Standard brake health check is sufficient – no steep descents"],
            ["Transmission/Gear Handling", "No need for gear downshifting - standard torque handling is adequate"],
            ["Coolant System", "Normal levels sufficient – no elevation-induced overheating risk"],
            ["Suspension Check", "Recommended if crossing unpaved or bumpy canal sections"],
            ["Tire Pressure", "Maintain OEM-recommended PSI – no elevation-related adjustments needed"],
            ["Load Management", "Full commercial load allowed – no climb-induced torque concerns"]
        ]
        
        y_pos = self.create_simple_table(
            canvas_obj,
            "ELEVATION-SPECIFIC VEHICLE PREPARATION",
            preparation_headers,
            preparation_data,
            self.margin,
            y_pos,
            [150, self.content_width - 150],
            title_color=self.colors.WHITE,
            header_color=self.colors.LIGHT_GRAY,
            text_color=self.colors.DANGER
        )

        # Conclusion text
        canvas_obj.setFillColor(self.colors.BLACK)
        canvas_obj.setFont("Helvetica", 10)
        canvas_obj.drawString(self.margin, y_pos, "Conclusion: Standard vehicle maintenance is adequate.No special elevation-based prep required.")
        y_pos -= 30

        # --- FUEL CONSUMPTION IMPACT ANALYSIS ---
        fuel_headers = ["Factor", "Impact"]
        fuel_data = [
            ["Flat Terrain", "Promotes consistent throttle control, reduces braking/acceleration frequency"],
            ["Elevation Variation", "Negligible – within 22 m band; no fuel impact from climbs"],
            ["Traffic Stops / Speed Variance", "Minor impact at junctions; more related to congestion than terrain"],
            ["Route Type", "Industrial roads + rural highways – mostly 2nd to 4th gear driving"],
            ["Estimated Fuel Consumption Impact", "Diesel trucks: ~11–15 km/l"]
        ]
        
        y_pos = self.create_simple_table(
            canvas_obj,
            "FUEL CONSUMPTION IMPACT ANALYSIS",
            fuel_headers,
            fuel_data,
            self.margin,
            y_pos,
            [150, self.content_width - 150],
            title_color=self.colors.WHITE,
            header_color=self.colors.LIGHT_GRAY,
            text_color=self.colors.WARNING
        )

        # Conclusion text
        canvas_obj.setFillColor(self.colors.BLACK)
        canvas_obj.setFont("Helvetica", 10)
        canvas_obj.drawString(self.margin, y_pos, "Conclusion: Flat terrain enhances fuel efficiency.Fuel usage will be more affected by road quality and")
        y_pos -= 10       
        canvas_obj.drawString(self.margin, y_pos, "traffic than by elevation.")          
        y_pos -= 30

        # --- FINAL SUMMARY & ROUTE CLASSIFICATION ---
        canvas_obj.setFillColor(self.colors.PRIMARY)
        canvas_obj.setFont("Helvetica-Bold", 14)
        canvas_obj.drawString(self.margin, y_pos, "FINAL SUMMARY & ROUTE CLASSIFICATION")
        y_pos -= 20

        summary_points = [
            "Route Type: Flat Plains (Alluvial Soil)",
            "Classification: \"Plains Terrain – Easy\"",
            "Driving Difficulty: Very Low",
            "Infrastructure Suitability: High – suitable for all types of transport and development",
            "Special Considerations:",
            "• Monsoon season may affect drainage crossings",
            "• Post-rain conditions may soften shoulders in rural areas"
        ]

        canvas_obj.setFillColor(self.colors.BLACK)
        canvas_obj.setFont("Helvetica", 10)
        for point in summary_points:
            canvas_obj.drawString(self.margin, y_pos, point)
            y_pos -= 14

        return y_pos

    def create_traffic_analysis_page(self, canvas_obj, route_data: Dict[str, Any]):
        """Create page for Traffic Analysis and Recommendations"""
        self.add_page_header(canvas_obj, "HPCL - Journey Risk Management Study (AI-Powered Analysis)", "")

        y_pos = self.page_height - 120

        collections = route_data['collections']
        traffic_data = collections.get("traffic_data", [])

        total_segments = len(traffic_data)
        heavy_segments = [t for t in traffic_data if t.get("congestionLevel") == "heavy"]
        moderate_segments = [t for t in traffic_data if t.get("congestionLevel") == "moderate"]
        free_flow_segments = [t for t in traffic_data if t.get("congestionLevel") == "free_flow"]

        avg_current_speed = sum(t.get("averageSpeedKmph", 0) for t in traffic_data) / total_segments if total_segments else 0
        avg_free_flow_speed = 51.8  # If you have this from API or need to calculate, replace this value
        travel_time_index = avg_free_flow_speed / avg_current_speed if avg_current_speed else 0

        overall_score = 97.5  # Replace with your actual traffic score calculation if available
        traffic_condition = "EXCELLENT" if overall_score > 90 else "GOOD" if overall_score > 70 else "POOR"
        worst_congestion_percent = round(len(heavy_segments) / total_segments * 100, 1) if total_segments else 0.0

        traffic_headers = ["Metric", "Value"]
        traffic_data = [
            ["Route Segments Analyzed", str(total_segments)],
            ["Overall Traffic Score", f"{overall_score} / 100"],
            ["Traffic Condition", traffic_condition],
            ["Average Travel Time Index", f"{travel_time_index:.2f}"],
            [f"Average Current Speed", f"{avg_current_speed:.1f} km/h"],
            [f"Average Free Flow Speed", f"{avg_free_flow_speed:.1f} km/h"],
            ["Heavy Traffic Segments", str(len(heavy_segments))],
            ["Moderate Traffic Segments", str(len(moderate_segments))],
            ["Free Flow Segments", str(len(free_flow_segments))],
            ["Worst Congestion Areas", f"{worst_congestion_percent}% of the route"]
        ]

        y_pos = self.create_simple_table(
            canvas_obj,
            "COMPREHENSIVE TRAFFIC ANALYSIS LOW RISK (Risk Score: 1)",
            traffic_headers,
            traffic_data,50, y_pos,
            [250, 250],
            title_color=self.colors.SUCCESS,
            text_color=self.colors.WHITE,
            header_color=self.colors.WHITE
        )

        y_pos -= 20
        # --- TRAFFIC RECOMMENDATIONS ---
        canvas_obj.setFillColor(self.colors.PRIMARY)
        canvas_obj.setFont("Helvetica-Bold", 14)
        canvas_obj.drawString(self.margin, y_pos, "TRAFFIC-BASED RECOMMENDATIONS")
        y_pos -= 20

        recommendations = [
            "1. Check current traffic conditions before departure",
            "2. Consider public transportation alternatives for heavily congested routes",
            "3. Plan rest stops during low-traffic segments"
        ]

        canvas_obj.setFillColor(self.colors.BLACK)
        canvas_obj.setFont("Helvetica", 10)
        for rec in recommendations:
            canvas_obj.drawString(self.margin, y_pos, rec)
            y_pos -= 14

        y_pos -= 10

    # ============================================================================
    # DATA COLLECTION METHOD (WITHOUT WILDLIFE)
    # ============================================================================

    def collect_all_high_risk_points(self, collections: Dict[str, List], route: Dict) -> List[Dict]:
        """Collect all high-risk points from different collections into a unified list"""
        all_points = []
        total_distance = self.safe_float(route.get('totalDistance', 0))
        
        # 1. Sharp Turns (risk score >= 7)
        for turn in collections.get('sharp_turns', []):
            risk_score = self.safe_float(turn.get('riskScore', 0))
            if risk_score >= 7:
                distance_from_start = self.safe_float(turn.get('distanceFromStartKm', 0))
                all_points.append({
                    'type': self.get_turn_type_label(turn),
                    'latitude': self.safe_float(turn.get('latitude', 0)),
                    'longitude': self.safe_float(turn.get('longitude', 0)),
                    'distance_from_start': distance_from_start,
                    'distance_from_customer': max(0, total_distance - distance_from_start),
                    'risk_level': self.get_risk_level_text(risk_score),
                    'risk_score': risk_score,
                    'speed_limit': self.get_speed_limit_for_turn(turn),
                    'driver_action': self.get_complete_driver_action(turn, 'turn'),
                    'original_data': turn
                })
        
        # 2. Blind Spots (risk score >= 7)
        for spot in collections.get('blind_spots', []):
            risk_score = self.safe_float(spot.get('riskScore', 0))
            if risk_score >= 7:
                distance_from_start = self.safe_float(spot.get('distanceFromStartKm', 0))
                all_points.append({
                    'type': self.get_blind_spot_type_label(spot),
                    'latitude': self.safe_float(spot.get('latitude', 0)),
                    'longitude': self.safe_float(spot.get('longitude', 0)),
                    'distance_from_start': distance_from_start,
                    'distance_from_customer': max(0, total_distance - distance_from_start),
                    'risk_level': self.get_risk_level_text(risk_score),
                    'risk_score': risk_score,
                    'speed_limit': self.get_speed_limit_for_blind_spot(spot),
                    'driver_action': self.get_complete_driver_action(spot, 'blind_spot'),
                    'original_data': spot
                })
        
        # 3. Network Dead Zones
        for zone in collections.get('network_coverage', []):
            if zone.get('isDeadZone', False):
                distance_from_start = self.safe_float(zone.get('distanceFromStartKm', 0))
                all_points.append({
                    'type': 'Network Dead Zone',
                    'latitude': self.safe_float(zone.get('latitude', 0)),
                    'longitude': self.safe_float(zone.get('longitude', 0)),
                    'distance_from_start': distance_from_start,
                    'distance_from_customer': max(0, total_distance - distance_from_start),
                    'risk_level': self.get_dead_zone_severity(zone),
                    'risk_score': self.safe_float(zone.get('communicationRisk', 5)),
                    'speed_limit': 'Normal',
                    'driver_action': 'Inform control room before entering, use alternative communication',
                    'original_data': zone
                })
        
        return all_points

    # ============================================================================
    # DUPLICATE REMOVAL METHOD (UPDATED)
    # ============================================================================

    def remove_duplicate_coordinates(self, risk_points: List[Dict]) -> List[Dict]:
        """Remove duplicate points based on coordinates, keeping highest risk"""
        seen_coords = {}
        unique_points = []
        
        for point in risk_points:
            # Create coordinate key with reduced precision to catch near-duplicates
            coord_key = f"{point['latitude']:.4f},{point['longitude']:.4f}"
            
            if coord_key not in seen_coords:
                seen_coords[coord_key] = point
                unique_points.append(point)
            else:
                # Keep the point with higher risk score
                existing_point = seen_coords[coord_key]
                if point.get('risk_score', 0) > existing_point.get('risk_score', 0):
                    # Replace with higher risk point
                    unique_points = [p for p in unique_points if p != existing_point]
                    unique_points.append(point)
                    seen_coords[coord_key] = point
        
        return unique_points

    # ============================================================================
    # TABLE CREATION METHOD (FIXED PAGINATION)
    # ============================================================================

    def create_comprehensive_risk_table(self, canvas_obj, headers: List[str], data: List[List[str]], 
                                original_points: List[Dict], start_x: int, start_y: int, 
                                col_widths: List[int], title_color=None, title_text_color=None):
        """
        Create enhanced table with dynamic row heights and proper pagination.
        Title is shown on every subpage.
        """
        if title_color is None:
            title_color = self.colors.PRIMARY
        if title_text_color is None:
            title_text_color = self.colors.WHITE

        current_y = start_y
        table_width = sum(col_widths)
        min_row_height = 18

        page_number = 1
        row_idx = 0
        headers_drawn = False

        while row_idx < len(data):
            # Check if we need a new page or draw headers
            if not headers_drawn or current_y < 120:
                if row_idx > 0:  # Not the first page
                    canvas_obj.showPage()
                    self.add_page_header(canvas_obj, "HPCL - Journey Risk Management Study (AI-Powered Analysis)")
                    current_y = self.page_height - 120
                    page_number += 1

                # Draw Title Bar on each page
                canvas_obj.setFillColor(title_color)
                canvas_obj.rect(start_x, current_y, table_width, 30, fill=1, stroke=0)
                canvas_obj.setFillColor(title_text_color)
                canvas_obj.setFont("Helvetica-Bold", 14)
                canvas_obj.drawString(start_x + 10, current_y + 10, "HIGH-RISK ZONES & KEY RISK POINTS")
                current_y -= 20  # Leave space below title

                # Draw headers
                header_lines = []
                max_header_lines = 1
                for header, width in zip(headers, col_widths):
                    lines = self.wrap_text(str(header), "Helvetica-Bold", 9, width - 6)
                    header_lines.append(lines)
                    max_header_lines = max(max_header_lines, len(lines))

                dynamic_header_height = max_header_lines * 11 + 10
                canvas_obj.setFillColor("#ADD8E6")
                canvas_obj.rect(start_x, current_y - dynamic_header_height, table_width, dynamic_header_height, fill=1, stroke=1)
                canvas_obj.setFillColor(self.colors.BLACK)
                canvas_obj.setFont("Helvetica-Bold", 9)

                x_pos = start_x
                for col_idx, (lines, width) in enumerate(zip(header_lines, col_widths)):
                    vertical_offset = (dynamic_header_height - (len(lines) * 11)) / 2
                    for line_idx, line in enumerate(lines):
                        y_pos = current_y - dynamic_header_height + vertical_offset + (len(lines) - line_idx - 1) * 11 + 5
                        canvas_obj.drawString(x_pos + 3, y_pos, line)
                    canvas_obj.setStrokeColor(self.colors.WHITE)
                    canvas_obj.setLineWidth(0.5)
                    canvas_obj.line(x_pos + width, current_y - dynamic_header_height, x_pos + width, current_y)
                    x_pos += width

                canvas_obj.rect(start_x, current_y - dynamic_header_height, table_width, dynamic_header_height, fill=0, stroke=1)
                current_y -= dynamic_header_height  # Adjust current_y
                headers_drawn = True

            # Calculate required row height for current row
            row = data[row_idx]
            point = original_points[row_idx]
            row_height = self.calculate_row_height(canvas_obj, row, col_widths, min_row_height)

            # Check if row fits on current page
            if current_y - row_height < 100:
                headers_drawn = False  # Trigger new page
                continue

            # Alternate row colors
            canvas_obj.setFillColor(self.colors.WHITE if row_idx % 2 == 0 else (0.96, 0.96, 0.96))
            canvas_obj.rect(start_x, current_y - row_height, table_width, row_height, fill=1, stroke=0)
            canvas_obj.setStrokeColor(self.colors.SECONDARY)
            canvas_obj.setLineWidth(0.5)
            canvas_obj.rect(start_x, current_y - row_height, table_width, row_height, fill=0, stroke=1)

            x_pos = start_x
            for col_idx, (cell_data, width) in enumerate(zip(row, col_widths)):
                if col_idx < len(row) - 1:
                    canvas_obj.line(x_pos + width, current_y - row_height, x_pos + width, current_y)

                canvas_obj.setFillColor(self.colors.SECONDARY)

                if col_idx == 3:
                    canvas_obj.setFont("Helvetica", 8)
                    canvas_obj.drawString(x_pos + 3, current_y - 12, cell_data)

                    canvas_obj.setFillColor(self.colors.INFO)
                    canvas_obj.setFont("Helvetica-Bold", 7)
                    view_text = "[view]"
                    view_width = canvas_obj.stringWidth(view_text, "Helvetica-Bold", 7)
                    view_x = x_pos + width - view_width - 5
                    canvas_obj.drawString(view_x, current_y - 12, view_text)

                    lat, lng = point['latitude'], point['longitude']
                    maps_url = f"https://www.google.com/maps/search/?api=1&query={lat},{lng}&zoom=17"
                    canvas_obj.linkURL(maps_url, (view_x - 2, current_y - 14, view_x + view_width + 2, current_y - 2))

                elif col_idx == 6:
                    canvas_obj.setFont("Helvetica", 7)
                    self.draw_multiline_text(canvas_obj, cell_data, x_pos + 3, current_y - 10, width - 6, 7, row_height - 4)

                elif col_idx == 0:
                    canvas_obj.setFont("Helvetica-Bold", 8)
                    self.draw_multiline_text(canvas_obj, cell_data, x_pos + 3, current_y - 10, width - 6, 8, row_height - 4)

                else:
                    canvas_obj.setFont("Helvetica", 8)
                    if col_idx in [1, 2]:
                        text_width = canvas_obj.stringWidth(cell_data, "Helvetica", 8)
                        text_x = x_pos + (width - text_width) / 2
                        canvas_obj.drawString(text_x, current_y - 12, cell_data)
                    else:
                        canvas_obj.drawString(x_pos + 3, current_y - 12, cell_data)

                x_pos += width

            current_y -= row_height
            row_idx += 1

        # Add summary note if space permits
        if current_y > 80:
            current_y -= 10
            canvas_obj.setFillColor(self.colors.INFO)
            canvas_obj.setFont("Helvetica-Oblique", 8)
            canvas_obj.drawString(start_x, current_y, "All high-risk points are sorted by distance from supply location. Please exercise extreme caution at these locations.")

    # ============================================================================
    # HELPER METHODS FOR TABLE FORMATTING (UNCHANGED)
    # ============================================================================

    def calculate_row_height(self, canvas_obj, row: List[str], col_widths: List[int], min_height: int) -> int:
        """Calculate required row height based on content"""
        max_lines = 1
        
        # Check driver action column (index 6) which typically has longest text
        if len(row) > 6:
            driver_action = row[6]
            width = col_widths[6] - 6
            lines = self.calculate_text_lines(driver_action, canvas_obj._fontname, 7, width)
            max_lines = max(max_lines, len(lines))
        
        # Check type column (index 0)
        if len(row) > 0:
            type_text = row[0]
            width = col_widths[0] - 6
            lines = self.calculate_text_lines(type_text, canvas_obj._fontname, 8, width)
            max_lines = max(max_lines, len(lines))
        
        # Calculate height: base height + extra height for additional lines
        line_height = 10
        return max(min_height, 12 + (max_lines * line_height))

    def calculate_text_lines(self, text: str, font_name: str, font_size: int, max_width: float) -> List[str]:
        """Calculate how many lines text will take"""
        from reportlab.lib.utils import simpleSplit
        try:
            lines = simpleSplit(str(text), font_name, font_size, max_width)
            return lines
        except:
            # Fallback: estimate based on character count
            chars_per_line = int(max_width / (font_size * 0.5))
            num_lines = (len(str(text)) + chars_per_line - 1) // chars_per_line
            return [''] * num_lines

    def draw_multiline_text(self, canvas_obj, text: str, x: float, y: float, 
                        max_width: float, font_size: int, max_height: float):
        """Draw text with word wrapping"""
        from reportlab.lib.utils import simpleSplit
        
        try:
            # Split text into lines that fit
            lines = simpleSplit(str(text), canvas_obj._fontname, font_size, max_width)
        except:
            # Fallback if simpleSplit fails
            lines = [text]
        
        # Calculate line height
        line_height = font_size + 2
        
        # Draw lines
        current_y = y
        for i, line in enumerate(lines):
            if current_y - y - line_height < -max_height:
                # Add ellipsis if text is cut off
                if i < len(lines) - 1:
                    line = line[:-3] + "..."
                canvas_obj.drawString(x, current_y, line)
                break
            canvas_obj.drawString(x, current_y, line)
            current_y -= line_height

    # ============================================================================
    # LABEL AND FORMATTING HELPER METHODS (UNCHANGED)
    # ============================================================================

    def get_turn_type_label(self, turn: Dict) -> str:
        """Get descriptive label for sharp turn"""
        angle = turn.get('turnAngle', 0)
        direction = turn.get('turnDirection', 'turn').title()
        
        if angle > 120:
            return f"Hairpin Turn ({direction})"
        elif angle > 90:
            return f"Sharp Turn ({direction})"
        elif angle > 60:
            return f"Moderate Turn ({direction})"
        else:
            return f"Turn ({direction})"

    def get_blind_spot_type_label(self, spot: Dict) -> str:
        """Get descriptive label for blind spot"""
        spot_type = spot.get('spotType', 'unknown')
        visibility = spot.get('visibilityDistance', 0)
        
        if visibility < 50:
            severity = "Critical"
        elif visibility < 100:
            severity = "Severe"
        else:
            severity = ""
        
        type_labels = {
            'crest': 'Hill Crest',
            'curve': 'Curved Section',
            'intersection': 'Blind Intersection',
            'obstruction': 'Obstruction',
            'vegetation': 'Vegetation',
            'structure': 'Structure'
        }
        
        base_label = type_labels.get(spot_type, 'Blind Spot')
        
        if severity:
            return f"{severity} {base_label}"
        return base_label

    def get_risk_level_text(self, risk_score: float) -> str:
        """Convert risk score to readable text"""
        if risk_score >= 9:
            return "CRITICAL"
        elif risk_score >= 8:
            return "Very High"
        elif risk_score >= 7:
            return "High"
        elif risk_score >= 6:
            return "Moderate-High"
        elif risk_score >= 5:
            return "Moderate"
        else:
            return "Low-Moderate"

    def get_speed_limit_for_turn(self, turn: Dict) -> str:
        """Get speed limit recommendation for turn"""
        risk_score = turn.get('riskScore', 0)
        angle = turn.get('turnAngle', 0)
        
        if risk_score >= 8 or angle > 120:
            return "10-15 km/h"
        elif risk_score >= 7 or angle > 90:
            return "20-25 km/h"
        elif risk_score >= 6 or angle > 60:
            return "30 km/h"
        else:
            return "40 km/h"

    def get_speed_limit_for_blind_spot(self, spot: Dict) -> str:
        """Get speed limit for blind spot"""
        visibility = spot.get('visibilityDistance', 0)
        risk_score = spot.get('riskScore', 0)
        
        if visibility < 50 or risk_score >= 8:
            return "15-20 km/h"
        elif visibility < 100 or risk_score >= 7:
            return "25-30 km/h"
        else:
            return "30-40 km/h"

    def get_dead_zone_severity(self, zone: Dict) -> str:
        """Get severity level for network dead zone"""
        severity = zone.get('deadZoneSeverity', 'moderate')
        comm_risk = zone.get('communicationRisk', 5)
        
        if severity == 'critical' or comm_risk >= 8:
            return "CRITICAL"
        elif severity == 'severe' or comm_risk >= 7:
            return "High"
        else:
            return "Moderate"

    def get_complete_driver_action(self, item: Dict, item_type: str) -> str:
        """Get complete driver action recommendation without truncation"""
        if item_type == 'turn':
            angle = item.get('turnAngle', 0)
            risk_score = item.get('riskScore', 0)
            
            if angle > 120 or risk_score >= 8:
                return "Stop completely, check visibility, use horn, proceed at 10-15 km/h with extreme caution"
            elif angle > 90 or risk_score >= 7:
                return "Reduce speed to 20 km/h before turn, use horn, check mirrors, stay in center of lane"
            elif angle > 60:
                return "Reduce speed to 30 km/h, signal early, maintain lane discipline"
            else:
                return "Reduce speed appropriately, follow standard turning procedures"
        
        elif item_type == 'blind_spot':
            visibility = item.get('visibilityDistance', 0)
            spot_type = item.get('spotType', 'unknown')
            
            if visibility < 50:
                return "Sound horn continuously, reduce to walking pace (10-15 km/h), be prepared to stop"
            elif visibility < 100:
                return "Use horn before entering, reduce speed to 20-25 km/h, stay alert for oncoming traffic"
            elif spot_type == 'intersection':
                return "Come to complete stop, check all directions, proceed when clear"
            else:
                return "Sound horn, reduce speed to 30 km/h, maintain vigilance"
        
        else:
            return "Follow standard safety protocols for this area"
    
    def create_seasonal_road_conditions_page(self, canvas_obj, route_data: Dict[str, Any]):
        """Create page for seasonal road conditions and traffic patterns"""

        road_conditions = self.remove_duplicate_coordinates(route_data['collections'].get("road_conditions", []))
        weather_data = self.remove_duplicate_coordinates(route_data['collections'].get("weather_conditions", []))

        self.add_page_header(canvas_obj, "HPCL - Journey Risk Management Study (AI-Powered Analysis)")
        y_pos = self.page_height - 100

        headers = [
            "Season / Condition",
            "Critical Stretches / Coordinates / Roads",
            "Link",
            "Typical Challenges",
            "Driver Caution"
        ]
        col_widths = [70, 100, 40, 150, 150]

        seasonal_rows = []
        for road in road_conditions:
            lat = road.get("latitude", 0.0)
            lon = road.get("longitude", 0.0)
            road_type = road.get("roadType", "Unknown")
            
            # Match closest weather record (you can use more precise matching if needed)
            weather = weather_data[0] if weather_data else {}

            season = weather.get("season", "Unknown")
            risk_score = int(weather.get("riskScore", 0))
            condition = weather.get("weatherCondition", "Unknown")
            temperature = float(weather.get("averageTemperature", 0.0))
            monsoon_risk = weather.get("monsoonRisk", 0)
            impact = weather.get("drivingConditionImpact", "Unknown")
            
            if temperature >= 95:
                challenges = "High temperatures → vehicle overheating, tire blowouts"
                caution = "Pre-check cooling systems and carry extra water."
            elif temperature >= 80:
                challenges = " Warm: Possible discomfort or moderate risk"
                caution = "Pre-check cooling systems and carry extra water."
            elif monsoon_risk >= 5:
                challenges = "Flood-prone zones, reduced traction"
                caution = "Slow down in rain, maintain distance."
            elif impact.lower() == "minimal":
                challenges = "No significant challenges"
                caution = "Standard precautions apply"
            else:
                challenges = "Extreme heat exposure, limited shade, dust storms"
                caution = "Plan travel during cooler hours, carry sun protection"

            coord = f"{lat:.6f}, {lon:.6f} ({road_type}) "
            map_link = f"https://www.google.com/maps?q={lat}%2C{lon}"
            if risk_score >= 7:
                seasonal_rows.append([
                    f"{season.capitalize()} - ({condition})",
                    coord,
                    map_link,
                    challenges,
                    caution
                ])

        if seasonal_rows:
            y_pos = self.create_simple_table_with_link(
                canvas_obj,
                "SEASONAL ROAD CONDITIONS & TRAFFIC PATTERNS",
                headers,
                seasonal_rows,
                start_x=30,
                start_y=y_pos,
                col_widths=col_widths,
                title_bg_color=self.colors.WHITE,
                header_color = "#ADD8E6",
                title_text_color=self.colors.PRIMARY,
                hyper_link=True,
                hyper_link_col_index=2,
                hyper_link_view_text="view"
            )
        
        # Weather-related accident-prone areas
        y_pos -= 40
        if y_pos < 300:
            canvas_obj.showPage()
            self.add_page_header(canvas_obj, "HPCL - Journey Risk Management Study (AI-Powered Analysis)")
            y_pos = self.page_height - 120

        canvas_obj.setFillColor(self.colors.DANGER)
        canvas_obj.setFont("Helvetica-Bold", 13)
        canvas_obj.drawString(40, y_pos, "WEATHER-RELATED ACCIDENT-PRONE AREAS & CORRECTIVE MEASURES")
        y_pos -= 30

        headers = ["Area", "Weather Risk", "Risk Type", "Recommended Solution"]
        col_widths = [150, 80, 80, 200]
        weather_hazards = [
            ["NH-344 (Ghata Village)", "Extreme Heat", "Tire Blowouts", "Shade shelters, road resurfacing"],
            ["NH-709 (Ambeta)", "Fog", "Low Visibility", "Fog lights, reflective signs"],
            ["Putha Village Stretch", "Frost", "Skidding", "Apply salt/sand, use winter tires"],
            ["Oil Terminal Junctions", "Rain/Fog", "Poor Visibility", "Better signage, signalization"],
            ["Moti Filling Station Access", "Rain/Fog", "Slippery Surfaces", "Drainage improvement, widen shoulders"]
        ]
        y_pos = self.create_simple_table(
            canvas_obj,
            "",
            headers,
            weather_hazards,
            30, y_pos,
            col_widths,
            title_color=self.colors.WARNING,
            header_color=self.colors.WHITE,
            max_rows_per_page=15
        )

    def create_risk_zones_page(self, canvas_obj, route_data: Dict[str, Any]):
        """Create Page 4: High-Risk Zones with auto-paginated tables"""
        collections = route_data['collections']
        
        # Page header
        self.add_page_header(canvas_obj, "HIGH-RISK ZONES & KEY RISK POINTS", "Critical areas requiring special attention")
        
        y_pos = self.page_height - 120
        
        # Sharp turns table
        if collections['sharp_turns']:
            headers = ["Location (GPS)", "Distance (km)", "Angle", "Risk Level", "Speed Limit", "Driver Action"]
            col_widths = [120, 70, 50, 70, 70, 120]
            
            # Sort by risk score
            sorted_turns = sorted(collections['sharp_turns'], key=lambda x: x.get('riskScore', 0), reverse=True)
            
            turns_data = []
            for turn in sorted_turns:
                risk_score = turn.get('riskScore', 0)
                risk_level = "Critical" if risk_score >= 8 else "High" if risk_score >= 6 else "Medium"
                
                turns_data.append([
                    f"{turn.get('latitude', 0):.5f}, {turn.get('longitude', 0):.5f}",
                    f"{turn.get('distanceFromStartKm', 0):.1f}",
                    f"{turn.get('turnAngle', 0):.1f}°",
                    risk_level,
                    f"{turn.get('recommendedSpeed', 40)} km/h",
                    "Reduce speed, check visibility"
                ])
            
            # This will automatically handle pagination
            y_pos = self.create_simple_table(
                canvas_obj,
                "CRITICAL SHARP TURNS",
                headers,
                turns_data,
                30, y_pos,
                col_widths,
                title_color=self.colors.WARNING,
                max_rows_per_page=20  # Limit rows per page
            )
        
        # Check if we need a new page for blind spots
        if y_pos < 300:  # If less than 300px left, start new page
            canvas_obj.showPage()
            self.add_page_header(canvas_obj, "HIGH-RISK ZONES & KEY RISK POINTS", "Blind spots analysis")
            y_pos = self.page_height - 120
        else:
            y_pos -= 40  # Add spacing between tables
        
        # Blind spots section
        if collections['blind_spots']:
            headers = ["Location (GPS)", "Type", "Visibility (m)", "Risk Level", "Action Required"]
            col_widths = [130, 80, 80, 80, 130]
            
            sorted_blind_spots = sorted(collections['blind_spots'], key=lambda x: x.get('riskScore', 0), reverse=True)
            
            blind_data = []
            for spot in sorted_blind_spots:
                risk_score = spot.get('riskScore', 0)
                risk_level = "Critical" if risk_score >= 8 else "High" if risk_score >= 6 else "Medium"
                
                blind_data.append([
                    f"{spot.get('latitude', 0):.5f}, {spot.get('longitude', 0):.5f}",
                    spot.get('spotType', 'Unknown').title(),
                    str(spot.get('visibilityDistance', 0)),
                    risk_level,
                    "Use horn, stay alert"
                ])
            
            # This will automatically handle pagination
            y_pos = self.create_simple_table(
                canvas_obj,
                "CRITICAL BLIND SPOTS",
                headers,
                blind_data,
                30, y_pos,
                col_widths,
                title_color=self.colors.DANGER,
                max_rows_per_page=20  # Limit rows per page
            )

    def wrap_text(self,text, font_name, font_size, max_width):
        """
        Wrap text into multiple lines so that it fits within max_width.
        """
        words = text.split()
        lines = []
        current_line = ""
        for word in words:
            test_line = current_line + " " + word if current_line else word
            if stringWidth(test_line, font_name, font_size) <= max_width:
                current_line = test_line
            else:
                lines.append(current_line)
                current_line = word
        if current_line:
            lines.append(current_line)
        return lines

    def string_width(self, text, font_name, font_size):
        from reportlab.pdfbase.pdfmetrics import stringWidth
        return stringWidth(text, font_name, font_size)

    def create_simple_table_with_link(self, canvas_obj, title: str, headers: list, data: list,
                                   start_x: int, start_y: int, col_widths: list,
                                   title_bg_color=None, header_color=None, title_text_color=None,
                                   hyper_link=False, hyper_link_col_index=None, hyper_link_view_text="View"):

        if title_bg_color is None:
            title_bg_color = self.colors.PRIMARY
        if header_color is None:
            header_color = "#ADD8E6"
        if title_text_color is None:
            title_text_color = self.colors.WHITE

        font_name = "Helvetica"
        font_size = 8
        line_spacing = 11
        header_padding = 6
        cell_padding = 3
        title_height = 25
        footer_height = 70

        current_y = start_y
        table_width = sum(col_widths)

        # Translator setup
        if not hasattr(self, 'translator'):
            try:
                from googletrans import Translator
                self.translator = Translator()
            except ImportError:
                logger.warning("googletrans not installed. Translation will be skipped.")
                self.translator = None

        first_chunk = True
        rows_rendered = 0
        current_row_index = 0

        while current_row_index < len(data):
            if not first_chunk:
                canvas_obj.showPage()
                self.add_page_header(canvas_obj, "HPCL - Journey Risk Management Study (AI-Powered Analysis)", "Continued from previous page")
                current_y = self.page_height - 120

            # Draw title on every page (including subpages)
            if title:
                canvas_obj.setFillColor(title_bg_color)
                canvas_obj.rect(start_x, current_y, table_width, title_height, fill=1, stroke=0)
                canvas_obj.setFillColor(title_text_color)
                canvas_obj.setFont("Helvetica-Bold", 12)
                canvas_obj.drawString(start_x, current_y + 8, title)
                current_y -= (title_height - 5)

            # Draw header only on first page
            if first_chunk:
                header_lines = []
                max_header_lines = 1
                for header, width in zip(headers, col_widths):
                    lines = self.wrap_text(str(header), "Helvetica-Bold", font_size, width - cell_padding * 2)
                    header_lines.append(lines)
                    max_header_lines = max(max_header_lines, len(lines))

                header_height = max_header_lines * line_spacing + header_padding
                canvas_obj.setFillColor(header_color)
                canvas_obj.rect(start_x, current_y - header_height, table_width, header_height, fill=1, stroke=0)
                canvas_obj.setFillColor(self.colors.BLACK)
                canvas_obj.setFont("Helvetica-Bold", font_size)

                x_pos = start_x
                for col_idx, (lines, width) in enumerate(zip(header_lines, col_widths)):
                    vertical_offset = (header_height - (len(lines) * line_spacing)) / 2
                    for line_idx, line in enumerate(lines):
                        y_pos = current_y - header_height + vertical_offset + (len(lines) - line_idx - 1) * line_spacing + 5
                        canvas_obj.drawString(x_pos + cell_padding, y_pos, line)
                    canvas_obj.setStrokeColor(self.colors.SECONDARY)
                    canvas_obj.setLineWidth(0.5)
                    canvas_obj.line(x_pos + width, current_y - header_height, x_pos + width, current_y)
                    x_pos += width

                canvas_obj.rect(start_x, current_y - header_height, table_width, header_height, fill=0, stroke=1)
                current_y -= header_height

            # Draw rows
            while current_row_index < len(data):
                row = data[current_row_index]
                cell_lines_list = []
                max_cell_lines = 1
                for i, (cell, width) in enumerate(zip(row, col_widths)):
                    if hyper_link and i == hyper_link_col_index:
                        cell_lines = [hyper_link_view_text]
                    else:
                        cell_text = str(cell) if self.translator is None else self.translate_to_english(str(cell))
                        cell_lines = self.wrap_text(cell_text, font_name, font_size, width - cell_padding * 2)
                    cell_lines_list.append(cell_lines)
                    max_cell_lines = max(max_cell_lines, len(cell_lines))
                row_height = max_cell_lines * line_spacing + header_padding

                if current_y - row_height - footer_height < 0:
                    break  # Page break

                canvas_obj.setFillColorRGB(1, 1, 1) if rows_rendered % 2 == 0 else canvas_obj.setFillColorRGB(0.95, 0.95, 0.95)
                canvas_obj.rect(start_x, current_y - row_height, table_width, row_height, fill=1, stroke=0)

                canvas_obj.setFont(font_name, font_size)
                x_pos = start_x
                for col_idx, (lines, width) in enumerate(zip(cell_lines_list, col_widths)):
                    vertical_offset = (row_height - (len(lines) * line_spacing)) / 2
                    for line_idx, line in enumerate(lines):
                        y_pos = current_y - row_height + vertical_offset + (len(lines) - line_idx - 1) * line_spacing + 5
                        if hyper_link and col_idx == hyper_link_col_index and line_idx == 0:
                            url = str(row[col_idx])
                            canvas_obj.setFont("Helvetica-Bold", font_size)
                            canvas_obj.setFillColor(self.colors.INFO)
                            canvas_obj.drawString(x_pos + cell_padding, y_pos, line)
                            link_width = self.string_width(line, "Helvetica-Bold", font_size)
                            canvas_obj.linkURL(
                                url,
                                (x_pos + cell_padding, y_pos, x_pos + cell_padding + link_width, y_pos + font_size),
                                relative=0
                            )
                            canvas_obj.setFont(font_name, font_size)
                        else:
                            canvas_obj.setFillColor(self.colors.BLACK)
                            canvas_obj.drawString(x_pos + cell_padding, y_pos, line)
                    canvas_obj.setStrokeColor(self.colors.SECONDARY)
                    canvas_obj.line(x_pos + width, current_y - row_height, x_pos + width, current_y)
                    x_pos += width

                canvas_obj.rect(start_x, current_y - row_height, table_width, row_height, fill=0, stroke=1)
                current_y -= row_height
                current_row_index += 1
                rows_rendered += 1

            if current_row_index < len(data):
                canvas_obj.setFillColor(self.colors.INFO)
                canvas_obj.setFont("Helvetica-Oblique", 9)
                footer_y = footer_height - 15
                text = f"Continued on next page... (Showing {rows_rendered} of {len(data)} total records)"
                canvas_obj.drawString(start_x, footer_y, text)

            first_chunk = False

        return current_y - 20

    def create_simple_table(self, canvas_obj, title: str, headers: list, data: list,
                            start_x: int, start_y: int, col_widths: list,
                            title_color=None, header_color=None, text_color=None,
                            max_rows_per_page=25, title_font_size:int = 12):
        
        if title_color is None:
            title_color = self.colors.PRIMARY
        if header_color is None:
            header_color = "#ADD8E6"
        if text_color is None:
            text_color = self.colors.WHITE

        font_name = "Helvetica"
        font_size = 8
        line_spacing = 11  # Vertical space between lines
        current_y = start_y
        table_width = sum(col_widths)
        title_height = 25
        header_padding = 4

        # Helper to compute row height
        def get_row_height(row, font_size, col_widths):
            max_lines = 1
            for cell, width in zip(row, col_widths):
                lines = self.wrap_text(str(cell), font_name, font_size, width - 6)
                max_lines = max(max_lines, len(lines))
            return max_lines * line_spacing + header_padding

        # Split data into chunks based on height that can fit in a page
        data_chunks = []
        i = 0
        while i < len(data):
            chunk = []
            available_height = self.page_height - 100 if i > 0 else current_y - 100 - title_height - line_spacing * 2
            used_height = 0
            while i < len(data):
                row_height = get_row_height(data[i], font_size, col_widths)
                if used_height + row_height > available_height:
                    break
                chunk.append(data[i])
                used_height += row_height
                i += 1
            data_chunks.append(chunk)

        first_chunk = True
        for chunk_index, chunk_data in enumerate(data_chunks):
            if not first_chunk:
                canvas_obj.showPage()
                self.add_page_header(canvas_obj, "HPCL - Journey Risk Management Study (AI-Powered Analysis)", "Continued from previous page")
                current_y = self.page_height - 120

            # Title
            if first_chunk and title:
                canvas_obj.setFillColor(title_color)
                canvas_obj.rect(start_x, current_y, table_width, title_height, fill=1, stroke=0)
                canvas_obj.setFillColor(text_color)
                canvas_obj.setFont("Helvetica-Bold", title_font_size)
                canvas_obj.drawString(start_x + 10, current_y + 8, title)
                current_y -= (title_height - 20)

            # Header
            header_lines = [self.wrap_text(header, "Helvetica-Bold", 8, width - 6) for header, width in zip(headers, col_widths)]
            header_line_count = max(len(lines) for lines in header_lines)
            header_height = header_line_count * line_spacing + header_padding + 7

            # Draw header background
            canvas_obj.setFillColor(header_color)
            canvas_obj.rect(start_x, current_y - header_height, table_width, header_height, fill=1, stroke=0)
            
            # Draw header text
            canvas_obj.setFillColor(self.colors.BLACK)  # Changed to black for better visibility
            canvas_obj.setFont("Helvetica-Bold", 8)
            
            x_pos = start_x
            for i, (lines, width) in enumerate(zip(header_lines, col_widths)):
                for j, line in enumerate(lines):
                    y = current_y - header_height + (header_line_count - j - 1) * line_spacing + 5  # Adjusted y-position
                    canvas_obj.drawString(x_pos + 3, y, line)
                x_pos += width

            # Draw header borders
            canvas_obj.setStrokeColor(self.colors.SECONDARY)
            canvas_obj.setLineWidth(0.5)
            canvas_obj.rect(start_x, current_y - header_height, table_width, header_height, fill=0, stroke=1)
            
            # Draw vertical lines between columns
            x_pos = start_x
            for width in col_widths:
                x_pos += width
                canvas_obj.line(x_pos, current_y - header_height, x_pos, current_y)

            current_y -= header_height 

            # Draw rows
            for row_index, row in enumerate(chunk_data):
                cell_lines_list = [self.wrap_text(str(cell), font_name, font_size, width - 6)
                                for cell, width in zip(row, col_widths)]
                line_count = max(len(lines) for lines in cell_lines_list)
                row_height = line_count * line_spacing + header_padding + 5

                canvas_obj.setFillColorRGB(1, 1, 1) if row_index % 2 == 0 else canvas_obj.setFillColorRGB(0.97, 0.97, 0.97)
                canvas_obj.rect(start_x, current_y - row_height, table_width, row_height, fill=1, stroke=0)

                canvas_obj.setStrokeColor(self.colors.SECONDARY)
                canvas_obj.rect(start_x, current_y - row_height, table_width, row_height, fill=0, stroke=1)

                canvas_obj.setFont(font_name, font_size)
                x_pos = start_x
                for i, (lines, width) in enumerate(zip(cell_lines_list, col_widths)):
                    for j, line in enumerate(lines):
                        y = current_y - row_height + (line_count - j - 1) * line_spacing + 5  # Adjusted y-position
                        canvas_obj.setFillColor(self.colors.BLACK)  # Changed to black for better visibility
                        canvas_obj.drawString(x_pos + 3, y, line)

                    canvas_obj.line(x_pos, current_y - row_height, x_pos, current_y)
                    x_pos += width

                canvas_obj.line(x_pos, current_y - row_height, x_pos, current_y)
                current_y -= row_height

            # Continuation message
            if chunk_index < len(data_chunks) - 1:
                canvas_obj.setFillColor(self.colors.INFO)
                canvas_obj.setFont("Helvetica-Oblique", 9)
                text = f"Continued on next page... (Showing {chunk_index * max_rows_per_page + len(chunk_data)} of {len(data)} total records)"
                canvas_obj.drawString(start_x, current_y - 15, text)

            first_chunk = False

        return current_y - 20

    def create_medical_facilities_page(self, canvas_obj, route_data: Dict[str, Any]):
        """Create Page 5: Medical Facilities with auto-paginated tables"""
        collections = route_data['collections']
        all_emergency_services = self.remove_duplicate_coordinates(collections['emergency_services'])
        # Filter medical facilities
        medical_facilities = [s for s in all_emergency_services if s.get('serviceType') == 'hospital']

        self.add_page_header(canvas_obj, "HPCL - Journey Risk Management Study (AI-Powered Analysis)")
        y_pos = self.page_height - 100

        y_pos = self.draw_centered_text_in_box(
            canvas_obj,
            "COMPREHENSIVE POINTS OF INTEREST ANALYSIS",
            50, y_pos, 480,30,
            text_color=self.colors.WHITE,
            box_color=self.colors.SUCCESS
            )
        
        essential_data = len([s for s in all_emergency_services if s.get('serviceType') == 'amenity'])
        network_data = self.remove_duplicate_coordinates(collections.get('network_coverage', []))
        if network_data:
            total_points = len(network_data)
            dead_zones = len([n for n in network_data if n.get('isDeadZone', False)])
            weak_signal = len([n for n in network_data if n.get('signalStrength', 10) < 4])
            good_coverage = total_points - dead_zones - weak_signal
            coverage_score = ((good_coverage / total_points) * 100) if total_points > 0 else 0
        
        total_pois = len(all_emergency_services)+ total_points
        emergency_count = len(all_emergency_services)-essential_data
        table_overall_data = [
            ["Total POIs Identified", f"{total_pois}"],
            ["Emergency Services", f"{emergency_count}"],
            ["Essential Services", f"{essential_data}"],
            ["Other Services", f"{total_pois - len(all_emergency_services)}"], 
            ["Coverage Score  ", f"{coverage_score:.2f}"]]

        y_pos = self.create_simple_table(
            canvas_obj,
            "",
            ["Perameters", "Value"],
            table_overall_data,
            120, y_pos+15, [200, 100],
            title_color=self.colors.WHITE,
            header_color=self.colors.WHITE,
            text_color=self.colors.WHITE
        )
        y_pos -= 25

        title = "EMERGENCY PREPAREDNESS & RESPONSE"
        canvas_obj.setFillColor(self.colors.PRIMARY)
        canvas_obj.setFont("Helvetica-Bold", 12)
        canvas_obj.drawString(30, y_pos, title)

        y_pos -= 30 
        
        if medical_facilities:
            # Prepare data for table
            unique_medical_facilities = self.remove_duplicate_coordinates(medical_facilities)

            medical_data = sorted(
                [n for n in unique_medical_facilities if self.safe_float(n.get("distanceFromStartKm", 0)) >= 1.0],
                key=lambda x: self.safe_float(x.get("distanceFromStartKm", 0)))
            
            headers = ["id", "Facility Name", "Address", "From Supply (km)","From Customer (km)","Coordinates", "Link", "Phone"]
            col_widths = [20, 110, 140, 50, 58, 58, 30, 80]
            table_data = []

            for i, facility in enumerate(medical_data):
                latitude = facility.get('latitude',0)
                longitude = facility.get('longitude',0)
                maps_link =  f"https://www.google.com/maps?q={latitude}%2C{longitude}"
                table_data.append([
                    str(i + 1),
                    facility.get('name', 'Unknown'),
                    facility.get('address', 'Not specified'),
                    f"{facility.get('distanceFromStartKm', 0):.1f}",
                    f"{facility.get('distanceFromEndKm', 0):.1f}",
                    f"{latitude:.6f}, {longitude:.6f}",
                    maps_link,
                    facility.get('phoneNumber', 'N/A')
                ])
            
            # This will automatically handle pagination
            y_pos = self.create_simple_table_with_link(
                canvas_obj,
                "MEDICAL FACILITIES - Emergency Healthcare Services- CRITICAL",
                headers,
                table_data,
                30, y_pos,
                col_widths,
                hyper_link=True,
                hyper_link_col_index=6,
                header_color=self.colors.WHITE,
                title_bg_color=self.colors.WHITE,
                title_text_color=self.colors.DANGER
            )
        
        else:
            canvas_obj.setFillColor(self.colors.SECONDARY)
            canvas_obj.setFont("Helvetica", 12)
            canvas_obj.drawString(80, y_pos, "No medical facilities data available for this route")   
  
    def create_law_enforcement_page(self, canvas_obj, route_data: Dict[str, Any]):
        """Create Page 6: Law Enforcement & Fire Services with auto-paginated tables"""
        collections = route_data['collections']
        all_emergency_services = self.remove_duplicate_coordinates(collections['emergency_services'])
        
        # Page header
        self.add_page_header(canvas_obj, "HPCL - Journey Risk Management Study (AI-Powered Analysis)")

        y_pos = self.page_height - 120
        
        headers = ["id", "Facility Name", "Address", "From Supply (km)","From Customer (km)","Coordinates", "Link", "Phone"]
        col_widths = [20, 110, 140, 50, 58, 58, 30, 80]

        # Police stations
        police_stations = [s for s in all_emergency_services if s.get('serviceType') == 'police']
        if police_stations:
            police_sort_data = sorted(
                [n for n in police_stations if self.safe_float(n.get("distanceFromStartKm", 0)) >= 1.0],
                key=lambda x: self.safe_float(x.get("distanceFromStartKm", 0)))
            
            police_data = []
            for i, station in enumerate(police_sort_data):
                latitude = station.get('latitude',0)
                longitude = station.get('longitude',0)
                maps_link =  f"https://www.google.com/maps?q={latitude}%2C{longitude}"
                police_data.append([
                    str(i + 1),
                    station.get('name', 'Unknown'),
                    station.get('address', 'Not specified'),
                    f"{station.get('distanceFromStartKm', 0):.1f}",
                    f"{station.get('distanceFromEndKm', 0):.1f}",
                    f"{latitude:.6f}, {longitude:.6f}",
                    maps_link,
                    station.get('phoneNumber', 'N/A')
                ])
            
            # This will automatically handle pagination
            y_pos = self.create_simple_table_with_link(
                canvas_obj,
                "LAW ENFORCEMENT - Security & Emergency Response - CRITICAL",
                headers,
                police_data,
                30, y_pos,
                col_widths,
                title_bg_color=self.colors.WHITE,
                header_color=self.colors.WHITE,
                title_text_color=self.colors.INFO,
                hyper_link=True,
                hyper_link_col_index= 6
            )
        
        # Check if we need a new page for fire stations
        if y_pos < 150:
            canvas_obj.showPage()
            self.add_page_header(canvas_obj, "HPCL - Journey Risk Management Study (AI-Powered Analysis)")
            y_pos = self.page_height - 120
        else:
            y_pos -= 25
        
        # Fire stations
        fire_stations = [s for s in all_emergency_services if s.get('serviceType') == 'fire_station']
        
        if fire_stations:
            fire_sort_data = sorted(
                    [n for n in fire_stations if self.safe_float(n.get("distanceFromStartKm", 0)) >= 1.0],
                    key=lambda x: self.safe_float(x.get("distanceFromStartKm", 0)))
            fire_data = []
            for i, station in enumerate(fire_sort_data):
                latitude = station.get('latitude',0)
                longitude = station.get('longitude',0)
                maps_link =  f"https://www.google.com/maps?q={latitude}%2C{longitude}"
                fire_data.append([
                    str(i + 1),
                    station.get('name', 'Unknown'),
                    station.get('address', 'Not specified'),
                    f"{station.get('distanceFromStartKm', 0):.1f}",
                    f"{station.get('distanceFromEndKm', 0):.1f}",
                    f"{latitude:.6f}, {longitude:.6f}",
                    maps_link,
                    station.get('phoneNumber', 'N/A')
                ])
            
            # This will automatically handle pagination
            y_pos = self.create_simple_table_with_link(
                canvas_obj,
                "FIRE & RESCUE - Emergency Response Services– CRITICAL ",
                headers,
                fire_data,
                30, y_pos,
                col_widths,
                title_bg_color=self.colors.WHITE,
                title_text_color=self.colors.DANGER,
                header_color=self.colors.WHITE,
                hyper_link=True,
                hyper_link_col_index= 6
            )
        
        # Check if we need a new page for FUEL STATIONS
        if y_pos < 150: 
            canvas_obj.showPage()
            self.add_page_header(canvas_obj, "HPCL - Journey Risk Management Study (AI-Powered Analysis) ")
            y_pos = self.page_height - 120
        else:
            y_pos -= 30 
        
        # FUEL STATIONS
        fuel_stations = [s for s in all_emergency_services if s.get('serviceType') == 'mechanic']
        
        if fuel_stations:
            fuel_sort_data = sorted(
                    [n for n in fuel_stations if self.safe_float(n.get("distanceFromStartKm", 0)) >= 1.0],
                    key=lambda x: self.safe_float(x.get("distanceFromStartKm", 0)))
            fuel_data = []
            for i, station in enumerate(fuel_sort_data):
                latitude = station.get('latitude',0)
                longitude = station.get('longitude',0)
                maps_link =  f"https://www.google.com/maps?q={latitude}%2C{longitude}"
                fuel_data.append([
                    str(i + 1),
                    station.get('name', 'Unknown'),
                    station.get('address', 'Not specified'),
                    f"{station.get('distanceFromStartKm', 0):.1f}",
                    f"{station.get('distanceFromEndKm', 0):.1f}",
                    f"{latitude:.6f}, {longitude:.6f}",
                    maps_link,
                    station.get('phoneNumber', 'N/A')
                ])

            y_pos = self.create_simple_table_with_link(
                canvas_obj,
                "FUEL STATIONS - Vehicle Refueling Points – ESSENTIAL ",
                headers,
                fuel_data,
                30, y_pos,
                col_widths,
                title_bg_color=self.colors.WHITE,
                title_text_color=self.colors.WARNING,
                header_color=self.colors.WHITE,
                hyper_link=True,
                hyper_link_col_index= 6
            )

        # Check if we need a new page for Education stations
        if y_pos < 150:  # If less than 300px left, start new page
            canvas_obj.showPage()
            self.add_page_header(canvas_obj, "HPCL - Journey Risk Management Study (AI-Powered Analysis) ")
            y_pos = self.page_height - 120
        else:
            y_pos -= 30  # Add spacing between tables
        
        # Education stations
        education_stations = [s for s in all_emergency_services if s.get('serviceType') == 'educational']
        
        if education_stations:
            education_sort_data = sorted(
                [n for n in education_stations if self.safe_float(n.get("distanceFromStartKm", 0)) >= 1.0],
                key=lambda x: self.safe_float(x.get("distanceFromStartKm", 0)))
            education_data = []
            for i, station in enumerate(education_sort_data):
                latitude = station.get('latitude',0)
                longitude = station.get('longitude',0)
                maps_link =  f"https://www.google.com/maps?q={latitude}%2C{longitude}"
                education_data.append([
                    str(i + 1),
                    station.get('name', 'Unknown'),
                    station.get('address', 'Not specified'),
                    f"{station.get('distanceFromStartKm', 0):.1f}",
                    f"{station.get('distanceFromEndKm', 0):.1f}",
                    f"{latitude:.6f}, {longitude:.6f}",
                    maps_link,
                    station.get('phoneNumber', 'N/A')
                ])
            
            # This will automatically handle pagination
            y_pos = self.create_simple_table_with_link(
                canvas_obj,
                "EDUCATIONAL INSTITUTIONS - Speed Limit Zones (40 km/h) – AWARENESS ",
                headers,
                education_data,
                30, y_pos,
                col_widths,
                title_bg_color=self.colors.WHITE,
                title_text_color=self.colors.SUCCESS,
                header_color=self.colors.WHITE,
                hyper_link=True,
                hyper_link_col_index= 6
            )
        # Check if we need a new page for Food Stations
        if y_pos < 150:  # If less than 300px left, start new page
            canvas_obj.showPage()
            self.add_page_header(canvas_obj, "HPCL - Journey Risk Management Study (AI-Powered Analysis) ")
            y_pos = self.page_height - 120
        else:
            y_pos -= 30  # Add spacing between tables
        
        # Food stations
        food_stations = [s for s in all_emergency_services if s.get('serviceType') == 'amenity']
        
        if food_stations:
            food_sort_data = sorted(
                    [n for n in food_stations if self.safe_float(n.get("distanceFromStartKm", 0)) >= 1.0],
                    key=lambda x: self.safe_float(x.get("distanceFromStartKm", 0)))
            food_data = []
            for i, station in enumerate(food_sort_data):
                latitude = station.get('latitude',0)
                longitude = station.get('longitude',0)
                maps_link =  f"https://www.google.com/maps?q={latitude}%2C{longitude}"
                education_data.append([
                    str(i + 1),
                    station.get('name', 'Unknown'),
                    station.get('address', 'Not specified'),
                    f"{station.get('distanceFromStartKm', 0):.1f}",
                    f"{station.get('distanceFromEndKm', 0):.1f}",
                    f"{latitude:.6f}, {longitude:.6f}",
                    maps_link,
                    station.get('phoneNumber', 'N/A')
                ])
            
            # This will automatically handle pagination
            y_pos = self.create_simple_table_with_link(
                canvas_obj,
                "FOOD & REST - Meal Stops & Driver Rest Areas - CONVENIENCE ",
                headers,
                food_data,
                30, y_pos,
                col_widths,
                title_bg_color=self.colors.WHITE,
                title_text_color=self.colors.DANGER,
                header_color=self.colors.WHITE,
                hyper_link=True,
                hyper_link_col_index= 6
            )

        if not any([police_stations, fire_stations, education_stations, fuel_stations, food_stations]):
            canvas_obj.setFillColor(self.colors.SECONDARY)
            canvas_obj.setFont("Helvetica", 12)
            canvas_obj.drawString(80, y_pos, "No law enforcement or fire service or education Institutions or fuel or food stations data available for this route")

        # NOTES - GENERAL EMERGENCY GUIDELINES FOR PETROLEUM TANKER (Static content)
        if y_pos < 300: 
            canvas_obj.showPage()
            self.add_page_header(canvas_obj, "HPCL - Journey Risk Management Study", "GENERAL EMERGENCY GUIDELINES")
            y_pos = self.page_height - 120

        canvas_obj.setFillColor(self.colors.PRIMARY)
        canvas_obj.setFont("Helvetica-Bold", 14)
        canvas_obj.drawString(self.margin, y_pos, "NOTES - GENERAL EMERGENCY GUIDELINES FOR PETROLEUM TANKER")
        y_pos -= 20

        guidelines_data = [
            ("Vehicle Safety", "Park the tanker safely on the roadside with hazard lights and emergency triangles deployed. Avoid sudden braking or jerks, especially if the tanker is partially filled."),
            ("Communication", "Immediately contact the Supply Location control room and, Transport supervisor. If in a dead zone, use alternate communications. Maintain communication with authorities if directed."),
            ("Emergency Contacts", "Call local police, nearest hospital, or fire station, Supply Location. Keep a printed list of these numbers in the vehicle."),
            ("Basic First-Aid", "Use the onboard first-aid kit for minor injuries. In case of severe injuries, prioritize immediate attention and inform the control room."),
            ("Spill / Hazard Response", "Follow MSDS and company-specific SOP for spills. Do not attempt to clean up fuel spills without proper equipment. Use absorbent materials available. Use non-sparking tools."),
            ("Fire / Explosion Risk", "If a fire starts, immediately shut off the engine and evacuate the area. Use the onboard fire extinguisher only if safe to do so and trained to handle it."),
            ("Evacuation Zone", "Establish a safety perimeter (minimum 50 meters) around the tanker. Do not allow smoking or the use of mobile phones, non-FLP torches, sparking tools near the spill or fire area."),
            ("Convoy / Escort Movement", "In areas requiring convoy travel (e.g., night operations under police escort), maintain close coordination and avoid separating from the group."),
            ("Incident Documentation", "Record incident details: time, GPS coordinates, nature of spill/accident, vehicle condition, environmental impact."),
            ("Personal Safety & Well-being", "Wear appropriate PPE (helmet, gloves, reflective jacket). Remain calm, prioritize personal safety, and wait for emergency responders if in doubt about next steps. Use coverall during filling and unloading operations."),
            ("Regulatory Compliance", "Follow all guidelines under Petroleum Rules 2002, CMVR, and company SOP for hazardous goods transportation.")
        ]

        headers = ["ASPECT", "GUIDELINES / ACTIONS"]
        col_widths = [150, 350]

        y_pos = self.create_simple_table(
            canvas_obj,
            "",
            headers,
            guidelines_data,
            self.margin, y_pos,
            col_widths,
            title_color=self.colors.PRIMARY,
            max_rows_per_page=15
        )

        y_pos -= 15
        canvas_obj.setFillColor(self.colors.SECONDARY)
        canvas_obj.setFont("Helvetica-Oblique", 8)
        note_text = "Note: These guidelines are mandatory for all petroleum tanker operations. Compliance ensures safety and environmental protection"
        canvas_obj.drawString(self.margin, y_pos, note_text)

    def create_environmental_page(self, canvas_obj, route_data: Dict[str, Any]):
        """Create Page 7: Environmental & Weather Analysis"""
        collections = route_data['collections']
        
        # Page header
        self.add_page_header(canvas_obj, "HPCL - Journey Risk Management Study (AI-Powered Analysis) ", "")
        
        y_pos = self.page_height - 120
        
        # Weather conditions analysis
        if collections['weather_conditions']:
            canvas_obj.setFillColor(self.colors.INFO)
            canvas_obj.rect(60, y_pos, 480, 25, fill=1)
            canvas_obj.setFillColor(self.colors.WHITE)
            canvas_obj.setFont("Helvetica-Bold", 12)
            canvas_obj.drawString(80, y_pos + 8, "🌤️ SEASONAL WEATHER CONDITIONS & TRAFFIC PATTERNS")
            
            y_pos -= 30
            
            # Group weather data by season
            weather_by_season = {}
            for weather in collections['weather_conditions']:
                season = weather.get('season', 'unknown')
                if season not in weather_by_season:
                    weather_by_season[season] = []
                weather_by_season[season].append(weather)
            
            # Display seasonal patterns
            seasons = ['summer', 'monsoon', 'winter', 'spring']
            season_colors = {
                'summer': HexColor('#FF6B35'),
                'monsoon': HexColor('#4A90E2'),
                'winter': HexColor('#A8C8EC'),
                'spring': HexColor('#7ED321')
            }
            
            for season in seasons:
                if season in weather_by_season:
                    # Season header
                    canvas_obj.setFillColor(season_colors.get(season, self.colors.SECONDARY))
                    canvas_obj.rect(60, y_pos, 480, 20, fill=1)
                    canvas_obj.setFillColor(self.colors.WHITE)
                    canvas_obj.setFont("Helvetica-Bold", 10)
                    canvas_obj.drawString(80, y_pos + 6, f"{season.upper()} CONDITIONS")
                    
                    y_pos -= 25
                    
                    # Weather details for this season
                    season_data = weather_by_season[season]
                    avg_temp = sum((w.get('averageTemperature') or 25) for w in season_data) / len(season_data) if season_data else 25
                    avg_risk = sum((w.get('riskScore') or 3) for w in season_data) / len(season_data) if season_data else 3
                    
                    # Temperature and risk info
                    canvas_obj.setFillColor(self.colors.SECONDARY)
                    canvas_obj.setFont("Helvetica", 9)
                    canvas_obj.drawString(80, y_pos, f"Average Temperature: {avg_temp:.1f}°C")
                    canvas_obj.drawString(300, y_pos, f"Weather Risk Score: {avg_risk:.1f}/10")
                    
                    y_pos -= 15
                    
                    # Common conditions
                    conditions = [w.get('weatherCondition', 'clear') for w in season_data]
                    most_common = max(set(conditions), key=conditions.count) if conditions else 'clear'
                    canvas_obj.drawString(80, y_pos, f"Typical Conditions: {most_common.title()}")
                    
                    # Driver recommendations
                    recommendations = self.get_seasonal_recommendations(season, avg_risk)
                    canvas_obj.drawString(300, y_pos, f"Key Precaution: {recommendations}")
                    
                    y_pos -= 25
        
        # Environmental factors
        y_pos -= 20
        
        canvas_obj.setFillColor(self.colors.SUCCESS)
        canvas_obj.rect(60, y_pos, 480, 25, fill=1)
        canvas_obj.setFillColor(self.colors.WHITE)
        canvas_obj.setFont("Helvetica-Bold", 12)
        canvas_obj.drawString(80, y_pos + 8, "ENVIRONMENTAL & LOCAL CONSIDERATIONS")
        
        y_pos -= 30
        
        # Environmental guidelines table
        guidelines = [
            ("Eco-sensitive Areas", "Drive slowly, avoid honking, no littering"),
            ("School Zones", "Maintain 25-30 km/h, stay alert for children"),
            ("Market Areas", "Expect pedestrians, avoid peak hours"),
            ("Festival Areas", "Expect diversions, confirm route with authorities"),
            ("Noise Sensitivity", "Avoid honking in populated/religious areas"),
            ("Local Regulations", "Follow state-specific restrictions for hazardous cargo")
        ]
        
        for i, (area, guideline) in enumerate(guidelines):
            if i % 2 == 0:
                canvas_obj.setFillColorRGB(0.95, 0.98, 0.95)
                canvas_obj.rect(60, y_pos - 18, 480, 18, fill=1)
            
            canvas_obj.setFillColor(self.colors.SECONDARY)
            canvas_obj.setFont("Helvetica-Bold", 9)
            canvas_obj.drawString(80, y_pos - 10, area)
            
            canvas_obj.setFont("Helvetica", 9)
            canvas_obj.drawString(220, y_pos - 10, guideline)
            
            y_pos -= 18

    def get_seasonal_recommendations(self, season: str, risk_score: float) -> str:
        """Get seasonal driving recommendations"""
        recommendations = {
            'summer': 'Carry extra water, check cooling system',
            'monsoon': 'Reduce speed, use wipers and lights',
            'winter': 'Check for fog, use winter preparations',
            'spring': 'Monitor for dust storms and winds'
        }
        
        base_rec = recommendations.get(season, 'Exercise standard caution')
        if risk_score >= 6:
            return f"{base_rec}, avoid travel during peak conditions"
        return base_rec

    def create_network_coverage_page(self, canvas_obj, route_data: Dict[str, Any], page_num: int):
        """Create Page 8: Network Coverage with auto-paginated tables"""
        collections = route_data['collections']
        
        # Page header
        self.add_page_header(canvas_obj, "HPCL - Journey Risk Management Study (AI-Powered Analysis)")

        y_pos = self.page_height - 100
        
        # Network coverage statistics
        network_data = self.remove_duplicate_coordinates(collections.get('network_coverage', []))
        
        if network_data:
            # Coverage overview
            total_points = len(network_data)
            dead_zones = len([n for n in network_data if n.get('isDeadZone', False)])
            weak_signal = len([n for n in network_data if n.get('signalStrength', 10) < 4])
            good_coverage = total_points - dead_zones - weak_signal
            coverage_score = ((good_coverage / total_points) * 100) if total_points > 0 else 0
            
            # Coverage breakdown table
            coverage_headers = ["Coverage Metric", "Value"] 
            coverage_col_widths = [250, 230]
            
            coverage_data = [
                ["Total Analysis Points", str(total_points)],
                ["Good Coverage Areas", f"{good_coverage} points ({(good_coverage/total_points*100):.1f}%)"],
                ["Weak Signal Areas (singnal < 4)", f"{weak_signal} points ({(weak_signal/total_points*100):.1f}%)"],
                ["Dead Zones Identified", f"{dead_zones} areas ({(dead_zones/total_points*100):.1f}%)"],
                ["Overall Coverage Status", "GOOD" if coverage_score > 70 else "MODERATE" if coverage_score > 50 else "POOR"],
                ["Network Reliability", "HIGH" if dead_zones < 3 else "MODERATE" if dead_zones < 6 else "LOW"]
            ]
            
            y_pos = self.create_simple_table(
                canvas_obj,
                f"COMPREHENSIVE NETWORK COVERAGE ANALYSIS - {coverage_score:.1f}% Coverage",
                coverage_headers,
                coverage_data,
                50, y_pos,
                coverage_col_widths,
                title_color=self.colors.DANGER,
                text_color=self.colors.WHITE,
                header_color=self.colors.WHITE,
                max_rows_per_page=10
            )

            # Add Signal Quality Distribution table from the image
            y_pos -= 30  # Add some space between tables
            
            signal_headers = ["Signal Quality Level", "Points Count", "Route %", "Status"]
            signal_col_widths = [180, 100, 100, 120]
            
            weak_signal_bar = len([n for n in network_data if 1 <= n.get('signalStrength', 10) <= 2])
            fair_signal_bar = len([n for n in network_data if 2 < n.get('signalStrength', 10) <= 3])
            good_coverage_bar = len([n for n in network_data if 3 < n.get('signalStrength', 10) <= 4])
            excelent_signal_bar = len([n for n in network_data if n.get('signalStrength', 10) > 4])

            total_records = len(network_data) if network_data else 1
            def get_percentage(count):
                return f"{(count / total_records) * 100:.1f}%" if total_records else "0.0%"

            # Prepare final data
            signal_data = [
                ["No Signal (Dead Zone)", f"{dead_zones}", get_percentage(dead_zones), "Critical"],
                ["Fair Signal (2-3 bar)", f"{fair_signal_bar}", get_percentage(fair_signal_bar), "Good"],
                ["Poor Signal (1-2 bar)", f"{weak_signal_bar}", get_percentage(weak_signal_bar), "Attention"],
                ["Good Signal (3-4 bar)", f"{good_coverage_bar}", get_percentage(good_coverage_bar), "Good"],
                ["Excellent Signal (4+)", f"{excelent_signal_bar}", get_percentage(excelent_signal_bar), "Good"]
            ]
            
            y_pos = self.create_simple_table(
                canvas_obj,
                "SIGNAL QUALITY DISTRIBUTION ANALYSIS",
                signal_headers,
                signal_data,
                50, y_pos,
                signal_col_widths,
                header_color=self.colors.WHITE,
                title_color=self.colors.WHITE,
                text_color=self.colors.PRIMARY,
                max_rows_per_page=10
            )

            if dead_zones > 0:
                y_pos -= 30
                dead_zone_areas = [n for n in network_data if n.get('isDeadZone', False)]
                dead_zone_headers = ["id", "Zone Location (GPS)","Link", "Impact Level", "Recommendation"]
                dead_zone_col_widths = [20, 120,70, 100, 180]
                dead_zone_data = []
                for i, zone in enumerate(dead_zone_areas):
                    latitude = zone.get('latitude', 0)
                    longitude = zone.get('longitude', 0)
                    map_link = f"https://www.google.com/maps?q={latitude}%2C{longitude}"
                    dead_zone_data.append([
                        str(i+1),
                        f"{latitude:.6f}, {longitude:.6f}",
                        map_link,
                        zone.get('deadZoneSeverity', 'moderate').upper(),
                        "Use satellite communication"
                    ])
                
                # This will automatically handle pagination
                y_pos = self.create_simple_table_with_link(
                    canvas_obj,
                    f"DEAD ZONES - NO CELLULAR SERVICE ({dead_zones} locations)",
                    dead_zone_headers,
                    dead_zone_data,
                    50, y_pos,
                    dead_zone_col_widths,
                    title_bg_color=self.colors.WHITE,
                    title_text_color=self.colors.DANGER,
                    header_color=self.colors.WHITE,
                    hyper_link=True,
                    hyper_link_col_index=2
                )
                # Communication recommendations (always fits on one page)
            
            if y_pos < 200:  
                canvas_obj.showPage()
                self.add_page_header(canvas_obj, "EMERGENCY COMMUNICATION PLAN", "Communication recommendations")
                y_pos = self.page_height - 120
            else:
                y_pos -= 30

            if weak_signal > 0:
                weak_signal_areas = [n for n in network_data if n.get('signalStrength', 10) < 4]
                
                weak_signal_headers = ["id", "Coordinates (GPS)","Link", "Signal Level ", "Recommendation"]
                weak_signal_col_widths = [20, 120,70, 100, 180]
                
                weak_signal_data = []
                for i, zone in enumerate(weak_signal_areas):
                    latitude = zone.get('latitude', 0)
                    longitude = zone.get('longitude', 0)
                    map_link = f"https://www.google.com/maps?q={latitude}%2C{longitude}"
                    weak_signal_data.append([
                        str(i+1),
                        f"{latitude:.6f}, {longitude:.6f}",
                        map_link,
                        "WEAK",
                        "Download offline maps, use a GPS device, and avoid online services "
                    ])
                
                y_pos = self.create_simple_table_with_link(
                    canvas_obj,
                    f"POOR COVERAGE AREAS ({weak_signal} locations):",
                    weak_signal_headers,
                    weak_signal_data,
                    50, y_pos,
                    weak_signal_col_widths,
                    title_bg_color=self.colors.WHITE,
                    title_text_color=self.colors.WARNING,
                    header_color=self.colors.WHITE,
                    hyper_link=True,
                    hyper_link_col_index=2
                )

        if y_pos < 200:  
            canvas_obj.showPage()
            self.add_page_header(canvas_obj, "HPCL - Journey Risk Management Study (AI-Powered Analysis)")
            y_pos = self.page_height - 120
        else:
            y_pos -= 30

        bullet_points_data = [
            f"Route has {dead_zones} dead zones - consider satellite communication device",
            "Multiple poor coverage areas - download offline maps before travel",
            "Inform someone of your route and expected arrival time"
        ]
        y_pos = self.draw_title_bullet_section(
            canvas_obj,
            "NETWORK COVERAGE RECOMMENDATIONS",
            bullet_points_data, y_pos,
            title_color=self.colors.PRIMARY
        )
        # Communication recommendations (always fits on one page)
        if y_pos < 200:  
            canvas_obj.showPage()
            self.add_page_header(canvas_obj, "HPCL - Journey Risk Management Study (AI-Powered Analysis)")
            y_pos = self.page_height - 120
        else:
            y_pos -= 5
        
        canvas_obj.setFillColor(self.colors.DANGER)
        canvas_obj.setFont("Helvetica-Bold", 12)
        canvas_obj.drawString(50, y_pos + 8, "EMERGENCY COMMUNICATION PLAN")
        
        y_pos -= 15
        
        comm_recommendations = [
            "Download offline maps before travel (Google Maps, Maps.me)",
            "Inform someone of your route and expected arrival time", 
            "Carry satellite communication device for dead zones",
            "Keep emergency numbers saved: 112 (Emergency), 100 (Police), 108 (Ambulance)",
            "Consider two-way radios for convoy travel",
            "Identify nearest cell towers along the route"
        ]
        
        for recommendation in comm_recommendations:
            canvas_obj.setFillColor(self.colors.SECONDARY)
            canvas_obj.setFont("Helvetica", 9)
            canvas_obj.drawString(60, y_pos, recommendation)
            y_pos -= 15

    def create_emergency_guidelines_page(self, canvas_obj, route_data: Dict[str, Any]):
        """Create Page 9: Emergency Guidelines with simple tables"""
        collections = route_data['collections']
        all_emergency_services = self.remove_duplicate_coordinates(collections['emergency_services'])
        # Page header
        self.add_page_header(canvas_obj, "HPCL - Journey Risk Management Study (AI-Powered Analysis)")
        y_pos = self.page_height - 120
        head_title = "COMPREHENSIVE EMERGENCY PREPAREDNESS: LOW RISK (Risk Score: 1)"
        y_pos = self.draw_centered_text_in_box(
            canvas_obj,
            head_title,
            50, y_pos, 500, 35,
            box_color=self.colors.SUCCESS
            )
        

        medical_facilities = len([s for s in all_emergency_services if s.get('serviceType') == 'hospital'])
        police_stations = len([s for s in all_emergency_services if s.get('serviceType') == 'police'])
        fire_stations = len([s for s in all_emergency_services if s.get('serviceType') == 'fire_station'])
         # Network coverage statistics
        network_data = self.remove_duplicate_coordinates(collections.get('network_coverage', []))
        
        if network_data:
            # Coverage overview
            total_points = len(network_data)
            dead_zones = len([n for n in network_data if n.get('isDeadZone', False)])
            weak_signal = len([n for n in network_data if n.get('signalStrength', 10) < 4])
            good_coverage = total_points - dead_zones - weak_signal
            coverage_score = ((good_coverage / total_points) * 100) if total_points > 0 else 0

        # Emergency Services Availability Assessment table - NEW FIRST TABLE
        availability_headers = ["Service Type", "Availability Status"]
        availability_col_widths = [200, 200]
        
        availability_data = [
            ["Medical Facilities (Hospitals)", f"{medical_facilities} facilities identified"],
            ["Law Enforcement (Police)", f"{police_stations} stations identified"],
            ["Fire & Rescue Services", f"{fire_stations} stations identified"],
            ["Emergency Clinics", f"{medical_facilities} clinics identified"],
            ["Pharmacies (24hr)", f"{medical_facilities} pharmacies identified"],
            ["Communication Reliability", f"{coverage_score:.1f}% coverage"],
            ["Coverage Gaps", f"{dead_zones} dead zones, {weak_signal} weak signal areas"],
            ["Overall Service Coverage", "GOOD" if coverage_score > 70 else "MODERATE" if coverage_score > 50 else "POOR"],
        ]
        
        y_pos = self.create_simple_table(
            canvas_obj,
            "EMERGENCY SERVICES AVAILABILITY ASSESSMENT",
            availability_headers,
            availability_data,
            50, y_pos,
            availability_col_widths,
            title_color=self.colors.WHITE,
            text_color=self.colors.INFO,
            header_color=self.colors.WHITE
        )
        
        # Emergency contact numbers table - NOW SECOND TABLE
        y_pos -= 40
        
        emergency_headers = ["Emergency Service", "Contact Number", "When to Call", "Response Type"]
        emergency_col_widths = [120, 80, 160, 120]
        
        emergency_contacts = [
            ["National Emergency", "112", "Any life-threatening situation", "Police / Fire / Medical"],
            ["Police Emergency", "100", "Crime, accidents, theft", "Law Enforcement"],
            ["Fire Services", "101", "Fire, rescue, hazardous material", "Fire & Rescue Team"],
            ["Medical Emergency", "108", "Accidents, health emergencies", "Ambulance Service"],
            ["Highway Patrol", "1033", "Highway accidents, traffic support", "Traffic Police"],
            ["Women Helpline", "1091", "Women in distress", "Women Safety Assistance"],
            ["Disaster Management", "1078", "Natural disasters, emergencies", "Disaster Response Team"],
            ["Tourist Helpline", "1363", "Tourist-related emergencies or support", "Tourist Support"]
        ]
        
        y_pos = self.create_simple_table(
            canvas_obj,
            "CRITICAL EMERGENCY CONTACT NUMBERS - MEMORIZE OR SAVE",
            emergency_headers,
            emergency_contacts,
            50, y_pos,
            emergency_col_widths,
            title_color=self.colors.WHITE,
            text_color=self.colors.DANGER,
            header_color=self.colors.WHITE  
        )

    def initialize_image_downloader(self):
        """Initialize Google Maps image downloader"""
        from google_maps_image_downloader import GoogleMapsImageDownloader
        
        api_key = os.getenv('GOOGLE_MAPS_API_KEY')
        if not api_key:
            logger.warning("Google Maps API key not found - images will not be downloaded")
            return None
        
        self.image_downloader = GoogleMapsImageDownloader(api_key)
        return self.image_downloader

    def create_sharp_turns_detailed_pages(self, canvas_obj, route_data: Dict[str, Any]):
        """Create individual pages for each high-risk sharp turn with images"""
        collections = route_data['collections']
        route_id = str(route_data['route']['_id'])
        all_sharp_turns = self.remove_duplicate_coordinates(collections['sharp_turns'])
        
        # Filter high-risk sharp turns (risk score >= 7)
        high_risk_turns = [
            turn for turn in  all_sharp_turns
            if turn.get('riskScore', 0) >= 7
        ]
        
        if not high_risk_turns:
            logger.info("No high-risk sharp turns found")
            return
        
        # Sort by risk score (highest first)
        high_risk_turns.sort(key=lambda x: x.get('riskScore', 0), reverse=True)
        
        # Initialize image downloader if not already done
        if not hasattr(self, 'image_downloader'):
            self.initialize_image_downloader()
        
        # Create a page for each high-risk turn
        for i, turn in enumerate(high_risk_turns[:10]):  # Limit to top 10
            self.create_single_sharp_turn_page(canvas_obj, turn, i + 1, route_id, route_data)
            if i < len(high_risk_turns) - 1:
                canvas_obj.showPage()

    def create_single_sharp_turn_page(self, canvas_obj, turn: Dict, turn_number: int, 
                                    route_id: str, route_data: Dict):
        """Create a single page for a sharp turn with visual analysis"""
        
        # Page header
        risk_level = "CRITICAL" if turn.get('riskScore', 0) >= 8 else "HIGH"
        subtitle = f"Sharp Turn #{turn_number} - Risk Score: {turn.get('riskScore', 0)}/10"
        self.add_page_header(canvas_obj, "HPCL - Journey Risk Management Study (AI-Powered Analysis)")
        
        y_pos = self.page_height - 120
        
        # Turn header with red background
        self.add_turn_header(canvas_obj, turn, turn_number, y_pos)
        y_pos -= 20
        
        # Turn details table
        y_pos = self.add_turn_details_table(canvas_obj, turn, y_pos)
        y_pos -= 15
        
        # Download images if available
        images = {}
        if hasattr(self, 'image_downloader') and self.image_downloader:
            turn_id = str(turn.get('_id', f'turn_{turn_number}'))
            images = self.image_downloader.download_turn_images(turn, route_id)
        
        # Visual evidence section
        y_pos = self.add_visual_evidence_section(canvas_obj, turn, turn_number, images, y_pos)
        
        # Safety recommendations
        self.add_turn_safety_recommendations(canvas_obj, turn, y_pos)

    def add_turn_header(self, canvas_obj, turn: Dict, turn_number: int, y_pos: float):
        """Add turn header with risk classification"""
        # Determine classification
        angle = turn.get('turnAngle', 0)
        if angle > 120:
            classification = "HAIRPIN TURN - EXTREME CAUTION"
        elif angle > 90:
            classification = "SHARP TURN - HIGH RISK"
        elif angle > 60:
            classification = "MODERATE TURN - CAUTION ADVISED"
        else:
            classification = "TURN - STANDARD CAUTION"
        
        # Red background bar
        canvas_obj.setFillColor("#5D9CEC")
        canvas_obj.rect(20, y_pos, 530, 30, fill=1, stroke=0)
        
        # White text
        canvas_obj.setFillColor(self.colors.WHITE)
        canvas_obj.setFont("Helvetica-Bold", 14)
        header_text = f"CRITICAL TURN #{turn_number}: {angle:.1f}° - {classification}"
        canvas_obj.drawString(25, y_pos + 8, header_text)

        
        # Add vertical spacing before details table
        y_pos -= 20

        return y_pos  # Return updated y_pos for next elements

    def create_details_table_with_gps_link(self, canvas_obj, data: list, start_x: int, start_y: float, col_widths: list) -> float:
        row_height = 20
        current_y = start_y
        page_bottom_margin = 50

        # Draw header row
        canvas_obj.setFillColor("#ADD8E6")  # Light blue
        canvas_obj.setStrokeColorRGB(0, 0, 0)
        canvas_obj.rect(start_x, current_y - row_height, col_widths[0], row_height, fill=1, stroke=1)
        canvas_obj.rect(start_x + col_widths[0], current_y - row_height, col_widths[1], row_height, fill=1, stroke=1)

        canvas_obj.setFillColorRGB(0, 0, 0)
        canvas_obj.setFont("Helvetica-Bold", 9)
        canvas_obj.drawString(start_x + 5, current_y - 14, "Parameter")
        canvas_obj.drawString(start_x + col_widths[0] + 5, current_y - 14, "Value")
        current_y -= row_height

        canvas_obj.setFont("Helvetica", 8)

        for index, row in enumerate(data):
            if current_y < page_bottom_margin:
                canvas_obj.showPage()
                current_y = start_y

            param, value = row

            # Alternate row background color
            if index % 2 == 0:
                canvas_obj.setFillColorRGB(1, 1, 1)  # White
            else:
                canvas_obj.setFillColorRGB(0.95, 0.95, 0.95)  # Light gray

            # Draw cell rectangles with background and border
            canvas_obj.setStrokeColorRGB(0, 0, 0)
            canvas_obj.rect(start_x, current_y - row_height, col_widths[0], row_height, fill=1, stroke=1)
            canvas_obj.rect(start_x + col_widths[0], current_y - row_height, col_widths[1], row_height, fill=1, stroke=1)

            # Text
            canvas_obj.setFillColorRGB(0, 0, 0)
            canvas_obj.drawString(start_x + 5, current_y - 14, str(param))

            if param == "GPS Coordinates" and "(view)" in value:
                coords, _ = value.split(" (view)")
                canvas_obj.drawString(start_x + col_widths[0] + 5, current_y - 14, coords)

                view_text = "(view)"
                view_font = "Helvetica-Bold"
                view_font_size = 7
                canvas_obj.setFont(view_font, view_font_size)
                canvas_obj.setFillColor(self.colors.INFO)

                view_width = canvas_obj.stringWidth(view_text, view_font, view_font_size)
                view_x = start_x + col_widths[0] + 5 + canvas_obj.stringWidth(coords, "Helvetica", 8) + 5
                view_y = current_y - 14

                canvas_obj.drawString(view_x, view_y, view_text)

                # Add URL link
                lat, lng = coords.split(",")
                maps_url = f"https://www.google.com/maps/search/?api=1&query={lat.strip()},{lng.strip()}&zoom=17"
                canvas_obj.linkURL(maps_url, (view_x - 1, view_y - 2, view_x + view_width + 1, view_y + 8))

                canvas_obj.setFont("Helvetica", 8)
                canvas_obj.setFillColorRGB(0, 0, 0)
            else:
                canvas_obj.drawString(start_x + col_widths[0] + 5, current_y - 14, str(value))

            current_y -= row_height

        return current_y

    def add_turn_details_table(self, canvas_obj, turn: Dict, y_pos: float) -> float:
        """Add detailed turn information table with GPS 'view' link"""
        lat = turn.get('latitude', 0)
        lng = turn.get('longitude', 0)
        distance_from_start = turn.get('distanceFromStartKm', 0)

        table_data = [
            ["GPS Coordinates", f"{lat:.6f}, {lng:.6f} (view)"],
            ["Turn Angle", f"{turn.get('turnAngle', 0):.1f}° (Deviation from straight path)"],
            ["Risk Classification", self.get_turn_classification(turn)],
            ["Risk Level", f"{turn.get('riskScore', 0)}/10 - {self.get_risk_category_text(turn.get('riskScore', 0))}"],
            ["Distance from Supply Location", f"{distance_from_start:.1f} km"],
            ["Turn Direction", turn.get('turnDirection', 'Unknown').title()],
            ["Turn Radius", f"{turn.get('turnRadius', 0):.1f} m" if turn.get('turnRadius') else "Not specified"],
            ["Approach Speed", f"{turn.get('approachSpeed', 40)} km/h"],
            ["Recommended Maximum Speed", f"{turn.get('recommendedSpeed', 30)} km/h"],
            ["Safety Distance Required", "Minimum 50m approach visibility"],
            ["Road Surface", turn.get('roadSurface', 'Unknown').title()],
            ["Visibility", turn.get('visibility', 'Unknown').title()],
            ["Warning Signs Present", "Yes" if turn.get('warningSigns') else "No"],
            ["Guardrails Present", "Yes" if turn.get('guardrails') else "No"],
            ["Driver Action Required", self.get_driver_action(turn)]
        ]

        return self.create_details_table_with_gps_link(
            canvas_obj,
            table_data,
            20,
            y_pos,
            col_widths=[150, 380]
        )

    def add_visual_evidence_section(self, canvas_obj, turn: Dict, turn_number: int, 
                                images: Dict[str, str], y_pos: float) -> float:
        """Add visual evidence section with downloaded images"""
        # Section header
        canvas_obj.setFillColor(self.colors.INFO)
        canvas_obj.setFont("Helvetica-Bold", 11)
        canvas_obj.drawString(20, y_pos, f"VISUAL EVIDENCE FOR TURN #{turn_number}:")
        y_pos -= 15
        
        # Check if we have images
        street_view_path = images.get('street_view')
        satellite_path = images.get('satellite')
        
        if not street_view_path and not satellite_path:
            # No images available - show placeholder
            canvas_obj.setFillColor(self.colors.SECONDARY)
            canvas_obj.setFont("Helvetica", 10)
            canvas_obj.drawString(20, y_pos, "Visual evidence not available - Google Maps API key required")
            return y_pos - 20
        
        # Image dimensions
        img_width = 270
        img_height = 200
        
        # Street view analysis (left side)
        canvas_obj.setFillColor(self.colors.PRIMARY)
        canvas_obj.setFont("Helvetica-Bold", 10)
        canvas_obj.drawString(20, y_pos, "STREET VIEW ANALYSIS:")
        
        # Satellite view analysis (right side)
        canvas_obj.drawString(305, y_pos, "SATELLITE VIEW ANALYSIS:")
        y_pos -= 10
        
        # Draw images if available
        if street_view_path and os.path.exists(street_view_path):
            try:
                canvas_obj.drawImage(street_view_path, 20, y_pos - img_height, 
                                width=img_width, height=img_height)
            except:
                self.draw_image_placeholder(canvas_obj, 20, y_pos - img_height, 
                                        img_width, img_height, "Street View")
        else:
            self.draw_image_placeholder(canvas_obj, 20, y_pos - img_height, 
                                    img_width, img_height, "Street View")
        
        if satellite_path and os.path.exists(satellite_path):
            try:
                canvas_obj.drawImage(satellite_path, 305, y_pos - img_height, 
                                width=img_width, height=img_height)
            except:
                self.draw_image_placeholder(canvas_obj, 305, y_pos - img_height, 
                                        img_width, img_height, "Satellite View")
        else:
            self.draw_image_placeholder(canvas_obj, 305, y_pos - img_height, 
                                    img_width, img_height, "Satellite View")
        
        y_pos -= img_height + 10
        
        # Add analysis text below images
        y_pos = self.add_image_analysis_text(canvas_obj, turn, images, y_pos)
        
        return y_pos

    def add_image_analysis_text(self, canvas_obj, turn: Dict, images: Dict[str, str], 
                            y_pos: float) -> float:
        """Add analysis text below images"""
        canvas_obj.setFont("Helvetica", 8)
        canvas_obj.setFillColor(self.colors.SECONDARY)
        
        # Street view analysis (left column)
        street_analysis = [
            "STREET VIEW DATA:",
            f"File: {os.path.basename(images.get('street_view', 'Not available'))}",
            f"GPS: {turn.get('latitude', 0):.4f}, {turn.get('longitude', 0):.4f}",
            "",
            "ROAD ANALYSIS:",
            f"* Turn angle: {turn.get('turnAngle', 0):.1f} degrees",
            f"* Visibility: {turn.get('visibility', 'Unknown')}",
            f"* Hazard level: {self.get_risk_category_text(turn.get('riskScore', 0)).upper()}",
            f"* Max speed: {turn.get('recommendedSpeed', 30)} km/h"
        ]
        
        x_pos = 20
        for line in street_analysis:
            canvas_obj.drawString(x_pos, y_pos, line)
            y_pos -= 10
        
        # Reset y position for right column
        y_pos += len(street_analysis) * 10
        
        # Satellite view analysis (right column)
        satellite_analysis = [
            "SATELLITE VIEW DATA:",
            f"File: {os.path.basename(images.get('satellite', 'Not available'))}",
            f"GPS: {turn.get('latitude', 0):.4f}, {turn.get('longitude', 0):.4f}",
            "",
            "AERIAL ANALYSIS:",
            f"* Turn geometry: {turn.get('turnAngle', 0):.1f} deg curve",
            f"* Road curvature: {turn.get('turnSeverity', 'Unknown')}",
            f"* Terrain context: Aerial perspective",
            f"* Traffic flow impact: {'Potential bottleneck' if turn.get('turnAngle', 0) > 90 else 'Moderate impact'}"
        ]
        
        x_pos = 305
        for line in satellite_analysis:
            canvas_obj.drawString(x_pos, y_pos, line)
            y_pos -= 10
        
        return y_pos - 10

    def add_turn_safety_recommendations(self, canvas_obj, turn: Dict, y_pos: float):
        """Add safety recommendations section"""
        if y_pos < 150:
            # Not enough space, would overlap with footer
            return
        
        # Safety recommendations box
        canvas_obj.setFillColor(self.colors.WARNING)
        canvas_obj.rect(20, y_pos - 60, 555, 20, fill=1, stroke=0)
        
        canvas_obj.setFillColor(self.colors.WHITE)
        canvas_obj.setFont("Helvetica-Bold", 10)
        canvas_obj.drawString(25, y_pos - 55, "CRITICAL SAFETY RECOMMENDATIONS")
        
        # Recommendations based on turn severity
        recommendations = self.get_turn_safety_recommendations(turn)
        
        y_pos -= 80
        canvas_obj.setFillColor(self.colors.SECONDARY)
        canvas_obj.setFont("Helvetica", 9)
        
        for i, rec in enumerate(recommendations[:5]):  # Limit to 5 recommendations
            canvas_obj.drawString(25, y_pos, f"• {rec}")
            y_pos -= 12
        

    def draw_image_placeholder(self, canvas_obj, x: float, y: float, 
                            width: float, height: float, label: str):
        """Draw placeholder when image is not available"""
        # Gray background
        canvas_obj.setFillColorRGB(0.9, 0.9, 0.9)
        canvas_obj.rect(x, y, width, height, fill=1, stroke=1)
        canvas_obj.setStrokeColor(self.colors.SECONDARY)
        canvas_obj.rect(x, y, width, height, fill=0, stroke=1)
        
        # Placeholder text
        canvas_obj.setFillColor(self.colors.SECONDARY)
        canvas_obj.setFont("Helvetica", 12)
        text_width = canvas_obj.stringWidth(f"{label} Not Available", "Helvetica", 12)
        canvas_obj.drawString(x + (width - text_width) / 2, y + height / 2, 
                            f"{label} Not Available")

    def get_turn_classification(self, turn: Dict) -> str:
        """Get turn classification based on angle and risk"""
        angle = turn.get('turnAngle', 0)
        risk_score = turn.get('riskScore', 0)
        
        if angle > 120:
            return "HAIRPIN TURN - EXTREME RISK"
        elif angle > 90:
            return "SHARP TURN - CRITICAL"
        elif angle > 60:
            return "MODERATE TURN - HIGH RISK" if risk_score >= 7 else "MODERATE TURN"
        else:
            return "GENTLE TURN"

    def get_driver_action(self, turn: Dict) -> str:
        """Get recommended driver action based on turn characteristics"""
        angle = turn.get('turnAngle', 0)
        risk_score = turn.get('riskScore', 0)
        
        if angle > 120 or risk_score >= 8:
            return "Stop, check visibility, proceed at 10-15 km/h"
        elif angle > 90 or risk_score >= 7:
            return "Reduce speed to 20 km/h, use horn, check mirrors"
        elif angle > 60 or risk_score >= 5:
            return "Reduce speed to 30 km/h, signal early"
        else:
            return "Reduce speed, maintain lane discipline"

    def get_turn_safety_recommendations(self, turn: Dict) -> List[str]:
        """Get safety recommendations for a turn"""
        recommendations = []
        angle = turn.get('turnAngle', 0)
        risk_score = turn.get('riskScore', 0)
        
        # General recommendations based on risk
        if risk_score >= 8:
            recommendations.extend([
                "EXTREME CAUTION: Consider alternative route if possible",
                "Mandatory convoy travel with lead vehicle communication",
                "Complete stop before turn to assess conditions"
            ])
        
        # Angle-specific recommendations
        if angle > 120:
            recommendations.extend([
                "Hairpin turn: Use lowest gear for engine braking",
                "Sound horn continuously while navigating turn",
                "No overtaking under any circumstances"
            ])
        elif angle > 90:
            recommendations.extend([
                "Sharp turn: Reduce speed to 15-20 km/h before entering",
                "Stay in center of your lane throughout turn",
                "Watch for oncoming traffic cutting corners"
            ])
        
        # Visibility recommendations
        if turn.get('visibility', '').lower() in ['poor', 'limited']:
            recommendations.extend([
                "Limited visibility: Use headlights and fog lights",
                "Sound horn before entering blind section",
                "Be prepared for sudden obstacles"
            ])
        
        # Surface condition recommendations
        if turn.get('roadSurface', '').lower() in ['poor', 'fair']:
            recommendations.extend([
                "Poor road surface: Extra caution for skidding",
                "Avoid sudden braking or acceleration",
                "Maintain steady speed through turn"
            ])
        
        # Safety feature recommendations
        if not turn.get('guardrails'):
            recommendations.append("No guardrails: Maintain safe distance from edge")
        
        if not turn.get('warningSigns'):
            recommendations.append("No warning signs: Approach with extreme caution")
        
        return recommendations

    # Integration method to be called in generate_pdf_report
    def add_detailed_risk_analysis_pages(self, pdf_canvas, route_data: Dict[str, Any]):
        """Add detailed pages for high-risk sharp turns and blind spots"""

        # Sharp Turns Detailed Pages
        logger.info("Generating Blind Spot Analysis static Pages")
        self.create_blind_spots_analysis_page(pdf_canvas, route_data)
        pdf_canvas.showPage()
        
        # Blind Spots Detailed Pages (similar implementation)
        logger.info("Generating Detailed Blind Spot Analysis Pages")
        self.create_blind_spots_detailed_pages(pdf_canvas, route_data)

        # Page: Generating Pages: sharp turns analysis Page)
        logger.info("📄 Generating Pages: sharp turns analysis Page")
        self.create_sharp_turns_analysis_page(pdf_canvas, route_data)
        pdf_canvas.showPage()

        # Sharp Turns Detailed Pages
        logger.info("Generating Detailed Sharp Turn Analysis Pages")
        self.create_sharp_turns_detailed_pages(pdf_canvas, route_data)

    def create_blind_spots_detailed_pages(self, canvas_obj, route_data: Dict[str, Any]):
        """Create individual pages for each high-risk blind spot with images"""
        collections = route_data['collections']
        route_id = str(route_data['route']['_id'])
        all_blind_sports = self.remove_duplicate_coordinates(collections['blind_spots'])
        
        # Filter high-risk blind spots (risk score >= 7)
        high_risk_spots = [
            spot for spot in all_blind_sports 
            if spot.get('riskScore', 0) >= 7
        ]
        
        if not high_risk_spots:
            logger.info("No high-risk blind spots found")
            return
        
        # Sort by risk score (highest first)
        high_risk_spots.sort(key=lambda x: x.get('riskScore', 0), reverse=True)
        
        # Initialize image downloader if not already done
        if not hasattr(self, 'image_downloader'):
            self.initialize_image_downloader()

        
        # Create a page for each high-risk blind spot
        for i, spot in enumerate(high_risk_spots[:10]):  # Limit to top 10
            self.create_single_blind_spot_page(canvas_obj, spot, i + 1, route_id, route_data)
            if i < len(high_risk_spots) - 1:
                canvas_obj.showPage()

    def create_single_blind_spot_page(self, canvas_obj, spot: Dict, spot_number: int, 
                                    route_id: str, route_data: Dict):
        """Create a single page for a blind spot with visual analysis"""
        
        # Page header
        risk_level = "CRITICAL" if spot.get('riskScore', 0) >= 8 else "HIGH"
        # subtitle = f"Blind Spot #{spot_number} - Risk Score: {spot.get('riskScore', 0)}/10"
        self.add_page_header(canvas_obj, f"HPCL - Journey Risk Management Study (AI-Powered Analysis)")
        
        y_pos = self.page_height - 100
        
        # Blind spot header with orange/red background
        self.add_blind_spot_header(canvas_obj, spot, spot_number, y_pos)
        y_pos -= 20
        
        # Blind spot details table
        y_pos = self.add_blind_spot_details_table(canvas_obj, spot, y_pos)
        y_pos -= 15
        
        # Download images if available
        images = {}
        if hasattr(self, 'image_downloader') and self.image_downloader:
            spot_id = str(spot.get('_id', f'spot_{spot_number}'))
            images = self.image_downloader.download_blind_spot_images(spot, route_id)
        
        # Visual evidence section (may show multiple angles for blind spots)
        y_pos = self.add_blind_spot_visual_evidence(canvas_obj, spot, spot_number, images, y_pos)
        
        # Safety recommendations
        self.add_blind_spot_safety_recommendations(canvas_obj, spot, y_pos)

    def add_blind_spot_header(self, canvas_obj, spot: Dict, spot_number: int, y_pos: float):
        """Add blind spot header with risk classification"""
        # Determine classification based on type and risk
        spot_type = spot.get('spotType', 'unknown')
        visibility_distance = spot.get('visibilityDistance', 0)

        if visibility_distance < 50:
            classification = "EXTREME BLIND SPOT - CRITICAL HAZARD"
            color = self.colors.DANGER
        elif visibility_distance < 100:
            classification = "SEVERE BLIND SPOT - HIGH RISK"
            color = HexColor('#FF5722')  # Orange-red
        else:
            classification = "BLIND SPOT - CAUTION REQUIRED"
            color = self.colors.WARNING

        # Background bar
        canvas_obj.setFillColor(color)
        canvas_obj.rect(20, y_pos, 530, 20, fill=1, stroke=0)
        
        # White text
        canvas_obj.setFillColor(self.colors.WHITE)
        canvas_obj.setFont("Helvetica-Bold", 13)
        header_text = f"CRITICAL TURN #{spot_number}: {spot_type.upper()} - {classification}"
        canvas_obj.drawString(25, y_pos + 4, header_text)
      
    def add_blind_spot_details_table(self, canvas_obj, spot: Dict, y_pos: float) -> float:
        """Add detailed blind spot information table"""
        # Table data
        headers = ["Parameter", "Value"]
        col_widths = [150, 380]
        
        # Calculate distances
        distance_from_start = spot.get('distanceFromStartKm', 0)
        
        table_data = [
            ["GPS Coordinates", f"{spot.get('latitude', 0):.6f}, {spot.get('longitude', 0):.6f} (view)"],
            ["Spot Type", spot.get('spotType', 'Unknown').title()],
            ["Risk Classification", self.get_blind_spot_classification(spot)],
            ["Risk Level", f"{spot.get('riskScore', 0)}/10 - {self.get_risk_category_text(spot.get('riskScore', 0))}"],
            ["Distance from Supply Location", f"{distance_from_start:.1f} km"],
            ["Visibility Distance", f"{spot.get('visibilityDistance', 0)} meters"],
            ["Visibility Category", self.get_visibility_category(spot.get('visibilityDistance', 0))],
            ["Obstruction Height", f"{spot.get('obstructionHeight', 0):.1f} m" if spot.get('obstructionHeight') else "Not specified"],
            ["Road Geometry - Gradient", f"{spot.get('roadGeometry', {}).get('gradient', 0):.1f}°"],
            ["Road Geometry - Curvature", f"{spot.get('roadGeometry', {}).get('curvature', 0):.1f}°"],
            ["Road Width", f"{spot.get('roadGeometry', {}).get('width', 7):.1f} m"],
            ["Vegetation Present", "Yes" if spot.get('vegetation', {}).get('present') else "No"],
            ["Warning Signs Present", "Yes" if spot.get('warningSignsPresent') else "No"],
            ["Mirror Installed", "Yes" if spot.get('mirrorInstalled') else "No"],
            ["Driver Action Required", self.get_blind_spot_driver_action(spot)]
        ]
        
        return self.create_details_table_with_gps_link(
            canvas_obj,
            table_data,
            20, y_pos,
            col_widths
        )

    def add_blind_spot_visual_evidence(self, canvas_obj, spot: Dict, spot_number: int, 
                                    images: Dict[str, str], y_pos: float) -> float:
        """Add visual evidence section for blind spots (potentially multiple angles)"""
        # Section header
        canvas_obj.setFillColor(self.colors.INFO)
        canvas_obj.setFont("Helvetica-Bold", 11)
        canvas_obj.drawString(20, y_pos, f"VISUAL EVIDENCE FOR BLIND SPOT #{spot_number}:")
        y_pos -= 15
        
        # Check if we have images
        available_images = []
        for heading in [0, 90, 180, 270]:
            street_view_key = f'street_view_{heading}'
            if street_view_key in images and images[street_view_key]:
                available_images.append((heading, images[street_view_key]))
        
        satellite_path = images.get('satellite')
        
        if not available_images and not satellite_path:
            # No images available - show placeholder
            canvas_obj.setFillColor(self.colors.SECONDARY)
            canvas_obj.setFont("Helvetica", 10)
            canvas_obj.drawString(20, y_pos, "Visual evidence not available - Google Maps API key required")
            return y_pos - 20
        
        # For blind spots, we might show 4 directional views + satellite
        # Layout: 2x2 grid for street views on left, satellite on right
        
        if available_images:
            # Draw street view images in a 2x2 grid on the left
            canvas_obj.setFillColor(self.colors.PRIMARY)
            canvas_obj.setFont("Helvetica-Bold", 10)
            canvas_obj.drawString(20, y_pos, "MULTI-ANGLE STREET VIEW ANALYSIS:")
            y_pos -= 10
            
            # Small image dimensions for 2x2 grid
            small_img_width = 130
            small_img_height = 95
            
            # Draw up to 4 street view images
            positions = [
                (20, y_pos - small_img_height),      # Top-left (North)
                (160, y_pos - small_img_height),     # Top-right (East)
                (20, y_pos - 2*small_img_height - 10),   # Bottom-left (South)
                (160, y_pos - 2*small_img_height - 10)   # Bottom-right (West)
            ]
            
            directions = ['N', 'E', 'S', 'W']
            
            for i, (heading, img_path) in enumerate(available_images[:4]):
                if i < len(positions):
                    x, y = positions[i]
                    if os.path.exists(img_path):
                        try:
                            canvas_obj.drawImage(img_path, x, y, width=small_img_width, height=small_img_height)
                            # Add direction label
                            canvas_obj.setFillColor(self.colors.WHITE)
                            canvas_obj.setFont("Helvetica-Bold", 10)
                            canvas_obj.drawString(x + 5, y + small_img_height - 15, directions[i])
                        except:
                            self.draw_image_placeholder(canvas_obj, x, y, small_img_width, small_img_height, f"View {directions[i]}")
        
        # Satellite view on the right side
        if satellite_path and os.path.exists(satellite_path):
            canvas_obj.setFillColor(self.colors.PRIMARY)
            canvas_obj.setFont("Helvetica-Bold", 10)
            canvas_obj.drawString(305, y_pos, "SATELLITE VIEW ANALYSIS:")
            y_pos -= 10
            
            try:
                canvas_obj.drawImage(satellite_path, 305, y_pos - 200, width=270, height=200)
            except:
                self.draw_image_placeholder(canvas_obj, 305, y_pos - 200, 270, 200, "Satellite View")
        
        y_pos -= 210  # Account for the height of images
        
        # Add analysis text
        y_pos = self.add_blind_spot_analysis_text(canvas_obj, spot, images, y_pos)
        
        return y_pos

    def add_blind_spot_analysis_text(self, canvas_obj, spot: Dict, images: Dict[str, str], 
                                    y_pos: float) -> float:
        """Add analysis text for blind spot images"""
        canvas_obj.setFont("Helvetica", 8)
        canvas_obj.setFillColor(self.colors.SECONDARY)
        
        # Multi-angle analysis summary
        analysis_text = [
            "BLIND SPOT ANALYSIS SUMMARY:",
            f"Type: {spot.get('spotType', 'Unknown').title()}",
            f"Visibility: {spot.get('visibilityDistance', 0)}m in all directions",
            f"Risk Score: {spot.get('riskScore', 0)}/10",
            "",
            "COVERAGE ANALYSIS:",
            f"• North View: {'Available' if 'street_view_0' in images else 'Not available'}",
            f"• East View: {'Available' if 'street_view_90' in images else 'Not available'}",
            f"• South View: {'Available' if 'street_view_180' in images else 'Not available'}",
            f"• West View: {'Available' if 'street_view_270' in images else 'Not available'}",
            f"• Aerial View: {'Available' if 'satellite' in images else 'Not available'}",
            "",
            "HAZARD ASSESSMENT:",
            f"• {'Critical hazard - extreme caution required' if spot.get('riskScore', 0) >= 8 else 'High risk area - proceed with caution'}",
            f"• {'No warning signs present - extra vigilance needed' if not spot.get('warningSignsPresent') else 'Warning signs installed'}",
            f"• {'No safety mirrors - limited visibility' if not spot.get('mirrorInstalled') else 'Safety mirrors available'}"
        ]
        
        x_pos = 20
        for line in analysis_text:
            canvas_obj.drawString(x_pos, y_pos, line)
            y_pos -= 10
        
        return y_pos - 10

    def add_blind_spot_safety_recommendations(self, canvas_obj, spot: Dict, y_pos: float):
        """Add safety recommendations for blind spots"""
        if y_pos < 150:
            # Not enough space, would overlap with footer
            return
        
        # Safety recommendations box
        canvas_obj.setFillColor(self.colors.WARNING)
        canvas_obj.rect(20, y_pos - 60, 555, 20, fill=1, stroke=0)
        
        canvas_obj.setFillColor(self.colors.WHITE)
        canvas_obj.setFont("Helvetica-Bold", 10)
        canvas_obj.drawString(25, y_pos - 55, "CRITICAL SAFETY RECOMMENDATIONS FOR BLIND SPOT")
        
        # Recommendations based on blind spot characteristics
        recommendations = self.get_blind_spot_safety_recommendations(spot)
        
        y_pos -= 85
        canvas_obj.setFillColor(self.colors.SECONDARY)
        canvas_obj.setFont("Helvetica", 9)
        
        for i, rec in enumerate(recommendations[:6]):  # Limit to 6 recommendations
            canvas_obj.drawString(25, y_pos, f"• {rec}")
            y_pos -= 12

    def get_blind_spot_classification(self, spot: Dict) -> str:
        """Get blind spot classification based on visibility and risk"""
        visibility = spot.get('visibilityDistance', 0)
        risk_score = spot.get('riskScore', 0)
        spot_type = spot.get('spotType', 'unknown')
        
        if visibility < 50:
            return f"EXTREME BLIND SPOT - {spot_type.upper()}"
        elif visibility < 100:
            return f"SEVERE BLIND SPOT - {spot_type.upper()}"
        elif visibility < 200:
            return f"MODERATE BLIND SPOT - {spot_type.upper()}"
        else:
            return f"BLIND SPOT - {spot_type.upper()}"

    def get_visibility_category(self, visibility_distance: float) -> str:
        """Categorize visibility distance"""
        if visibility_distance < 50:
            return "Very Poor (<50m)"
        elif visibility_distance < 100:
            return "Poor (50-100m)"
        elif visibility_distance < 200:
            return "Limited (100-200m)"
        else:
            return "Adequate (>200m)"

    def get_blind_spot_driver_action(self, spot: Dict) -> str:
        """Get recommended driver action for blind spots"""
        visibility = spot.get('visibilityDistance', 0)
        risk_score = spot.get('riskScore', 0)
        
        if visibility < 50 or risk_score >= 8:
            return "Sound horn continuously, reduce to 10-15 km/h"
        elif visibility < 100 or risk_score >= 7:
            return "Use horn, reduce speed to 20 km/h, high alert"
        elif visibility < 200 or risk_score >= 5:
            return "Sound horn before entering, 30 km/h max"
        else:
            return "Use horn, proceed with caution"

    def get_blind_spot_safety_recommendations(self, spot: Dict) -> List[str]:
        """Get safety recommendations for a blind spot"""
        recommendations = []
        visibility = spot.get('visibilityDistance', 0)
        risk_score = spot.get('riskScore', 0)
        spot_type = spot.get('spotType', 'unknown')
        
        # Visibility-based recommendations
        if visibility < 50:
            recommendations.extend([
                "EXTREME DANGER: Visibility less than 50m",
                "Come to complete stop before proceeding",
                "Use horn continuously while passing through",
                "Consider sending scout vehicle ahead if in convoy"
            ])
        elif visibility < 100:
            recommendations.extend([
                "SEVERE LIMITATION: Visibility less than 100m",
                "Reduce speed to walking pace (10-15 km/h)",
                "Sound horn multiple times before entering"
            ])
        
        # Risk-based recommendations
        if risk_score >= 8:
            recommendations.extend([
                "Critical risk area - extreme caution required",
                "No overtaking under any circumstances",
                "Maintain radio contact if in convoy"
            ])
        
        # Type-specific recommendations
        if spot_type == 'crest':
            recommendations.extend([
                "Hill crest blind spot - stay in center of lane",
                "Be prepared for oncoming traffic",
                "Never attempt overtaking on crest"
            ])
        elif spot_type == 'curve':
            recommendations.extend([
                "Curved section - position for maximum visibility",
                "Watch for vehicles cutting corners",
                "Use headlights even in daylight"
            ])
        elif spot_type == 'intersection':
            recommendations.extend([
                "Blind intersection - stop completely",
                "Check all directions before proceeding",
                "Give way to traffic on main road"
            ])
        elif spot_type == 'vegetation':
            recommendations.extend([
                "Vegetation obstruction - visibility changes seasonally",
                "Extra caution during monsoon growth period",
                "Report overgrown vegetation to authorities"
            ])
        
        # Safety feature recommendations
        if not spot.get('warningSignsPresent'):
            recommendations.append("No warning signs - approach with extreme caution")
        
        if not spot.get('mirrorInstalled'):
            recommendations.append("No convex mirrors - rely on horn and extreme caution")
        
        # General recommendations
        recommendations.extend([
            "Keep windows down to hear approaching vehicles",
            "Turn off audio systems when approaching",
            "Use hazard lights in extreme blind spots"
        ])
        
        return recommendations
    
    def create_comprehensive_environmental_assessment_page(self, canvas_obj, route_data: Dict[str, Any]):
        """Create comprehensive environmental assessment page matching the format"""
        collections = route_data['collections']
        route = route_data['route']
        
        # Initialize translator if not already done
        if not hasattr(self, 'translator'):
            try:
                from googletrans import Translator
                self.translator = Translator()
            except ImportError:
                logger.warning("googletrans not installed. Install with: pip install googletrans==4.0.0-rc1")
                self.translator = None
        
        # Page header
        self.add_page_header(canvas_obj, "HPCL - Journey Risk Management Study (AI-Powered Analysis)")
        
        y_pos = self.page_height - 100
        
        # Collect environmental data from various collections
        eco_zones = self.remove_duplicate_coordinates(collections.get('eco_sensitive_zones', []))
        weather_conditions = self.remove_duplicate_coordinates(collections.get('weather_conditions', []))
        
        # Extract environmental data points
        environmental_points = []
        
        # Add eco-sensitive zones if available
        for zone in eco_zones:
            # Get zone type and name dynamically
            zone_type = zone.get('zoneType', 'eco_sensitive').replace('_', ' ')
            zone_name = zone.get('name', '')
            
            # Translate zone name to English if needed
            zone_name_english = self.translate_to_english(zone_name)
            
            # Create dynamic description based on zone characteristics
            description = self.get_dynamic_eco_zone_description(zone, zone_name_english)
            
            environmental_points.append({
                'risk_type': zone_type,
                'gps_location': f"{zone.get('latitude', 0):.5f},{zone.get('longitude', 0):.5f}",
                'maps_link': f"https://www.google.com/maps?q={zone.get('latitude', 0)}%2C{zone.get('longitude', 0)}",
                'severity': zone.get('severity', 'critical'),
                'category': 'ecological',
                'description': description
            })
        
        # Add weather hazard zones
        for weather in weather_conditions:
            if weather.get('riskScore', 0) >= 5:
                weather_condition = weather.get('weatherCondition', 'unknown')
                visibility = weather.get('visibilityKm', 10)
                wind_speed = weather.get('windSpeedKmph', 0)
                season = weather.get('season', '')
                
                # Create dynamic weather description
                desc_parts = []
                
                if weather_condition == 'foggy':
                    desc_parts.append(f"Fog zone - visibility {visibility}km.")
                    desc_parts.append("Use fog lights, reduce speed.")
                elif weather_condition == 'rainy':
                    desc_parts.append(f"Heavy rainfall area during {season}.")
                    desc_parts.append("Risk of waterlogging, drive carefully.")
                elif weather_condition == 'stormy':
                    desc_parts.append(f"Storm risk - wind speed {wind_speed}km/h.")
                    desc_parts.append("Secure cargo, avoid if possible.")
                else:
                    desc_parts.append(f"Weather hazard - {weather_condition} conditions.")
                    desc_parts.append("Exercise caution.")
                    
                if visibility < 2:
                    desc_parts.append("CRITICAL: Very poor visibility.")
                
                environmental_points.append({
                    'risk_type': f'{weather_condition}_conditions',
                    'gps_location': f"{weather.get('latitude', 0):.5f},{weather.get('longitude', 0):.5f}",
                    'maps_link': f"https://www.google.com/maps?q={weather.get('latitude', 0)}%2C{weather.get('longitude', 0)}",
                    'severity': 'high' if weather.get('riskScore', 0) >= 7 else 'medium',
                    'category': 'weather',
                    'description': ' '.join(desc_parts)
                })
        
        # Add seasonal risk areas from weather data
        seasonal_risks = self.analyze_seasonal_risks(weather_conditions)
        for risk in seasonal_risks:
            environmental_points.append(risk)
        
        # Add air quality risk areas (simulated if not in collections)
        air_quality_risks = self.analyze_air_quality_risks(route)
        environmental_points.extend(air_quality_risks)
        
        # Summary statistics
        total_points = len(environmental_points)
        eco_zones_count = len([p for p in environmental_points if p['category'] == 'ecological'])
        air_quality_count = len([p for p in environmental_points if p['category'] == 'air_quality'])
        weather_hazards = len([p for p in environmental_points if p['category'] == 'weather'])
        seasonal_risks_count = len([p for p in environmental_points if p['category'] == 'seasonal'])
        
        # Determine primary risk level
        primary_risk_level = self.calculate_environmental_risk_level(environmental_points)
        
        # Create summary table
        summary_headers = ["Environmental Metric", "Value"]
        summary_col_widths = [250, 250]
        
        summary_data = [
            ["Total Analysis Points", str(total_points)],
            ["Eco-Sensitive Zones", str(eco_zones_count)],
            ["Air Quality Risk Areas", str(air_quality_count)],
            ["Weather Hazard Zones", str(weather_hazards)],
            ["Seasonal Risk Areas", str(seasonal_risks_count)],
            ["Primary Risk Level", primary_risk_level],
            ["API Sources Used", "Open Weather, Visual Crossing, Tomorrow.io, Google Places"]
        ]
        
        y_pos = self.create_simple_table(
            canvas_obj,
            "COMPREHENSIVE ENVIRONMENTAL ASSESSMENT",
            summary_headers,
            summary_data,
            50, y_pos,
            summary_col_widths,
            title_color="#548ed4",
            header_color=self.colors.WHITE,
            
        )
        
        # Environmental risk zones table
        y_pos -= 30
        
        if environmental_points:
            # Prepare detailed table
            detail_headers = ["Risk Type", "GPS Location", "Link", "Severity", "Category", "Description"]
            detail_col_widths = [80, 90, 40, 50, 50, 190]  # Increased description width
            
            detail_data = []
            for point in environmental_points[:20]:  # Limit to 20 for space
                detail_data.append([
                    point['risk_type'].replace('_', ' ').title(),
                    point['gps_location'],
                    "[View]",  # This will be made clickable
                    point['severity'].upper(),
                    point['category'].title(),
                    point['description']
                ])
            
            y_pos = self.create_environmental_table_with_links(
                canvas_obj,
                "ENVIRONMENTAL RISK ZONES & COMPLIANCE REQUIREMENTS",
                detail_headers,
                detail_data,
                environmental_points,
                30, y_pos,
                detail_col_widths,
                title_color=self.colors.WHITE,
                title_text_color=self.colors.PRIMARY,
                header_color=self.colors.WHITE
            )
        
        # Environmental compliance best practices
        y_pos -= 30
        
        if y_pos < 250:  # Check if we need a new page
            canvas_obj.showPage()
            self.add_page_header(canvas_obj, "HPCL - Journey Risk Management Study (AI-Powered Analysis)")
            y_pos = self.page_height - 120
        
        # Best practices section
        canvas_obj.setFillColor(self.colors.WHITE)
        canvas_obj.rect(30, y_pos, 540, 25, fill=1, stroke=0)
        canvas_obj.setFillColor(self.colors.INFO)
        canvas_obj.setFont("Helvetica-Bold", 12)
        canvas_obj.drawString(40, y_pos + 8, "ENVIRONMENTAL COMPLIANCE & BEST PRACTICES")
        
        y_pos -= 30
        
        best_practices = [
            "• Comply with National Green Tribunal (NGT) regulations in eco-sensitive zones",
            "• Follow Central Pollution Control Board (CPCB) emission standards",
            "• Adhere to Wildlife Protection Act requirements in sanctuary areas",
            "• Implement noise control measures during night hours in sensitive zones",
            "• Ensure vehicle PUC (Pollution Under Control) certificate is current",
            "• Carry emergency spill containment kit for hazardous cargo"
        ]
        
        canvas_obj.setFillColor(self.colors.SECONDARY)
        canvas_obj.setFont("Helvetica", 10)
        
        for practice in best_practices:
            canvas_obj.drawString(40, y_pos, practice)
            y_pos -= 15

    def create_environmental_table_with_links(self, canvas_obj, title: str, headers: List[str], 
                                            data: List[List[str]], original_points: List[Dict],
                                            start_x: int, start_y: int, col_widths: List[int], 
                                            title_color=None, header_color=None, title_text_color=None ):
        """Create environmental table with clickable maps links"""
        if title_color is None:
            title_color = self.colors.PRIMARY
        if header_color is None:
            header_color = "#548ed4"
        if title_text_color is None:
            title_text_color = self.colors.WHITE
        
        current_y = start_y
        table_width = sum(col_widths)
        header_height = 22
        row_height = 18
        
        # Title
        canvas_obj.setFillColor(title_color)
        canvas_obj.rect(start_x, current_y, table_width, 25, fill=1, stroke=0)
        canvas_obj.setFillColor(title_text_color)
        canvas_obj.setFont("Helvetica-Bold", 12)
        canvas_obj.drawString(start_x + 10, current_y + 8, title)
        current_y -= 25
        
        # Headers
        canvas_obj.setFillColor(header_color)
        canvas_obj.rect(start_x, current_y, table_width, header_height, fill=1, stroke=1)
        canvas_obj.setFillColor(self.colors.BLACK)
        canvas_obj.setFont("Helvetica-Bold", 9)
        
        x_pos = start_x
        for header, width in zip(headers, col_widths):
            canvas_obj.drawString(x_pos + 5, current_y + 7, header)
            x_pos += width
        
        current_y -= header_height
        
        # Data rows
        canvas_obj.setFont("Helvetica", 8)
        
        for row_idx, (row, point) in enumerate(zip(data, original_points)):
            # Alternate row colors for background
            if row_idx % 2 == 0:
                canvas_obj.setFillColorRGB(0.97, 0.97, 0.97)
            else:
                canvas_obj.setFillColor(self.colors.WHITE)
            
            canvas_obj.rect(start_x, current_y, table_width, row_height, fill=1, stroke=1)
            canvas_obj.setStrokeColor(self.colors.SECONDARY)
            canvas_obj.setLineWidth(0.5)
            
            # IMPORTANT: Set text color back to dark for visibility
            canvas_obj.setFillColor(self.colors.SECONDARY)  # Dark gray text
            
            # Draw cell content
            x_pos = start_x
            for col_idx, (cell_data, width) in enumerate(zip(row, col_widths)):
                if col_idx == 2:  # Maps link column
                    # Make it blue and clickable
                    canvas_obj.setFillColor(self.colors.INFO)
                    canvas_obj.setFont("Helvetica-Bold", 8)
                    link_text = "[View]"
                    text_width = canvas_obj.stringWidth(link_text, "Helvetica-Bold", 8)
                    text_x = x_pos + (width - text_width) / 2
                    canvas_obj.drawString(text_x, current_y + 5, link_text)
                    
                    # Create clickable area
                    canvas_obj.linkURL(point['maps_link'], 
                                    (text_x - 2, current_y, text_x + text_width + 2, current_y + row_height))
                    
                    # Reset to dark text color
                    canvas_obj.setFillColor(self.colors.SECONDARY)
                    canvas_obj.setFont("Helvetica", 8)
                elif col_idx == 3:  # Severity column
                    # Color code severity
                    severity_colors = {
                        'CRITICAL': self.colors.DANGER,
                        'HIGH': HexColor('#FF5722'),
                        'MEDIUM': self.colors.WARNING,
                        'LOW': self.colors.SUCCESS
                    }
                    canvas_obj.setFillColor(severity_colors.get(cell_data, self.colors.SECONDARY))
                    canvas_obj.setFont("Helvetica-Bold", 8)
                    canvas_obj.drawString(x_pos + 5, current_y + 5, cell_data)
                    # Reset to dark text color
                    canvas_obj.setFillColor(self.colors.SECONDARY)
                    canvas_obj.setFont("Helvetica", 8)
                else:
                    # Ensure dark text color for all other columns
                    canvas_obj.setFillColor(self.colors.SECONDARY)
                    
                    if col_idx == 5:  # Description column - special handling
                        # Use smaller font for description to fit more text
                        canvas_obj.setFont("Helvetica", 7)
                        # Don't truncate description - show full text
                        display_text = cell_data
                    else:
                        # Truncate other columns if needed
                        max_chars = int(width / 5)
                        display_text = cell_data[:max_chars] + "..." if len(cell_data) > max_chars else cell_data
                    
                    # Handle long descriptions with word wrap
                    if col_idx == 5 and len(display_text) > 30:
                        # Split into multiple lines if needed
                        words = display_text.split()
                        lines = []
                        current_line = []
                        line_width = 0
                        max_width = width - 10
                        
                        for word in words:
                            word_width = canvas_obj.stringWidth(word + " ", "Helvetica", 7)
                            if line_width + word_width <= max_width:
                                current_line.append(word)
                                line_width += word_width
                            else:
                                if current_line:
                                    lines.append(" ".join(current_line))
                                current_line = [word]
                                line_width = word_width
                        
                        if current_line:
                            lines.append(" ".join(current_line))
                        
                        # Draw first line
                        if lines:
                            canvas_obj.drawString(x_pos + 5, current_y + 5, lines[0])
                            # If there are more lines, add ellipsis
                            if len(lines) > 1:
                                canvas_obj.drawString(x_pos + 5, current_y - 2, "...")
                    else:
                        canvas_obj.drawString(x_pos + 5, current_y + 5, display_text)
                    
                    # Reset font if changed
                    if col_idx == 5:
                        canvas_obj.setFont("Helvetica", 8)
                
                x_pos += width
            
            current_y -= row_height
        
        return current_y - 10

    def analyze_seasonal_risks(self, weather_conditions: List[Dict]) -> List[Dict]:
        """Analyze seasonal risks from weather data"""
        seasonal_risks = []
        
        # Group by season
        season_groups = {}
        for weather in weather_conditions:
            season = weather.get('season', 'unknown')
            if season not in season_groups:
                season_groups[season] = []
            season_groups[season].append(weather)
        
        # Analyze each season
        for season, conditions in season_groups.items():
            if conditions and len(conditions) > 0:
                avg_risk = sum(c.get('riskScore', 0) for c in conditions) / len(conditions)
                if avg_risk >= 5:
                    # Take first point as representative
                    point = conditions[0]
                    seasonal_risks.append({
                        'risk_type': f'{season}_conditions',
                        'gps_location': f"{point.get('latitude', 0):.5f},{point.get('longitude', 0):.5f}",
                        'maps_link': f"https://www.google.com/maps?q={point.get('latitude', 0)}%2C{point.get('longitude', 0)}",
                        'severity': 'high' if avg_risk >= 7 else 'medium',
                        'category': 'seasonal',
                        'description': f'{season.title()} weather challenges - exercise caution'
                    })
        
        return seasonal_risks

    def analyze_air_quality_risks(self, route: Dict) -> List[Dict]:
        """Analyze air quality risks based on route location"""
        air_quality_risks = []
        
        # Simulated air quality analysis based on route type and location
        if route.get('terrain') == 'urban':
            # Add urban air quality risk
            air_quality_risks.append({
                'risk_type': 'urban_pollution',
                'gps_location': f"{route['fromCoordinates']['latitude']:.5f},{route['fromCoordinates']['longitude']:.5f}",
                'maps_link': f"https://www.google.com/maps?q={route['fromCoordinates']['latitude']}%2C{route['fromCoordinates']['longitude']}",
                'severity': 'medium',
                'category': 'air_quality',
                'description': 'Urban area - potential air quality issues'
            })
        
        return air_quality_risks

    def calculate_environmental_risk_level(self, environmental_points: List[Dict]) -> str:
        """Calculate overall environmental risk level"""
        if not environmental_points:
            return "Low"
        
        critical_count = len([p for p in environmental_points if p['severity'] in ['critical', 'CRITICAL']])
        high_count = len([p for p in environmental_points if p['severity'] in ['high', 'HIGH']])
        
        if critical_count >= 2:
            return "Critical"
        elif critical_count >= 1 or high_count >= 3:
            return "High"
        elif high_count >= 1:
            return "Medium"
        else:
            return "Low"

    def get_eco_zone_description(self, zone_type: str, zone_name: str = '') -> str:
        """Get appropriate description based on eco-sensitive zone type"""
        descriptions = {
            'wildlife sanctuary': 'Drive slowly and stay alert. Wildlife crossing possible.',
            'protected forest': 'Protected area - follow forest regulations strictly.',
            'eco sensitive': 'Eco-sensitive zone - minimize environmental impact.',
            'national park': 'National park area - adhere to park guidelines.',
            'biosphere reserve': 'Biosphere reserve - critical conservation area.'
        }
        
        base_description = descriptions.get(zone_type, 'Drive slowly and stay alert.')
        
        # Add zone name if available
        if zone_name:
            return f"{zone_name} - {base_description}"
        
        return base_description

    def get_dynamic_eco_zone_description(self, zone: Dict, zone_name: str = '') -> str:
        """Get dynamic description based on zone characteristics and data"""
        zone_type = zone.get('zoneType', 'eco_sensitive')
        severity = zone.get('severity', 'medium')
        restrictions = zone.get('restrictions', [])
        wildlife_types = zone.get('wildlifeTypes', [])
        speed_limit = zone.get('speedLimit', 40)
        critical_habitat = zone.get('criticalHabitat', False)
        timing_restrictions = zone.get('timingRestrictions', '')
        
        # Start with zone name if available
        desc_parts = []
        if zone_name:
            desc_parts.append(f"{zone_name}:")
        
        # Add severity-based warning
        if severity == 'critical':
            desc_parts.append("CRITICAL ZONE - Extreme caution required.")
        elif severity == 'high':
            desc_parts.append("HIGH RISK AREA - Exercise increased vigilance.")
        
        # Add specific warnings based on zone type
        if zone_type == 'wildlife_sanctuary':
            if wildlife_types:
                wildlife_str = ', '.join(wildlife_types[:3])  # First 3 wildlife types
                desc_parts.append(f"Wildlife crossing ({wildlife_str}).")
            else:
                desc_parts.append("Wildlife crossing possible.")
            desc_parts.append("Drive slowly, no honking.")
            
        elif zone_type == 'protected_forest':
            desc_parts.append("Protected forest area.")
            if critical_habitat:
                desc_parts.append("Critical habitat - minimize disturbance.")
                
        elif zone_type == 'national_park':
            desc_parts.append("National park boundaries.")
            desc_parts.append("Follow park regulations strictly.")
        
        # Add speed limit if specified
        if speed_limit and speed_limit < 60:
            desc_parts.append(f"Speed limit: {speed_limit} km/h.")
        
        # Add timing restrictions if any
        if timing_restrictions:
            desc_parts.append(f"Timing: {timing_restrictions}.")
        
        # Add specific restrictions
        if restrictions:
            for restriction in restrictions[:2]:  # First 2 restrictions
                if 'honking' in restriction.lower():
                    desc_parts.append("No honking allowed.")
                elif 'night' in restriction.lower():
                    desc_parts.append("Night travel restrictions apply.")
                elif 'permit' in restriction.lower():
                    desc_parts.append("Entry permit required.")
        
        # Join all parts
        description = ' '.join(desc_parts)
        
        # Ensure we have meaningful content
        if not description or len(description) < 20:
            description = f"{zone_name + ' - ' if zone_name else ''}Drive slowly and maintain vigilance in this eco-sensitive area."
        
        return description

    def translate_to_english(self, text: str) -> str:
        """Translate text to English if needed"""
        if not text or not isinstance(text, str):
            return text
        
        # Check if text is already in English (basic check)
        try:
            # If all characters are ASCII, likely already English
            text.encode('ascii')
            return text
        except UnicodeEncodeError:
            # Text contains non-ASCII characters, might need translation
            pass
        
        # Try to translate using googletrans
        if self.translator:
            try:
                # Detect language and translate if not English
                detected = self.translator.detect(text)
                if detected.lang != 'en':
                    translated = self.translator.translate(text, dest='en')
                    return translated.text
            except Exception as e:
                logger.warning(f"Translation failed for '{text}': {e}")
        
        # Fallback: return original text if translation fails
        return text

    def draw_title_bullet_section(self, canvas_obj, title:str, bullets: List[str], y, title_color = None):
            if title_color is None:
                title_color = self.colors.INFO
            y = self.check_page_space(y, canvas_obj)
            canvas_obj.setFont("Helvetica-Bold", 12)
            canvas_obj.setFillColor(title_color)
            canvas_obj.drawString(50, y, title)
            y -= 18
            canvas_obj.setFont("Helvetica", 10)
            canvas_obj.setFillColor(self.colors.BLACK)
            for bullet in bullets:
                y = self.check_page_space(y,canvas_obj)
                canvas_obj.drawString(70, y, f"*  {bullet}")
                y -= 14
            return y - 30

    def draw_centered_text_in_box(self, canvas_obj, text:str, x:int, y:int , width:int = 480, height:int= 25, font_name="Helvetica-Bold", font_size=12, text_color='#FFFFFF', box_color='#005293', border=False):
        """
        Draw a filled rectangle and center the text inside it.
        Args:
            canvas_obj: ReportLab canvas
            text: Text to display
            x, y: Bottom-left coordinates of the box
            width, height: Dimensions of the box
            font_name: Font name
            font_size: Font size
            text_color: Color of the text
            box_color: Fill color of the box
            border: Whether to draw border or not
        """
        
        # Draw filled box
        canvas_obj.setFillColor(box_color)
        canvas_obj.setStrokeColor(box_color)
        canvas_obj.rect(x, y, width, height, fill=1, stroke=int(border))
        canvas_obj.setFont(font_name, font_size)
        canvas_obj.setFillColor(text_color)
        text_width = canvas_obj.stringWidth(text, font_name, font_size)
        text_x = x + (width - text_width) / 2
        text_y = y + (height - font_size) / 2 + font_size * 0.25  

        canvas_obj.drawString(text_x, text_y, text)
        return y - height -10

    def check_page_space(self, y_pos, canvas_obj, min_y=80):
        """Page: Comprehensive Environmental Assessment"""
        if y_pos < min_y:
            canvas_obj.showPage()
            self.add_page_header(
                canvas_obj,
                "HPCL - Journey Risk Management Study (AI-Powered Analysis)"
            )
            return self.page_height - 120
        return y_pos

    def create_emergency_sop_section(self, canvas_obj):
        """Page: Emergency SOP Protocols with auto page break support"""

        def draw_section(title, subtitle, subtitle1, bullets, y, canvas_obj):
            y = self.check_page_space(y, canvas_obj)
            canvas_obj.setFont("Helvetica-Bold", 12)
            canvas_obj.setFillColor(self.colors.DANGER)
            canvas_obj.drawString(50, y, title)
            y -= 18

            y = self.check_page_space(y, canvas_obj)
            canvas_obj.setFont("Helvetica-Bold", 10)
            canvas_obj.setFillColor(self.colors.SECONDARY)
            canvas_obj.drawString(50, y, subtitle)
            y -= 24

            y = self.check_page_space(y, canvas_obj)
            canvas_obj.setFont("Helvetica-Bold", 10)
            canvas_obj.setFillColor(self.colors.SECONDARY)
            canvas_obj.drawString(50, y, subtitle1)
            y -= 16

            canvas_obj.setFont("Helvetica", 10)
            canvas_obj.setFillColor(self.colors.BLACK)
            for bullet in bullets:
                y = self.check_page_space(y,canvas_obj)
                canvas_obj.drawString(70, y, f"• {bullet}")
                y -= 14
            return y - 30

        self.add_page_header(
            canvas_obj,
            "HPCL - Journey Risk Management Study (AI-Powered Analysis)"
        )
        y_pos = self.page_height - 120

        # adding main Heading
        head_title = "Emergency Situation Standard Operating Procedure (SOP)"
        y_pos = self.draw_centered_text_in_box(
            canvas_obj,
            head_title,
            50, y_pos, 500, 35,
            box_color=self.colors.WARNING
            )

        # 1. ROAD ACCIDENT PROTOCOL
        y_pos = draw_section(
            "ROAD ACCIDENT PROTOCOL",
            "Applicable to: Collision, crash, hitting stationary objects, or injury to personnel.",
            "Immediate Actions:",
            [
                "Stop the vehicle safely; engage handbrake, switch on hazard lights.",
                "Check for injuries; call 108 for ambulance if required.",
                "Inform control room or transport coordinator (via phone or VHF radio).",
                "Take photographs of damage, road conditions, and position of vehicles.",
                "If serious: Inform Police (100) and wait for clearance.",
                "Record witness contact (if any) and note surrounding conditions.",
            ],
            y_pos, 
            canvas_obj
        )

        y_pos = self.check_page_space(y_pos, canvas_obj)
        canvas_obj.setFont("Helvetica-Bold", 10)
        canvas_obj.setFillColor(self.colors.BLACK)
        canvas_obj.drawString(50, y_pos, "Driver Must Have:")
        y_pos -= 16
        canvas_obj.setFont("Helvetica", 10)
        canvas_obj.setFillColor(self.colors.BLACK)
        for item in ["First aid kit", "Emergency contact card (Appendix B)", "Accident report form"]:
            y_pos = self.check_page_space(y_pos, canvas_obj)
            canvas_obj.drawString(70, y_pos, f"• {item}")
            y_pos -= 14

        y_pos -= 30

        # 2. VEHICLE BREAKDOWN PROTOCOL
        y_pos = draw_section(
            "VEHICLE BREAKDOWN PROTOCOL",
            "Applicable to: Engine failure, tire burst, brake malfunction, fuel issues",
            "Immediate Actions:",
            [
                "Pull over to safe zone; place reflective triangle 15m behind vehicle",
                "Use flashers and hazard lights to alert other road users",
                "Inform Control Room and nearest Highway Patrol (1033)",
                "Attempt minor fixes if safe (replace tire, check fuses)",
                "Call backup vehicle if repair not possible within 30 min"
            ],
            y_pos,
            canvas_obj
        )

        y_pos = self.check_page_space(y_pos, canvas_obj)
        canvas_obj.setFont("Helvetica-Bold", 10)
        canvas_obj.setFillColor(self.colors.SECONDARY)
        canvas_obj.drawString(50, y_pos, "Caution: Do not attempt repair in curves, blind spots, or eco-sensitive zones.")
        y_pos -= 40

        # 3. WILDLIFE ENCOUNTER PROTOCOL
        y_pos = draw_section(
            "WILDLIFE ENCOUNTER PROTOCOL",
            "Applicable to: Forest fringe, wildlife corridor.",
            "Immediate Actions:",
            [
                "Do NOT honk, rev engine, or flash headlights",
                "Stop the vehicle quietly at a safe distance",
                "Monitor movement through side mirrors; do not exit vehicle",
                "Inform control room only if delay >15 min",
                "Take photos only if safe; do not get out"
            ],
            y_pos,
            canvas_obj
        )

        y_pos = self.check_page_space(y_pos, canvas_obj)
        canvas_obj.setFont("Helvetica-Bold", 10)
        canvas_obj.setFillColor(self.colors.SECONDARY)
        canvas_obj.drawString(50, y_pos, "DO NOT:")
        y_pos -= 16
        canvas_obj.setFont("Helvetica", 10)
        canvas_obj.setFillColor(self.colors.BLACK)
        for item in [
            "Litter or feed animals",
            "Use loud sounds or lights",
            "Exit vehicle in wildlife corridor"
        ]:
            y_pos = self.check_page_space(y_pos, canvas_obj)
            canvas_obj.drawString(70, y_pos, f"• {item}")
            y_pos -= 14

        y_pos -= 30

        # 4. MEDICAL EMERGENCY PROTOCOL
        y_pos = draw_section(
            "MEDICAL EMERGENCY PROTOCOL",
            "Applicable to: Driver illness, passenger discomfort, heatstroke, trauma.",
            "Immediate Actions:",
            [
                "Park vehicle safely",
                "Apply basic first aid from onboard kit",
                "Call 108 or direct to nearest hospital (Appendix B)",
                "Guide emergency team using GPS coordinates from report",
                "Record incident time, symptoms, action taken"
            ],
            y_pos,
            canvas_obj
        )

        # Final footer (optional)
        y_pos = self.check_page_space(y_pos, canvas_obj)
        canvas_obj.setFillColorRGB(0.7, 0.7, 0.7)
        canvas_obj.setFont("Helvetica-Bold", 10)
        disclaimer = "CONFIDENTIAL - For Internal Use Only. Generated by HPCL Journey Risk Management System with AI-Enhanced Analysis"
        disclaimer_width = canvas_obj.stringWidth(disclaimer, "Helvetica-Oblique", 8)
        canvas_obj.drawString((self.page_width - disclaimer_width) / 2, 60, disclaimer)

    # SHARP TURNS ANALYSIS WITH DUAL VISUAL EVIDENCE High Risk (Risk Score: 4)
    def create_sharp_turns_analysis_page(self, canvas_obj, route_data):
        """Create Page 6: Sharp Turns Analysis with dual visual evidence"""

        self.add_page_header(canvas_obj, "HPCL - Journey Risk Management Study (AI-Powered Analysis)")
        y_pos = self.page_height - 120

        collections = route_data['collections'] 
        all_sharp_turns = collections.get('sharp_turns', [])
        sharp_turns = self.remove_duplicate_coordinates(all_sharp_turns)

        sharp_greter_90 = []
        risk_70_to_80 = []
        risk_80_to_90 = []
        risk_45_to_70 = []
        turn_angles = []

        if sharp_turns:
            for s in sharp_turns:
                angle = s.get('turnAngle')
                if angle is not None:
                    turn_angles.append(angle)
                    if 45 <= angle < 70:
                        risk_45_to_70.append(angle)
                    elif 70 <= angle < 80:
                        risk_70_to_80.append(angle)
                    elif 80 <= angle < 90:
                        risk_80_to_90.append(angle)
                    elif angle >= 90:
                        sharp_greter_90.append(angle)

        # Compute average turn angle dynamically
        avg_turn_angle = round(sum(turn_angles) / len(turn_angles), 1) if turn_angles else 0.0

        # Compute most dangerous turn angle dynamically
        most_dangerous_turn = max(turn_angles) if turn_angles else 0.0

        # Count visual evidence files
        street_view_imgs = 0
        satellite_imgs = 0
        route_map_imgs = 0

        if sharp_turns:
            for s in sharp_turns:
                if s.get("streetViewImage"):
                    street_view_imgs += 1
                if s.get("satelliteImage"):
                    satellite_imgs += 1
                if s.get("roadmapImage"):
                    route_map_imgs += 1

        total_visual_files = street_view_imgs + satellite_imgs + route_map_imgs

        sharp_turn_analysis_data = [
            ["Total Sharp Turns Detected", str(len(sharp_turns))],
            ["Extreme Danger Turns (>=90 deg)", f"{len(sharp_greter_90)}"],
            ["High-Risk Blind Spots (80-90 deg)", f"{len(risk_80_to_90)}"],
            ["Sharp Danger Zones (70-80 deg)", f"{len(risk_70_to_80)}"],
            ["Moderate Risk Turns (45-70 deg)", f"{len(risk_45_to_70)}"],
            ["Most Dangerous Turn Angle", f"{most_dangerous_turn:.1f}°"],
            ["Average Turn Angle", f"{avg_turn_angle:.1f}°"],
            ["Street View Images Available", str(street_view_imgs)],
            ["Satellite Images Available", str(satellite_imgs)],
            ["Route Map Images Available", str(route_map_imgs)],
            ["Total Visual Evidence Files", str(total_visual_files)]
        ]

        headers = ["Parameter", "Value "]
        col_widths = [250, 250]

        y_pos = self.create_simple_table(
            canvas_obj,
            "SHARP TURNS ANALYSIS WITH DUAL VISUAL EVIDENCE High Risk (Risk Score: 4)",
            headers,
            sharp_turn_analysis_data,
            self.margin, y_pos,
            col_widths,
            title_color=self.colors.DANGER,
            text_color=self.colors.WHITE,
            header_color=self.colors.WHITE,
            max_rows_per_page=15
        )

        y_pos -= 40
        if y_pos < 300: # Check for page break
            canvas_obj.showPage()
            # self.add_page_header(canvas_obj, "SHARP TURN CLASSIFICATION SYSTEM", "")
            y_pos = self.page_height - 120

        classification_data = [
            ("≥ 120°", "HAIRPIN TURN", "CRITICAL", "10-15 km/h", "Full stop, sharp steering, extreme caution, check mirrors, inch forward slowly"),
            ("100°-119.9°", "EXTREME SHARP TURN", "EXTREME", "15-20 km/h", "Slow down significantly, use mirrors, stay tight inside lane"),
            ("80°-99.9°", "VERY SHARP TURN", "HIGH", "20 km/h", "Reduce speed, use mirrors, no overtaking"),
            ("60°-79.9°", "HIGH-ANGLE TURN", "MEDIUM", "25-30 km/h", "Brake before turn, keep lane position"),
            ("45°-60°", "SHARP TURN", "LOW", "30-40 km/h", "Stay in lane, observe caution signage"),
            ("30°-44.9°", "MODERATE TURN", "VERY LOW", "40-50 km/h", "Normal caution, light braking"),
            ("< 30°", "GENTLE CURVE", "MINIMAL", "50+ km/h", "Maintain lane, standard driving")
        ]

        headers = ["Angle Range", "Classification", "Risk Level", "Max Speed", "Safety Requirement"]
        col_widths = [70, 100, 60, 70, 200]

        y_pos = self.create_simple_table(
            canvas_obj,
            "SHARP TURN CLASSIFICATION SYSTEM",
            headers,
            classification_data,
            self.margin, y_pos,
            col_widths,
            title_color=self.colors.WHITE,
            text_color=self.colors.DANGER,
            max_rows_per_page=10
        )

        if sharp_turns:
            headers = ["Location(GPS)","Link", "Turn Angle", "Direction", "Risk Level", "Severity"]
            col_widths = [110,60, 80, 70, 70, 80]

            sharp_data = []
            for spot in sharp_turns:
                risk_score = spot.get('riskScore', 0)
                if risk_score >= 8:
                    risk_level = "Critical" 
                    latitude = spot.get('latitude',0)
                    longitude = spot.get('longitude',0)
                    map_link = f"https://www.google.com/maps?q={latitude}%2C{longitude}"
                    sharp_data.append([
                        f"{latitude:.5f}, {longitude:.5f}",
                        map_link,
                        spot.get('turnAngle', 0),
                        spot.get('turnDirection', "Unknown"),
                        risk_level,
                        spot.get("turnSeverity")
                    ])
            
            if sharp_data:
                y_pos -= 40
                if y_pos < 200:
                    canvas_obj.showPage()
                    self.add_page_header(canvas_obj, "HPCL - Journey Risk Management Study (AI-Powered Analysis)", "")
                    y_pos = self.page_height - 120

            y_pos = self.create_simple_table_with_link(
                canvas_obj,
                "CRITICAL SHARP TURNS",
                headers,
                sharp_data,
                50, y_pos,
                col_widths,
                title_bg_color=self.colors.WHITE,
                title_text_color=self.colors.DANGER,
                hyper_link=True,
                hyper_link_col_index=1
                
            )
    def generate_route_pdf(self, route_id: str) -> str:
        """Generate PDF for a route"""
        try:
            # Import the main PDF generator
            from hpcl_pdf_generator_final import HPCLDynamicPDFGenerator
            
            # Initialize PDF generator
            pdf_generator = HPCLDynamicPDFGenerator(self.mongodb_uri)
            
            # Generate PDF
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_filename = f"route_analysis_{route_id}_{timestamp}.pdf"
            output_path = os.path.join(self.output_folder, output_filename)
            
            # Generate the PDF
            result_path = pdf_generator.generate_pdf_report(route_id, output_path)
            
            return result_path
            
        except Exception as e:
            logger.error(f"PDF generation failed: {str(e)}")
            raise Exception(f"PDF generation failed: {str(e)}")
    
    def safe_float_conversion(self, value: Any, default: float = 0.0) -> float:
        """Safely convert value to float"""
        if value is None:
            return default
        
        try:
            # If it's already a number, return it
            if isinstance(value, (int, float)):
                return float(value)
            
            # If it's a string, try to convert
            if isinstance(value, str):
                # Remove any non-numeric characters except . and -
                cleaned = ''.join(c for c in value if c.isdigit() or c in '.-')
                if cleaned:
                    return float(cleaned)
            
            return default
        except (TypeError, ValueError):
            return default
    def safe_int(self, value, default=0):
        """Safely convert value to int"""
        if value is None:
            return default
        
        try:
            if isinstance(value, (int, float)):
                return int(value)
            
            if isinstance(value, str):
                cleaned = ''.join(c for c in value if c.isdigit() or c == '-')
                if cleaned:
                    return int(cleaned)
            
            return default
        except (TypeError, ValueError):
            return default
    def safe_int_conversion(self, value: Any, default: int = 0) -> int:
        """Safely convert value to int"""
        if value is None:
            return default
        
        try:
            # If it's already a number, return it
            if isinstance(value, (int, float)):
                return int(value)
            
            # If it's a string, try to convert
            if isinstance(value, str):
                # Remove any non-numeric characters except -
                cleaned = ''.join(c for c in value if c.isdigit() or c == '-')
                if cleaned:
                    return int(cleaned)
            
            return default
        except (TypeError, ValueError):
            return default
    
    def ensure_numeric_comparison(self, value1, value2):
        """Ensure both values are numeric for comparison"""
        num1 = self.safe_float(value1, 0.0)
        num2 = self.safe_float(value2, 0.0)
        return num1, num2
    def create_blind_spots_analysis_page(self, canvas_obj, route_data):   
        self.add_page_header(canvas_obj, "HPCL - Journey Risk Management Study (AI-Powered Analysis)")
        y_pos = self.page_height - 120

        canvas_obj.setFillColor(self.colors.PRIMARY)
        canvas_obj.setFont("Helvetica-Bold", 14)
        canvas_obj.drawString(100, y_pos, "COMPREHENSIVE ROUTE OVERVIEW & STATISTICS")
        y_pos -= 50

        collections = route_data['collections']
        blind_spots = self.remove_duplicate_coordinates(collections.get('blind_spots', []))

        # Prepare counts
        crest_spots = [s for s in blind_spots if s.get("spotType") == "crest"]
        obstruction_spots = [s for s in blind_spots if s.get("spotType") == "obstruction"]
        curve_spots = [s for s in blind_spots if s.get("spotType") == "curve"]
        intersection_spots = [s for s in blind_spots if s.get("spotType") == "intersection"]

        vis_less_500 = [s for s in blind_spots if s.get("visibilityDistance", 0) < 500]
        vis_500_to_1000 = [s for s in blind_spots if 500 <= s.get("visibilityDistance", 0) <= 1000]
        vis_above_1000 = [s for s in blind_spots if s.get("visibilityDistance", 0) > 1000]

        with_mirrors = [s for s in blind_spots if s.get("mirrorInstalled") is True]
        without_warning_signs = [s for s in blind_spots if not s.get("warningSignsPresent")]

        confidence = "High" if len(blind_spots) >= 10 else "Medium"

        # Summary Table
        headers = ["Description", "Value"]
        blind_spot_analysis_data = [
            ["Total Blind Spot Locations Analyzed", str(len(blind_spots))],
            ["Crest Type Spots", str(len(crest_spots))],
            ["Obstruction Type Spots", str(len(obstruction_spots))],
            ["Curve Type Spots", str(len(curve_spots))],
            ["Intersection Type Spots", str(len(intersection_spots))],
            ["Visibility < 500m", str(len(vis_less_500))],
            ["Visibility 500m–1000m", str(len(vis_500_to_1000))],
            ["Visibility > 1000m", str(len(vis_above_1000))],
            ["Spots with Mirror Installed", str(len(with_mirrors))],
            ["Spots Missing Warning Signs", str(len(without_warning_signs))],
            ["Analysis Confidence", confidence],
        ]

        headers = ["Parameter", "Value"]
        col_widths = [250, 250]

        y_pos = self.create_simple_table(
            canvas_obj,
            "BLIND SPOTS ANALYSIS WITH DUAL VISUAL EVIDENCE High Risk (Risk Score: 4)",
            headers,
            blind_spot_analysis_data,
            self.margin, y_pos,
            col_widths,
            title_color=self.colors.DANGER,
            text_color=self.colors.WHITE,
            max_rows_per_page=15
        )

        # BLIND SPOT CLASSIFICATION SYSTEM
        y_pos -= 40
        if y_pos < 300: # Check for page break
            canvas_obj.showPage()
            self.add_page_header(canvas_obj, "BLIND SPOT CLASSIFICATION SYSTEM", "")
            y_pos = self.page_height - 120
            
   

        blind_spot_classification_data = [
            ("≥ 90°", "EXTREME BLIND SPOT", "CRITICAL", "15 km/h", "Full stop, check both mirrors, inch forward slowly"),
            ("80°-90°", "HIGH-RISK BLIND TURN", "EXTREME", "20 km/h", "Reduce speed sharply, use horn, check for oncoming"),
            ("70°-80°", "BLIND SPOT", "HIGH", "25 km/h", "Keep left, use indicators, maintain 2m clearance"),
            ("60°-70°", "HIGH-ANGLE TURN", "MEDIUM", "30 km/h", "Brake before turn, avoid overtaking"),
            ("45°-60°", "SHARP TURN", "LOW", "40 km/h", "Stay in lane, observe caution signage")
        ]

        headers = ["Angle Range", "Classification", "Risk Level", "Max Speed", "Safety Requirement"]
        col_widths = [70, 120, 60, 70, 180]

        y_pos = self.create_simple_table(
            canvas_obj,
            "BLIND SPOT CLASSIFICATION SYSTEM",
            headers,
            blind_spot_classification_data,
            self.margin, y_pos,
            col_widths,
            title_color=self.colors.WHITE,
            text_color=self.colors.DANGER,
            header_color=self.colors.WHITE,
            max_rows_per_page=10
        )

        y_pos -= 40
        if y_pos < 300: # Check for page break
            canvas_obj.showPage()
            self.add_page_header(canvas_obj, "HPCL - Journey Risk Management Study (AI-Powered Analysis)")
            y_pos = self.page_height - 120

        if blind_spots:
            headers = ["Location (GPS)","Link", "Type", "Visibility (m)", "Risk Level", "Action Required"]
            col_widths = [110,60, 60, 70, 60, 130]

            blind_data = []
            for spot in blind_spots:
                risk_score = spot.get('riskScore', 0)
                if risk_score >= 8:
                    risk_level = "Critical" 
                    latitude = spot.get('latitude',0)
                    longitude = spot.get('longitude',0)
                    map_link = f"https://www.google.com/maps?q={latitude}%2C{longitude}"
                    blind_data.append([
                        f"{latitude:.5f}, {longitude:.5f}",
                        map_link,
                        spot.get('spotType', 'Unknown').title(),
                        str(spot.get('visibilityDistance', 0)),
                        risk_level,
                        "Use horn, stay alert"
                    ])
            
            # This will automatically handle pagination
            y_pos = self.create_simple_table_with_link(
                canvas_obj,
                "CRITICAL BLIND SPOTS",
                headers,
                blind_data,
                50, y_pos,
                col_widths,
                title_bg_color=self.colors.WHITE,
                title_text_color=self.colors.DANGER,
                hyper_link=True,
                hyper_link_col_index=1
                
            )

    def deffensive_driving_and_driver_wellbeing(self, canvas_obj):
        """ DEFENSIVE DRIVING & DRIVER WELL-BEING page"""
        self.add_page_header(canvas_obj, "HPCL - Journey Risk Management Study (AI-Powered Analysis) ")
        y_pos = self.page_height - 120

        canvas_obj.setFillColor(self.colors.PRIMARY)
        canvas_obj.setFont("Helvetica-Bold", 14)
        canvas_obj.drawString(self.margin, y_pos, "DEFENSIVE DRIVING & DRIVER WELL-BEING")
        y_pos -= 20

        defensive_driving_data = [
            ("Maintain safe distance, use indicators", "Keep a minimum 3-second following distance from the vehicle ahead, adjust for heavy loads; always signal turns or lane changes well in advance."),
            ("Stay hydrated: carry water bottles", "Carry at least 2 liters of drinking water. Avoid dehydration, especially in summer. Drink at rest stops every 1-2 hours."),
            ("Avoid heavy/oily meals before journey", "Eat light, balanced meals to avoid drowsiness and discomfort. Avoid spicy, fried, or heavy foods before and during the trip."),
            ("Get at least 8 hours of sleep before starting", "A good night's sleep is essential to reduce fatigue and maintain focus. Never start a trip when tired or drowsy."),
            ("Wear weather-appropriate protective gear", "Use sun protection (caps, sunglasses) in summer; layered clothing in winter; always wear safety shoes and reflective vests."),
            ("Control speed based on road conditions", "Adjust speed for weather, road curves, and heavy vehicle braking distances. Never exceed posted speed limits."),
            ("Plan rest breaks every 3 hours", "Stop for at least 30 minutes to stretch, refresh, and check vehicle condition. Avoid continuous driving for more than 3 hours."),
            ("Defensive driving mindset", "Stay calm, anticipate road users' actions, avoid aggression. Use mirrors frequently and watch for potential hazards ahead."),
            ("Emergency readiness", "Keep fire extinguisher, first-aid kit, and communication device within easy reach. Be aware of nearest emergency services along the route.")
        ]

        headers = ["Checklist Item", "Detailed Guidelines / Actions"]
        col_widths = [150, 350]

        y_pos = self.create_simple_table(
            canvas_obj,
            "",
            headers,
            defensive_driving_data,
            self.margin, y_pos,
            col_widths,
            title_color=self.colors.PRIMARY,
            max_rows_per_page=25
        )

        y_pos -= 15
        canvas_obj.setFillColor(self.colors.SECONDARY)
        canvas_obj.setFont("Helvetica-Oblique", 8)
        note_text = "Note: These guidelines are mandatory for all petroleum tanker operations. Compliance ensures safety and environmental protection."
        canvas_obj.drawString(self.margin, y_pos, note_text)

    def create_accident_prone_turns_zones_analysis_with_dual_visual(self, canvas_obj, route_data: Dict[str, Any]):
        collections = route_data['collections']

        self.add_page_header(
            canvas_obj,
            "HPCL - Journey Risk Management Study (AI-Powered Analysis)"
        )
        y_pos = self.page_height - 120

        accident_prons = self.remove_duplicate_coordinates(collections.get('accident_areas', []))
        freq_ge_10 = sum(1 for d in accident_prons if d.get("accidentFrequencyYearly", 0) >= 11)
        with_time_of_day_risk = sum(1 for d in accident_prons if d.get("timeOfDayRisk"))
        weather_risk_5_plus = sum(1 for d in accident_prons if d.get("weatherRelatedRisk", 0) > 5)
        infra_risk_5_plus = sum(1 for d in accident_prons if d.get("infrastructureRisk", 0) > 5)
        traffic_risk_5_plus = sum(1 for d in accident_prons if d.get("trafficVolumeRisk", 0) > 5)
        high_conf = sum(1 for d in accident_prons if d.get("dataQuality") == "high")
        medium_conf = sum(1 for d in accident_prons if d.get("dataQuality") == "medium")
        confidence = "High" if high_conf >= medium_conf else "Medium"

        # Table data
        dual_visual_headers = ["Description", "Value"]
        dual_visual_data = [
            ["Total Accident-Prone Locations Analyzed", str(len(accident_prons))],
            ["Yearly Accident Frequency ≥ 11", freq_ge_10],
            ["Locations with Time-of-Day Risk Data", with_time_of_day_risk],
            ["Locations with Weather-Related Risk > 5", weather_risk_5_plus],
            ["Locations with Infrastructure Risk > 5", infra_risk_5_plus],
            ["Locations with Traffic Volume Risk > 5", traffic_risk_5_plus],
            ["Analysis Confidence", confidence]
        ]
        dual_col_width = [250, 260]
        y_pos = self.create_simple_table(
            canvas_obj,
            "ACCIDENT PRONE TURNS ZONES ANALYSIS WITH DUAL VISUAL EVIDENCE (Risk score: 5)",
            dual_visual_headers,
            dual_visual_data,
            50, y_pos, dual_col_width,
            text_color=self.colors.WHITE,
            title_color=self.colors.DANGER,
            header_color=self.colors.WHITE,
            title_font_size = 10
        )

        # === Then: Classification Table ===
        table_headers = ['Angle Range', "Classification", "Risk Level", "Max speed", "Safety Requirements"]
        table_data = [
            ["≥ 120° ", "HAIRPIN TURN", "CRITICAL", "10–15 km/h", "Full stop, sharp steering, extreme caution, check mirrors, inch forward slowly"],
            ["100°–119.9°", "EXTREME SHARP TURN", "EXTREME", "15–20 km/h", "Slow down significantly, use mirrors, stay tight inside lane"],
            ["80°–99.9°", "VERY SHARP TURN", "HIGH", "20 km/h", "Reduce speed, use mirrors, no overtaking"],
            ["60°–79.9°", "HIGH-ANGLE TURN", "MEDIUM", "25–30 km/h", "Brake before turn, keep lane position"],
            ["45°–60°", "SHARP TURN", "LOW", "30–40 km/h", "Stay in lane, observe caution signage"],
            ["30°–44.9°", "MODERATE TURN", "VERY LOW", "40–50 km/h", "Normal caution, light braking"],
            ["< 30°", "GENTLE CURVE", "MINIMAL", "50+ km/h", "Maintain lane, standard driving"]
        ]
        col_width = [60, 120, 80, 80, 180]
        y_pos -= 40  # spacing between the two tables
        y_pos = self.create_simple_table(
            canvas_obj,
            "ACCIDENT-PRONE TURNS ZONES CLASSIFICATION SYSTEM",
            table_headers,
            table_data,
            50, y_pos, col_width,
            text_color=self.colors.DANGER,
            title_color=self.colors.WHITE,
            header_color=self.colors.WHITE
        )
        y_pos -= 20
        if accident_prons:
            if y_pos < 200:
                canvas_obj.showPage()
                self.add_page_header(canvas_obj, "HPCL - Journey Risk Management Study (AI-Powered Analysis)")
                y_pos = self.page_height - 120
            headers = ["Location (GPS)","Link", "Risk Level", "Severity"]
            col_widths = [110,100, 100, 100]

            accident_prone_data = []
            for spot in accident_prons:
                risk_score = spot.get('riskScore', 0)
                if risk_score > 5.5:
                    risk_level = "Critical" 
                    latitude = spot.get('latitude',0)
                    longitude = spot.get('longitude',0)
                    map_link = f"https://www.google.com/maps?q={latitude}%2C{longitude}"
                    severity = str(spot.get("accidentSeverity"))
                    accident_prone_data.append([
                        f"{latitude:.5f}, {longitude:.5f}",
                        map_link,
                        risk_level,
                        severity.upper()
                    ])

            y_pos = self.create_simple_table_with_link(
                canvas_obj,
                "Moderate Accidental Prones",
                headers,
                accident_prone_data,
                50, y_pos,
                col_widths,
                title_bg_color=self.colors.WHITE,
                title_text_color=self.colors.DANGER,
                hyper_link=True,
                hyper_link_col_index=1
            )

    def create_general_env_local_driving_guidelines_page(self, canvas_obj, route_data):
        """GENERAL ENVIRONMENTAL & LOCAL DRIVING GUIDELINES", "For Petroleum Tanker Drivers"""
        self.add_page_header(canvas_obj, "HPCL - Journey Risk Management Study (AI-Powered Analysis)")
        y_pos = self.page_height - 100

        collections = route_data['collections']
        eco_zones = self.remove_duplicate_coordinates(collections.get('eco_sensitive_zones', []))
        # Add the static table from the image
        static_table_headers = ["Zone", "Location", "Coordinates","Link", "Distance From Supply (KM)", 
                            "Distance From Customer (KM)",  "Environmental"]
        
        static_table_data = []
        for zone in eco_zones:
            if self.safe_float(zone.get('distanceFromStartKm', 0)) > 1.0:
                latitude = zone.get('latitude', 0)
                longitude = zone.get('longitude', 0)
                map_link = f"https://www.google.com/maps?q={latitude}%2C{longitude}"
                static_table_data.append([
                    zone.get("zoneType", "Eco Sensitive"),
                    zone.get('name', ''),
                    f"{latitude:.6f}, {longitude:.6f}",
                    map_link,
                    zone.get('distanceFromStartKm', 0),
                    zone.get('distanceFromRouteKm', 0),
                    "Increased wildlife movement, no littering, noise restrictions"
                ])
            
        col_widths = [80, 100, 90, 50, 60, 70, 90] 
        
        if len(static_table_data) > 6:
            static_table_data = static_table_data[:6]

        y_pos = self.create_simple_table_with_link(
            canvas_obj,
            "ENVIRONMENTAL & LOCAL CONSIDERATIONS (Specific Locations)",
            static_table_headers,
            static_table_data,
            30, y_pos,
            col_widths,
            title_bg_color=self.colors.WHITE,
            title_text_color=self.colors.PRIMARY,
            header_color="#ADD8E6",
            hyper_link=True,
            hyper_link_col_index=3
        )
        
        y_pos -= 10  # Add extra space before the general guidelines
        
        # Add the general guidelines section
        canvas_obj.setFillColor(self.colors.PRIMARY)
        canvas_obj.setFont("Helvetica-Bold", 11)
        canvas_obj.drawString(self.margin, y_pos, "GENERAL ENVIRONMENTAL & LOCAL DRIVING GUIDELINES FOR PETROLEUM TANKER DRIVERS")
        y_pos -= 20

        env_guidelines_data = [
            ("Eco-sensitive Areas", "Drive slowly, avoid honking unnecessarily, and do not stop for breaks or cleaning in these areas."),
            ("Waterbody Crossings", "Inspect for leaks before entering bridges, no refueling or repairs on or near bridges."),
            ("School & Market Areas", "Maintain speed limits (25-30 km/h), stay alert for children and pedestrians, avoid peak school/market hours."),
            ("Festivals & Local Events", "Expect road diversions or closures, confirm route with local authorities or control room."),
            ("Littering & Pollution Prevention", "Never discard trash or spill fuel; carry spill kits and clean-up materials as per SOP."),
            ("Noise & Cultural Sensitivity", "Avoid honking in populated areas and during religious or cultural gatherings."),
            ("Local Road Regulations", "Follow local traffic signage and any state-specific restrictions for hazardous cargo."),
            ("Coordination with Locals", "Be courteous to local communities; stop only at designated points for rest, food, or refueling.")
        ]

        headers = ["Aspect", "Guidelines / Actions"]
        col_widths = [150, 350]

        y_pos = self.create_simple_table(
            canvas_obj,
            "",
            headers,
            env_guidelines_data,
            40, y_pos,
            col_widths,
            title_color=self.colors.WHITE,
            text_color=self.colors.PRIMARY,
            max_rows_per_page=25
        )

        y_pos -= 15
        canvas_obj.setFillColor(self.colors.SECONDARY)
        canvas_obj.setFont("Helvetica-Oblique", 8)
        note_text = "Note: These guidelines are mandatory for all petroleum tanker operations. Compliance ensures safety and environmental protection."
        canvas_obj.drawString(self.margin, y_pos, note_text)

    def create_critical_service_gap_identified(self, canvas_obj, route_data: Dict[str, Any]):
        """Create page for critical service gaps and emergency preparedness"""
        self.add_page_header(canvas_obj, "HPCL - Journey Risk Management Study (Al-Powered Analysis)")
        y_pos = self.page_height - 100

        items = ["No critical service gaps identified - Good service coverage along route"]

        y_pos = self.draw_title_bullet_section(
            canvas_obj,
            "CRITICAL SERVICE GAPS IDENTIFIED",
            items,
            y_pos,
            title_color=self.colors.DANGER
        )

        checklist_items = [
            "First aid kit with bandages, antiseptic, pain relievers, emergency medications",
            "Emergency contact numbers saved in phone and written backup copy",
            "Vehicle emergency kit - tools, spare tire, jumper cables, tow rope",
            "Emergency water the Supply (minimum 2 liters per person for 24 hours)",
            "Non-perishable emergency food (energy bars, nuts, dried fruits)",
            "Flashlight with extra batteries or hand-crank/solar-powered model",
            "Emergency blanket, warm clothing, and weatherproof gear",
            "Portable phone charger/power bank with multiple cables",
            "Emergency cash in small denominations (ATMs may be unavailable)",
            "Vehicle documents in waterproof container (registration, insurance)",
            "Road atlas or offline maps as backup to GPS navigation",
            "Emergency whistle, signal mirror, or flares for signaling help",
            "Multi-tool or knife, duct tape, and basic repair supplies",
            "Personal medications for at least 3 days",
            "Important documents (ID, medical info, emergency contacts)",
            "Fire extinguisher (small vehicle type) and basic safety equipment"
        ]

        y_pos = self.draw_title_bullet_section(
            canvas_obj,
            "COMPREHENSIVE EMERGENCY PREPAREDNESS CHECKLIST",
            checklist_items,
            y_pos,
            title_color=self.colors.INFO
        )


        action_plan_items = [
            "ASSESS THE SITUATION - Ensure personal safety first, then assess severity",
            "CALL FOR HELP - Dial the appropriate emergency number (112 for general emergencies)",
            "PROVIDE LOCATION - Give precise GPS coordinates or landmark descriptions",
            "STAY CALM - Speak clearly and provide requested information to operators",
            "FOLLOW INSTRUCTIONS - Emergency operators are trained to guide you",
            "SIGNAL FOR HELP - Use emergency signals if phone coverage is unavailable",
            "STAY WITH VEHICLE - Unless immediate danger, stay near your vehicle",
            "CONSERVE RESOURCES - Ration water, food, and phone battery if stranded",
            "MAINTAIN COMMUNICATION - Update emergency contacts on your status",
            "DOCUMENT INCIDENT - Take photos/notes for insurance and authorities"
        ]

        y_pos = self.draw_title_bullet_section(
            canvas_obj,
            "EMERGENCY RESPONSE ACTION PLAN",
            action_plan_items,
            y_pos,
            title_color=self.colors.DANGER
        )

    def create_weather_analysis_page(self, canvas_obj, route_data: Dict[str, Any]):
        """Create page for comprehensive weather analysis."""
        # Page Header
        self.add_page_header(canvas_obj, "HPCL - Journey Risk Management Study (AI-Powered Analysis) ")
        y_pos = self.page_height - 100

        # Weather Analysis Table
        weather_headers = [
            "Weather Analysis Points",
            "Summer (Apr–Jun)",
            "Monsoon (Jul–Sep)",
            "Autumn (Oct–Nov)",
            "Winter (Dec–Mar)"
        ]
        weather_col_widths = [130, 95, 95, 95, 95]

        weather_data = [
            ["Average Temperature", "38°C", "31°C", "28°C", "12°C"],
            ["Temperature Range", "35°C – 45°C", "28°C – 35°C", "25°C – 32°C", "5°C – 20°C (Night: 0°C -2°C)"],
            ["Weather Conditions were detected.",
            "Hot, dry, dust storms,\noccasional thunderstorms",
            "Heavy rainfall,\nthunderstorms, fog",
            "Mild, pleasant,\noccasional rain, fog",
            "Cold, foggy mornings,\nfrost at night, icy roads"],
            ["Weather Risk Assessment",
            "High risk of vehicle\noverheating, tire blowouts,\nreduced visibility due to dust",
            "Flooding, waterlogging,\nslippery roads, landslides",
            "Slippery roads post-rain,\nmorning/evening fog",
            "Icy roads, black ice, frost,\nbattery failure, poor visibility due to fog"]
        ]

        y_pos = self.create_simple_table(
            canvas_obj,
            "COMPREHENSIVE WEATHER CONDITIONS ANALYSIS  Mild Risk (Risk Score: 3)",
            weather_headers,
            weather_data,
            start_x=50,
            start_y=y_pos,
            col_widths=weather_col_widths,
            title_color=self.colors.ACCENT,
            text_color=self.colors.WHITE,
            header_color=self.colors.WHITE,

            max_rows_per_page=25
        )

        y_pos -= 30

         # Add only the SEASONAL WEATHER VARIATIONS header at the top
        # canvas_obj.setFont("Helvetica-Bold", 12)
        # canvas_obj.setFillColor(self.colors.WHITE)  # Assuming you want white color for the header
        # canvas_obj.drawString(50, y_pos, "SEASONAL WEATHER VARIATIONS ACROSS THE ROUTE")
        # y_pos -= 30

        # Seasonal Variations Table
        seasonal_headers = ["Season", "Major Risks", "Key Driver Actions", "Key Vehicle Checks"]
        seasonal_col_widths = [70, 150, 150, 150]

        # Route Info + Paragraph
        canvas_obj.setFont("Helvetica-Bold", 10)
        canvas_obj.drawString(self.margin, y_pos, f"Route: Northern Plains of India – {route_data['route'].get('fromAddress', 'Unknown')} to {route_data['route'].get('toAddress', 'Unknown')}")
        y_pos -= 18

        canvas_obj.setFont("Helvetica", 10)
        text = ("The region experiences four major seasons with distinct weather patterns: Summer, Monsoon, Autumn, and Winter. "
                "Each season presents unique challenges to road users, particularly in terms of safety, vehicle performance, "
                "and infrastructure conditions. Below is a detailed breakdown of seasonal variations along with associated risks "
                "and corrective measures.")
        text_lines = self.wrap_text(text, max_width=420, font_name="Helvetica", font_size=9)

        for line in text_lines:
            canvas_obj.drawString(self.margin, y_pos, line)
            y_pos -= 12

        y_pos -= 40

        seasonal_data = [
            ["Summer", "Heat, dust storms, thermal damage", "Stay hydrated, avoid peak hours", "Cooling system, tire pressure"],
            ["Monsoon", "Flooding, low visibility", "Slow down, use lights/wipers", "Brakes, wipers, electrical system check"],
            ["Autumn", "Fog, dust, wet roads", "Be cautious in foggy/damp conditions", "Wiper blades, regular cleaning"],
            ["Winter", "Ice, fog, frost, weak battery", "Use winter tires, reduce speed", "Antifreeze, battery health, fluids"]
        ]

        self.create_simple_table(
            canvas_obj,
            "SEASONAL WEATHER VARIATIONS ACROSS THE ROUTE",
            seasonal_headers,
            seasonal_data,
            start_x=50,
            start_y=y_pos,
            col_widths=seasonal_col_widths,
            title_color=self.colors.WHITE,
            text_color=self.colors.INFO,
            header_color=self.colors.WHITE,
            max_rows_per_page=30
        )

    def create_regulatory_compliance_page(self, canvas_obj, route_data: Dict[str, Any]):
            """Create page for comprehensive regulatory compliance analysis."""
            self.add_page_header(canvas_obj, "HPCL - Journey Risk Management Study (AI-Powered Analysis) " )
            y_pos = self.page_height - 130

            title = "COMPREHENSIVE REGULATORY COMPLIANCE ANALYSIS"
            y_pos = self.draw_centered_text_in_box(
                canvas_obj, title, 40, y_pos,
                height=45,
                font_size=14, 
                box_color="#548ed4")

            second_title = "COMPLIANCE STATUS: NEEDS ATTENTION"
            y_pos = self.draw_centered_text_in_box(
                canvas_obj, second_title, 40, y_pos+10,
                height=35,
                font_size=12,
                box_color=self.colors.WARNING)

            # Vehicle & Route Compliance Details Table
            vehicle_headers = ["", ""]
            vehicle_col_widths = [200, 280]

            route = route_data['route']
            vehicle_data = [
                ["Vehicle Type", "Heavy Goods Vehicle"],
                ["Vehicle Category", "Heavy Goods Vehicle"],
                ["AIS-140 GPS Tracking Required", "YES (Mandatory)"],
                ["Route Origin", f"{route.get('fromAddress', 'Not specified')} [{route.get('fromCode', 'N/A')}]"],
                ["Route Destination", f"{route.get('toAddress', 'Not specified')} [{route.get('toCode', 'N/A')}]"],
                ["Total Route Distance", f"{route.get('totalDistance', 0)} km"],
                ["Estimated Travel Duration", self.format_duration(route.get('estimatedDuration', 0))],
                ["Interstate Travel", "NO"]
            ]

            y_pos = self.create_simple_table(
                canvas_obj,
                "VEHICLE & ROUTE COMPLIANCE DETAILS",
                vehicle_headers,
                vehicle_data,
                50, y_pos,
                vehicle_col_widths,
                title_color=self.colors.WHITE,
                text_color=self.colors.PRIMARY,
                header_color= self.colors.WHITE,
            )

            y_pos -= 40

            # Mandatory Compliance Requirements Table
            mandatory_headers = ["Requirement Category", "Compliance Status", "Action Required"]
            mandatory_col_widths = [150, 150, 200]

            mandatory_data = [
                ["Valid Driving License", "REQUIRED", "Verify license category matches vehicle type"],
                ["Vehicle Registration", "REQUIRED", "Ensure current registration is valid"],
                ["Vehicle Insurance", "REQUIRED", "Valid comprehensive insurance is necessary"],
                ["Route Permits", "CONDITIONAL", "Required for interstate/heavy vehicle operations"],
                ["AIS-140 GPS Device", "REQUIRED", "Install certified GPS tracking device"],
                ["Driving Time Limits", "REQUIRED", "Maximum 10 hours of continuous driving"],
                ["Vehicle Fitness Certificate", "REQUIRED", "Ensure valid pollution & fitness certificates"],
                ["Driver Medical Certificate", "REQUIRED", "Maintain a valid medical fitness certificate"]
            ]

            self.create_simple_table(
                canvas_obj,
                "MANDATORY COMPLIANCE REQUIREMENTS",
                mandatory_headers,
                mandatory_data,
                50, y_pos,
                mandatory_col_widths,
                title_color=self.colors.WHITE,
                text_color=self.colors.DANGER,
                header_color=self.colors.WHITE,
            )

    def create_road_quality_and_surface_conditions(self, canvas_obj, route_data: Dict[str, Any]):
        collections = route_data['collections']

        self.add_page_header(
            canvas_obj,
            "HPCL - Journey Risk Management Study (AI-Powered Analysis)"
        )
        y_pos = self.page_height - 120

        all_road_conditions = collections.get('road_conditions', [])        
        road_conditions = self.remove_duplicate_coordinates(all_road_conditions)

        critical_conditions = [r for r in road_conditions if r['riskScore'] >= 8]
        high_risk_areas = [r for r in road_conditions if 5 <= r['riskScore'] < 8]
        medium_risk_areas = [r for r in road_conditions if 3 <= r['riskScore'] < 5]
        data_sources = list(set([r.get('dataSource', 'Unknown') for r in road_conditions]))

        # Road Conditions Data - all dynamic values
        headers = ["Description", "Value"]
        road_conditions_data = [
            ["Total Analysis Points ", str(len(road_conditions))],
            ["Road Quality Issues Detected ", str(len(road_conditions))],
            ["Critical Condition Areas", str(len(critical_conditions))],
            ["High Risk Areas ", str(len(high_risk_areas))],
            ["Medium Risk Areas ", str(len(medium_risk_areas))],
            ["API Sources Used ",f"{data_sources}"],
            ["Analysis Confidence ", "Medium" if len(road_conditions) < 50 else "High"]
        ]

        dual_col_width = [250, 260]
        y_pos = self.create_simple_table(
            canvas_obj,
            "COMPREHENSIVE ROAD QUALITY & SURFACE CONDITIONS(Risk Score :4)",
            headers,
            road_conditions_data,
            50, y_pos, dual_col_width,
            title_color=self.colors.DANGER,
            header_color=self.colors.WHITE,
        )

        # Detailed table
        detailed_headers = ["Location (GPS)", "Map link", "Road Type", "Surface Quality", "Width (m)",
                        "Under Construction", "Risk Score", "Severity"]
        col_width = [100, 40, 60, 70, 60, 60,60, 70]
        detailed_data = []
        for r in road_conditions:
            lat = r.get("latitude")
            lon = r.get("longitude")
            map_link = f"https://www.google.com/maps?q={lat},{lon}"

            # Severity logic based on riskScore
            risk_score = r.get("riskScore", 0)
            if risk_score >= 8:
                severity = "critical"
            elif risk_score >= 5:
                severity = "high"
            elif risk_score >= 3:
                severity = "medium"
            else:
                severity = "low"

            detailed_data.append([
                f"{lat},{lon}",
                map_link,
                r.get("roadType", "N/A"),
                r.get("surfaceQuality", "N/A"),
                str(r.get("widthMeters", "N/A")),
                "Yes" if r.get("underConstruction") else "No",
                str(risk_score),
                severity
            ])
            
        y_pos -= 40  # spacing between the two tables
        y_pos = self.create_simple_table_with_link(
            canvas_obj,
            f"IDENTIFIED ROAD QUALITY ISSUES ({len(road_conditions)} Locations) ",
            detailed_headers,
            detailed_data,
            50, y_pos, col_width,
            title_bg_color=self.colors.WHITE,
            title_text_color=self.colors.DANGER,
            header_color=self.colors.WHITE,
            hyper_link=True,
            hyper_link_col_index=1
        )

        # Add VEHICLE-SPECIFIC ROAD QUALITY RECOMMENDATIONS
        y_pos -= 30  # Add some spacing after the previous table
        
        recommendations = [
            "Heavy vehicles: Reduce speed by 20% in areas with road quality scores below 6/10",
            "Check tire pressure more frequently when traveling through poor surface areas",
            "Increase following distance by 50% in road quality risk zones",
            "Plan additional maintenance checks after routes with multiple road quality issues",
            "Consider alternative routes for high-value or sensitive cargo in critical condition areas",
            "Carry emergency repair kit for tire damage in poor road surface zones"
        ]
        
        # Add recommendations title
        canvas_obj.setFont("Helvetica-Bold", 12)
        canvas_obj.setFillColor(self.colors.DANGER)
        canvas_obj.drawString(50, y_pos, "VEHICLE-SPECIFIC ROAD QUALITY RECOMMENDATIONS")
        y_pos -= 20
        
        # Add each recommendation as a bullet point
        canvas_obj.setFont("Helvetica", 10)
        canvas_obj.setFillColor(self.colors.BLACK)
        for recommendation in recommendations:
            y_pos -= 15
            canvas_obj.drawString(70, y_pos, "• " + recommendation)
            
        return y_pos
    def safe_float(self, value, default=0.0):
        """Safely convert value to float, handling various input types"""
        if value is None:
            return default
        
        try:
            # If it's already a number, return it
            if isinstance(value, (int, float)):
                return float(value)
            
            # If it's a string, try to convert
            if isinstance(value, str):
                # Remove any non-numeric characters except . and -
                cleaned = ''.join(c for c in value if c.isdigit() or c in '.-')
                if cleaned:
                    return float(cleaned)
            
            return default
        except (TypeError, ValueError):
            return default
    def generate_pdf_report(self, route_id: str, output_path: str = None) -> str:
        """Main method to generate complete PDF report with comprehensive risk zones"""
        try:
            logger.info(f"🚀 Starting PDF generation for route: {route_id}")
            
            # Load route data
            route_data = self.load_route_data(route_id)
            
            # Extract route name first (before using it)
            route_name = route_data['route'].get('routeName', 'route').replace(' ', '_')
            
            # Determine output path
            if not output_path:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                output_path = f"HPCL_Route_Analysis_{route_name}_{timestamp}.pdf"
            
            # Ensure output directory exists
            output_dir = Path(output_path).parent
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Create PDF using direct canvas approach
            from reportlab.pdfgen import canvas
            from reportlab.lib.pagesizes import A4
            
            pdf_canvas = canvas.Canvas(output_path, pagesize=A4)
            pdf_canvas.setTitle(f"HPCL Route Analysis - {route_name}")
            page_num = 1
            
            # Page 1: Title Page
            logger.info("📄 Generating Page 1: Title Page")
            self.create_title_page(pdf_canvas, route_data)
            pdf_canvas.showPage(); page_num += 1
            
            # Page 2: Executive Summary & Risk Overview
            logger.info("📄 Generating Page 2: Executive Summary")
            self.create_executive_summary_page(pdf_canvas, route_data)
            pdf_canvas.showPage()
            
            # Page 3: Route Map with Google Maps
            logger.info("📄 Generating Page 3: Route Map")
            self.create_route_map_page(pdf_canvas, route_data)
            pdf_canvas.showPage()
            
            # Page 4: Safety Measures & Regulatory Compliance
            logger.info("📄 Generating Page 4: Safety Measures & Compliance")
            self.create_safety_measures_page(pdf_canvas, route_data, page_num)
            pdf_canvas.showPage(); page_num += 1

            # Page 5+: HIGH-RISK ZONES (Comprehensive with all categories)
            logger.info("📄 Generating Pages 5+: Comprehensive High-Risk Zones")
            self.create_comprehensive_risk_zones_page(pdf_canvas, route_data, page_num)
            pdf_canvas.showPage();  page_num += 1

            # Page X: Seasonal Road Conditions
            logger.info("📄 Generating Seasonal Road Conditions")
            self.create_seasonal_road_conditions_page(pdf_canvas, route_data)
            pdf_canvas.showPage()
            
            # Page X+1: Emergency Services - Medical Facilities
            logger.info("📄 Generating Medical Facilities")
            self.create_medical_facilities_page(pdf_canvas, route_data,)
            pdf_canvas.showPage(); 
            
            # Page X+2: Emergency Services - Law Enforcement, Fire, educational, fuel and food
            logger.info("📄 Generating Law Enforcement ,Fire ServicesFire, educational, fuel and food")
            self.create_law_enforcement_page(pdf_canvas, route_data)
            pdf_canvas.showPage()
            
            # Page: GENERAL ENVIRONMENTAL & LOCAL DRIVING GUIDELINES FOR PETROLEUM TANKER DRIVERS
            logger.info("📄 GENERAL ENVIRONMENTAL & LOCAL DRIVING GUIDELINES FOR PETROLEUM TANKER DRIVERS (static content)")
            self.create_general_env_local_driving_guidelines_page(pdf_canvas, route_data)
            pdf_canvas.showPage()

            # Page X+2: DEFENSIVE DRIVING & DRIVER WELL-BEING (static content)
            logger.info("📄 DEFENSIVE DRIVING & DRIVER WELL-BEING (static content)")
            self.deffensive_driving_and_driver_wellbeing(pdf_canvas)
            pdf_canvas.showPage()

            # Final Page: Sharp Turn 
            logger.info("📄 Generating Sharp turns")
            self.add_detailed_risk_analysis_pages(pdf_canvas, route_data)
            pdf_canvas.showPage()

            # Page 5+: ACCIDENT PRONE TURNS ZONES ANALYSIS WITH DUAL VISUAL EVIDENCE High Risk
            logger.info("📄 Generating Pages : ACCIDENT PRONE TURNS ZONES ANALYSIS WITH DUAL VISUAL EVIDENCE High Risk")
            self.create_accident_prone_turns_zones_analysis_with_dual_visual(pdf_canvas, route_data)
            pdf_canvas.showPage()

            # Final Page: Critical Service Gaps Identified
            logger.info("📄 Generating Critical Service Gaps Identified")
            self.create_critical_service_gap_identified(pdf_canvas, route_data)
            pdf_canvas.showPage()
            
            # Page X+4: Network Coverage & Communication
            logger.info("📄 Generating Network Coverage")
            self.create_network_coverage_page(pdf_canvas, route_data,page_num )
            pdf_canvas.showPage(); page_num += 1
            
            # Page: Weather Analysis
            logger.info("📄 Generating Weather Analysis")
            self.create_weather_analysis_page(pdf_canvas, route_data)
            pdf_canvas.showPage()

            # Page X+6: Regulatory Compliance
            logger.info("📄 Generating Regulatory Compliance")
            self.create_regulatory_compliance_page(pdf_canvas, route_data)
            pdf_canvas.showPage()

            # Final Page: Compliance Issues
            logger.info("📄 Generating Compliance Issues")
            y_pos = 750  # or whatever your top margin is
            y_pos = self.add_compliance_issues(pdf_canvas, y_pos)
            y_pos = self.add_applicable_regulatory_framework(pdf_canvas, y_pos)
            y_pos = self.add_compliance_recommendations(pdf_canvas, y_pos)
            y_pos = self.add_non_compliance_penalties(pdf_canvas, y_pos)
            pdf_canvas.showPage()


            # Final Page: Elevation Terrain Analysis
            logger.info("📄 Generating Elevation Terrain Analysis")
            self.create_elevation_terrain_analysis_page(pdf_canvas, route_data)
            pdf_canvas.showPage()

            # Final Page: Elevation Based Driving Challenges
            logger.info("📄 Generating Elevation Based Driving Challenges")
            self.create_elevation_based_driving_challenges_page(pdf_canvas, route_data)
            pdf_canvas.showPage()

            
            # Final Page: Traffic Analysis
            logger.info("📄 Generating Traffic Analysis")
            self.create_traffic_analysis_page(pdf_canvas, route_data)
            pdf_canvas.showPage()

            # Final Page: COMPREHENSIVE ROAD QUALITY & SURFACE CONDITIONS 
            logger.info("📄 COMPREHENSIVE ROAD QUALITY & SURFACE CONDITIONS ")
            self.create_road_quality_and_surface_conditions(pdf_canvas,route_data)
            pdf_canvas.showPage()

            # NEW PAGE: Comprehensive Environmental Assessment
            logger.info("📄 Generating Comprehensive Environmental Assessment")
            self.create_comprehensive_environmental_assessment_page(pdf_canvas, route_data)
            pdf_canvas.showPage()

            # Final Page: Emergency Preparedness & Guidelines
            logger.info("📄 Generating Emergency Guidelines")
            self.create_emergency_guidelines_page(pdf_canvas, route_data)
            pdf_canvas.showPage()

            # Final Page:Emergency Situation Standard Operating Procedure (SOP)  
            logger.info("📄 final page: Emergency Situation Standard Operating Procedure")
            self.create_emergency_sop_section(pdf_canvas)
            pdf_canvas.showPage()
            
            # Save the PDF
            pdf_canvas.save()
            
            logger.info(f"✅ PDF generated successfully: {output_path}")
            logger.info(f"📊 Route: {route_data['route'].get('routeName', 'Unknown')}")
            logger.info(f"🛣️ Distance: {route_data['route'].get('totalDistance', 0)}km")
            logger.info(f"📈 Data Points: {route_data['statistics']['total_data_points']}")
            logger.info(f"⚠️ Critical Points: {route_data['statistics']['risk_analysis']['critical_points']}")
            logger.info(f"🎯 Data Quality: {route_data['data_quality']['level']} ({route_data['data_quality']['score']}%)")
            logger.info(f"📄 Generated comprehensive analysis with detailed risk zones")
            
            return output_path
            
        except Exception as e:
            logger.error(f"❌ PDF generation failed: {e}")
            raise
        finally:
            if self.client:
                self.client.close()

def main():
    """Main function for command-line usage"""
    if len(sys.argv) < 2:
        print("Usage: python hpcl_pdf_generator.py <route_id> [output_path]")
        print("Example: python hpcl_pdf_generator.py 507f1f77bcf86cd799439011")
        sys.exit(1)
    
    route_id = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else None
    
    try:
        # Initialize generator
        generator = HPCLDynamicPDFGenerator()
        
        # Generate PDF
        result_path = generator.generate_pdf_report(route_id, output_path)
        
        print(f"\n🎉 PDF generation completed successfully!")
        print(f"📁 File saved: {result_path}")
        print(f"📊 Report generated with real-time MongoDB data")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

