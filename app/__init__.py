from flask import Flask, render_template, request, jsonify, redirect, url_for
import os
from datetime import datetime
import json

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024 * 1024  # 16GB max file size

# Ensure upload folder exists
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

@app.route('/')
def index():
    """Render the main page"""
    return render_template('index.html')

@app.route('/process', methods=['POST'])
def process_video():
    """Handle video processing request"""
    try:
        # Get form data
        start_time = request.form.get('start_time')
        end_time = request.form.get('end_time')
        
        # Here you would add your S3 processing logic
        # For now, we'll just return a success message
        return jsonify({
            'status': 'success',
            'message': f'Processing video from {start_time} to {end_time}'
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
